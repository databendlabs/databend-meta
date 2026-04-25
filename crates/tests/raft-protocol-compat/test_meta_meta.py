#!/usr/bin/env python3
"""
E2E raft-protocol backward compatibility test for databend-meta.

Verifies that the current-source binary stays interoperable with one or
more released versions across the raft wire protocol (AppendEntries,
InstallSnapshot) and persisted on-disk formats (raft log, state-machine
snapshot). Each pinned old version lives in its own `bin-<tag>/`
workspace; the current source tree is built in `bin-current/`.

For every `(test, old_version, direction)` triple, the orchestrator brings
up a 2-node cluster with current↔old playing leader/follower (and the
inverse), then verifies internal raft events via `/v1/ctrl/status` plus an
absolute content assertion on the exported state machine.

Adding a new old version is purely a directory creation: copy
`bin-v260205.4.0/` to `bin-v<NEW_TAG>/`, edit `Cargo.toml` to point at the
new tag, fix any divergent API in `src/main.rs`. The orchestrator
auto-discovers `bin-v*` directories at startup.

Usage:
    python3 test_meta_meta.py                                          # build and run full matrix
    python3 test_meta_meta.py --skip-build                             # skip cargo build
    python3 test_meta_meta.py --test SnapshotReplication               # one test, all old versions
    python3 test_meta_meta.py --test RestartReplication
    python3 test_meta_meta.py --old-version v260205.4.0                # one old version, both directions
    python3 test_meta_meta.py --direction current-to-old               # only one direction
    python3 test_meta_meta.py --direction old-to-current
    python3 test_meta_meta.py --keep-data                              # preserve .test-data
"""

import argparse
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Iterable, Optional


SCRIPT_DIR = Path(__file__).parent

# Convention for binary directories under SCRIPT_DIR:
#   bin-current/          — workspace with path deps on the working source tree
#   bin-v<tag>/           — workspace with git deps pinned to a released tag
#
# `discover_versions()` enumerates these and exposes them as a `name -> binary`
# map, where the name is the directory's suffix after `bin-` (so `bin-current`
# becomes `current`, `bin-v260205.4.0` becomes `v260205.4.0`). Names appear in
# CLI flags, role tags, and direction arrows — making them user-visible.
CURRENT_VERSION = "current"
BIN_DIR_PREFIX = "bin-"


def discover_versions() -> dict[str, Path]:
    """Map version name -> path of its built binary.

    Scans `SCRIPT_DIR` for directories matching `bin-<name>` and returns
    `{name: <dir>/target/debug/databend-meta-compat}` for each. The current
    source tree is the entry whose name is `current`; everything else is an
    old version. Raises if `bin-current/` is missing — the test is meaningless
    without it.
    """
    versions: dict[str, Path] = {}
    for d in sorted(SCRIPT_DIR.iterdir()):
        if not d.is_dir() or not d.name.startswith(BIN_DIR_PREFIX):
            continue
        name = d.name[len(BIN_DIR_PREFIX):]
        versions[name] = d / "target" / "debug" / "databend-meta-compat"
    if CURRENT_VERSION not in versions:
        raise RuntimeError(
            f"missing {BIN_DIR_PREFIX}{CURRENT_VERSION}/ under {SCRIPT_DIR}; "
            "the compat test needs the current source tree as one endpoint"
        )
    return versions


BINARIES: dict[str, Path] = discover_versions()
OLD_VERSIONS: list[str] = sorted(v for v in BINARIES if v != CURRENT_VERSION)


# --- output helpers ---------------------------------------------------------
#
# All console output goes through these helpers so the test log has a uniform,
# scannable shape:
#
#   ┌────────────────────────────────────────────────────────────────────┐
#   │ TEST: <description>                                                │
#   └────────────────────────────────────────────────────────────────────┘
#     [current/1]      starting --single
#     [current/1]      ready (pid=12345, grpc=127.0.0.1:28201)
#     [current/1]      feed test_key_0..9 (10 keys)
#     [v260205.4.0/2]  wait: follower joined cluster as voter
#              ✓ voters=[1, 2] term=1 leader=1 applied=26 snapshot=13 purged=13
#     [verify]         node1-raw: 20/20 expected keys present ✓
#
# The leading two-space indent inside each section keeps the test body visually
# distinct from the box-drawn headers and the final PASSED line. Role tags
# (`current/1`, `v260205.4.0/2`, …) vary in width because tags vary in length;
# this is accepted in exchange for showing which version owns each line.

INDENT = "  "
WAIT_OK = " " * 9 + "✓ "  # aligns with the column after "[role] "

def hr(title: str = "") -> None:
    """Print a section header with a horizontal rule."""
    width = 72
    print()
    print("┌" + "─" * (width - 2) + "┐")
    if title:
        print(f"│ {title:<{width - 4}} │")
    print("└" + "─" * (width - 2) + "┘")


def step(role: str, msg: str) -> None:
    """One step in the test, prefixed with the role tag (e.g. `[old/1]`)."""
    print(f"{INDENT}[{role}] {msg}")


def detail(msg: str) -> None:
    """Indented continuation of the previous step (e.g. wait result)."""
    print(f"{INDENT}{WAIT_OK}{msg}")


def banner(msg: str) -> None:
    """Top-level status line (PASSED / ALL TESTS PASSED)."""
    print()
    print(f"{INDENT}{msg}")


def format_status(s: dict) -> str:
    """One-line summary of the fields that drive `wait_status` predicates."""
    def v(x):
        return str(x) if x is not None else "-"
    return (
        f"voters={s['voters']} term={s['current_term']} "
        f"leader={v(s['current_leader'])} applied={v(s['last_applied_index'])} "
        f"snapshot={v(s['snapshot_index'])} purged={v(s['purged_index'])}"
    )


def format_indices(indices: list[int]) -> str:
    """Format a list of contiguous ints as `0..9 (10 keys)`."""
    if not indices:
        return "(no keys)"
    if indices == list(range(indices[0], indices[-1] + 1)):
        return f"test_key_{indices[0]}..{indices[-1]} ({len(indices)} keys)"
    return f"{indices} ({len(indices)} keys)"


def wait_tcp_port(port: int, timeout: int = 30) -> None:
    """Block until a TCP port accepts connections, or raise TimeoutError."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                sock.connect(("127.0.0.1", port))
                return
        except (socket.error, socket.timeout):
            time.sleep(0.5)
    raise TimeoutError(f"Port {port} did not become available within {timeout}s")


class Node:
    """A single databend-meta-compat node.

    Owns its identity (`node_id`, `version`), endpoints derived from `node_id`
    (admin/grpc/raft addresses), the on-disk raft directory, and — once
    started — the subprocess handle. Methods wrap the binary's subcommands and
    the admin HTTP endpoints so a test reads as operations on this node, not
    a sequence of (version, node_id) tuples.
    """

    def __init__(
        self,
        node_id: int,
        version: str,
        binary: Path,
        raft_dir: Path,
        log_path: Path,
    ):
        self.node_id = node_id
        self.version = version  # "current" or a tag like "v260205.4.0"
        self.binary = binary
        self.raft_dir = raft_dir
        self.log_path = log_path
        self.admin_port = 28100 + node_id
        self.grpc_port = 28200 + node_id
        self.raft_port = 28300 + node_id
        self.admin_addr = f"127.0.0.1:{self.admin_port}"
        self.grpc_addr = f"127.0.0.1:{self.grpc_port}"
        self.raft_addr = f"127.0.0.1:{self.raft_port}"
        self.process: Optional[subprocess.Popen] = None

    @property
    def role(self) -> str:
        """Tag used as the line prefix in test output, e.g. `old/1`."""
        return f"{self.version}/{self.node_id}"

    # --- lifecycle ---

    def start(self, extra_args: list[str]) -> None:
        """Run `serve`; block until the gRPC port accepts connections."""
        self.raft_dir.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        cmd = [
            str(self.binary),
            "serve",
            "--id", str(self.node_id),
            "--admin-api-address", self.admin_addr,
            "--grpc-api-address", self.grpc_addr,
            "--raft-listen-host", "127.0.0.1",
            "--raft-advertise-host", "127.0.0.1",
            "--raft-api-port", str(self.raft_port),
            "--raft-dir", str(self.raft_dir),
            "--max-applied-log-to-keep", "0",
            "--log-stderr-on",
            "--log-stderr-level", "DEBUG",
            *extra_args,
        ]
        # Show only the differentiating flags (`--single`, `--join …`); the
        # boilerplate is the same for every start and adds noise.
        step(self.role, f"starting {' '.join(extra_args)}")

        log_fd = open(self.log_path, "w")
        self.process = subprocess.Popen(cmd, stderr=log_fd, stdout=log_fd)

        wait_tcp_port(self.grpc_port, timeout=30)
        step(self.role, f"ready (pid={self.process.pid}, grpc={self.grpc_addr})")

    def stop(self) -> None:
        """Stop gracefully (SIGTERM, then SIGKILL after 10s)."""
        if self.process is None:
            return
        try:
            self.process.send_signal(signal.SIGTERM)
        except OSError:
            pass
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        step(self.role, "stopped")
        self.process = None

    # --- gRPC subcommands ---

    def upsert(self, key: str, value: str) -> None:
        """Write a single KV pair via the binary's `upsert` subcommand."""
        cmd = [
            str(self.binary),
            "upsert",
            "--grpc-api-address", self.grpc_addr,
            "--key", key,
            "--value", value,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            step(self.role, f"upsert FAILED for key={key}")
            print(f"          stdout: {result.stdout}")
            print(f"          stderr: {result.stderr}")
            raise RuntimeError(f"upsert failed on {self.role} for key={key}")

    def feed_data(self, indices: Iterable[int]) -> dict[str, str]:
        """Upsert `test_key_<i> = test_value_<i>_data` for each i in `indices`.

        Returns the {key: value} dict that was fed.
        """
        indices = list(indices)
        step(self.role, f"feed {format_indices(indices)}")
        fed: dict[str, str] = {}
        for i in indices:
            key = f"test_key_{i}"
            value = f"test_value_{i}_data"
            self.upsert(key, value)
            fed[key] = value
        return fed

    def export(self, output: Path) -> None:
        """Export state machine data via the binary's `export` subcommand."""
        cmd = [str(self.binary), "export", "--grpc-api-address", self.grpc_addr]
        step(self.role, f"export → {output.name}")
        with open(output, "w") as f:
            subprocess.run(cmd, stdout=f, check=True)

    # --- admin HTTP endpoints ---

    def trigger_snapshot(self) -> None:
        """POST /v1/ctrl/trigger_snapshot. Available on both old and new."""
        url = f"http://{self.admin_addr}/v1/ctrl/trigger_snapshot"
        step(self.role, "trigger snapshot")
        urllib.request.urlopen(url).read()

    def get_status(self) -> dict:
        """GET /v1/ctrl/status. Available on new-version nodes only."""
        url = f"http://{self.admin_addr}/v1/ctrl/status"
        with urllib.request.urlopen(url, timeout=5) as resp:
            return json.loads(resp.read().decode())

    def wait_status(
        self,
        predicate: Callable[[dict], bool],
        desc: str,
        timeout: int = 30,
    ) -> dict:
        """Poll /v1/ctrl/status until `predicate(status)` holds, or raise.

        On timeout the error includes the last observed status, so post-mortems
        show exactly what condition was missed.
        """
        step(self.role, f"wait: {desc}")
        deadline = time.time() + timeout
        last: Optional[dict] = None
        while time.time() < deadline:
            try:
                last = self.get_status()
                if predicate(last):
                    detail(format_status(last))
                    return last
            except (urllib.error.URLError, ConnectionError, json.JSONDecodeError):
                pass
            time.sleep(0.2)
        raise TimeoutError(
            f"{self.role} timeout after {timeout}s waiting for: {desc}; "
            f"last status: {last}"
        )


class TestContext:
    """Two-node test environment with version assignment per raft role.

    Node 1 is always the leader; node 2 always joins as follower. Their
    binary versions are parameterized by `leader_version` /
    `follower_version`, so the same test exercises both `current→<old>` and
    `<old>→current` by instantiating two contexts with the assignment
    swapped. Test bodies refer to `ctx.leader` / `ctx.follower` and stay
    agnostic to which version is on each side.

    Both nodes share `data_dir` for their per-node raft directories and log
    files. `cleanup()` stops both processes and wipes `data_dir`.
    """

    def __init__(self, leader_version: str, follower_version: str):
        self.leader_version = leader_version
        self.follower_version = follower_version
        self.data_dir = SCRIPT_DIR / ".test-data"
        self.leader = Node(
            node_id=1,
            version=leader_version,
            binary=BINARIES[leader_version],
            raft_dir=self.data_dir / "meta_1",
            log_path=self.data_dir / "meta_log_1.txt",
        )
        self.follower = Node(
            node_id=2,
            version=follower_version,
            binary=BINARIES[follower_version],
            raft_dir=self.data_dir / "meta_2",
            log_path=self.data_dir / "meta_log_2.txt",
        )

    @property
    def direction(self) -> str:
        """Compact label, e.g. `current→v260205.4.0`, used in test banners."""
        return f"{self.leader_version}→{self.follower_version}"

    def nodes(self) -> list[Node]:
        return [self.leader, self.follower]

    def stop_all(self) -> None:
        for n in self.nodes():
            n.stop()
        time.sleep(1)

    def cleanup(self) -> None:
        for n in self.nodes():
            n.stop()
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)


# --- export parsing & comparison helpers (file-level, context-independent) ---

def parse_kv_state(export_path: Path) -> dict[str, str]:
    """Reconstruct the KV state machine view from an export.

    An export has two sections that together cover every write a node has
    seen:

      - `state_machine/0` lines: `GenericKV` entries, materialized from the
        persisted snapshot. Covers writes that fell into the latest snapshot.
      - `raft_log` lines: `LogEntry` commands still present in the raft log
        store. Each `Normal` payload is a `Transaction` whose `if_then` list
        carries the original `Put` commands. Covers writes after the latest
        snapshot (and any unpurged earlier ones).

    Reading only `state_machine` would miss post-snapshot writes; reading only
    `raft_log` would miss writes from before any logs were purged. The union
    is what the running state machine would return.

    For our tests every key is written at most once, so order between
    sections doesn't matter — each key has exactly one source.
    """
    result: dict[str, str] = {}

    with open(export_path) as f:
        for line in f:
            entry = json.loads(line)
            if not isinstance(entry, list) or len(entry) != 2:
                continue
            domain, payload = entry
            if not isinstance(payload, dict):
                continue

            # Persisted-snapshot section.
            if domain.startswith("state_machine") and "GenericKV" in payload:
                kv = payload["GenericKV"]
                result[kv["key"]] = bytes(kv["value"]["data"]).decode("utf-8")
                continue

            # Raft log section: extract Put commands from Transaction.if_then.
            # Payloads can also be `"Blank"` (string) or `{"Membership": ...}`,
            # which we skip without further parsing.
            if domain == "raft_log" and "LogEntry" in payload:
                inner = payload["LogEntry"].get("payload")
                if not isinstance(inner, dict):
                    continue
                normal = inner.get("Normal")
                if not isinstance(normal, dict):
                    continue
                txn = normal.get("cmd", {}).get("Transaction")
                if not isinstance(txn, dict):
                    continue
                for op in txn.get("if_then", []):
                    put = op.get("request", {}).get("Put")
                    if put:
                        result[put["key"]] = bytes(put["value"]).decode("utf-8")
    return result


def assert_kv_present(export_path: Path, expected: dict[str, str]) -> None:
    """Assert every (key, value) in `expected` is present in the export.

    The check is one-way: the export may contain additional keys (raft
    bookkeeping, etc.); but every expected key must be present with the exact
    expected value. On mismatch the error lists missing keys and value
    mismatches separately.
    """
    actual = parse_kv_state(export_path)
    missing = [k for k in expected if k not in actual]
    wrong = [
        (k, expected[k], actual[k])
        for k in expected
        if k in actual and actual[k] != expected[k]
    ]
    if missing or wrong:
        raise AssertionError(
            f"Export {export_path} content mismatch:\n"
            f"  missing keys: {missing}\n"
            f"  value mismatches: {wrong}\n"
            f"  actual keys present: {sorted(actual.keys())}"
        )
    step("verify", f"{export_path.name}: {len(expected)}/{len(expected)} expected keys present ✓")


def filter_and_sort(
    input_file: Path,
    output_file: Path,
    include: Optional[str] = None,
    exclude: Optional[list[str]] = None,
    transform=None,
) -> None:
    """Filter, transform, and sort lines from input_file."""
    lines = []
    with open(input_file, "r") as f:
        for line in f:
            if include and include not in line:
                continue
            if exclude and any(pattern in line for pattern in exclude):
                continue
            if transform:
                line = transform(line)
            lines.append(line)

    with open(output_file, "w") as f:
        f.writelines(sorted(lines))


def diff_files(file1: Path, file2: Path) -> None:
    """Compare files and raise if different."""
    result = subprocess.run(["diff", str(file1), str(file2)], capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout[:2000])
        raise RuntimeError(f"Files differ: {file1} vs {file2}")
    step("verify", f"{file1.name} vs {file2.name}: identical ✓")


def filter_and_compare_exports(
    raw1: Path,
    raw2: Path,
    include: Optional[str] = None,
    exclude: Optional[list[str]] = None,
) -> None:
    """Filter exported data and compare for consistency."""
    filtered1 = raw1.parent / f"{raw1.stem}-filtered"
    filtered2 = raw2.parent / f"{raw2.stem}-filtered"

    def transform(line):
        line = re.sub(r',?"proposed_at_ms":\d+', "", line)
        line = re.sub(r',?"expire_at":null', "", line)
        line = re.sub(r',?"meta":\{\}', "", line)
        line = re.sub(r',?"meta":null', "", line)
        return line

    filter_and_sort(raw1, filtered1, include=include, exclude=exclude, transform=transform)
    filter_and_sort(raw2, filtered2, include=include, exclude=exclude, transform=transform)
    diff_files(filtered1, filtered2)


def export_both_and_compare(ctx: TestContext, expected: dict[str, str]) -> tuple[Path, Path]:
    """Export from both nodes; assert content; cross-compare exports.

    `export` covers two sources together — the persisted state-machine
    snapshot AND the un-purged raft log entries — so post-snapshot writes
    are visible in the `raft_log` section even when the snapshot doesn't
    yet include them. `parse_kv_state` reads both, so no end-of-test
    snapshot flush is required.

    The cross-node `state_machine` diff specifically exercises cross-version
    snapshot serialization compatibility: both nodes hold the *transferred*
    snapshot (built by leader, received by follower) in their persisted
    form, so an identical diff proves the receiving version reads & re-emits
    the sending version's on-disk format byte-for-byte equivalently — in
    whichever direction the test is currently exercising.
    """
    raw1 = ctx.data_dir / "node1-raw"
    raw2 = ctx.data_dir / "node2-raw"

    ctx.leader.export(raw1)
    ctx.follower.export(raw2)

    assert_kv_present(raw1, expected)
    assert_kv_present(raw2, expected)
    filter_and_compare_exports(raw1, raw2, include="state_machine", exclude=["DataHeader"])
    return raw1, raw2


class TestCase:
    """Base class for cross-version raft compatibility tests.

    The subclass's class name is the test's canonical identifier — used as
    the source-code symbol, the CLI selector (`--test <ClassName>`), and the
    base of the title in the run output. The actual run banner appends the
    direction (e.g. `SnapshotReplication[old→new]`) so each direction is
    visually distinct in the log while still being addressable as a single
    test class on the CLI.

    Subclasses implement `run(ctx)` to return the `{key: value}` dict fed
    during the test; the framework's `execute()` wraps each run with
    cleanup, export verification, and a PASSED banner. Override `execute()`
    for tests that need different verification.
    """

    @classmethod
    def name(cls) -> str:
        return cls.__name__

    def label(self, ctx: TestContext) -> str:
        """Display label including direction, e.g. `SnapshotReplication[old→new]`."""
        return f"{self.name()}[{ctx.direction}]"

    def run(self, ctx: TestContext) -> dict[str, str]:
        raise NotImplementedError

    def execute(self, ctx: TestContext) -> None:
        hr(f"TEST: {self.label(ctx)}")
        ctx.cleanup()
        ctx.data_dir.mkdir(parents=True)
        fed = self.run(ctx)
        export_both_and_compare(ctx, fed)
        ctx.stop_all()
        banner(f"PASSED: {self.label(ctx)}")


class SnapshotReplication(TestCase):
    """Leader builds a snapshot; follower installs it via cross-version
    InstallSnapshot RPC.

    Test setup:
      1. Leader writes 10 KV pairs.
      2. Leader triggers a snapshot covering those 10 writes.
      3. Leader writes 10 more KV pairs (these stay in the raft log,
         post-snapshot).
      4. Follower joins.

    What proves the snapshot was actually transferred (vs. caught up via log
    replay only): on the follower we expect `0 < snapshot_index <
    last_applied_index`. The snapshot covers earlier state (the first 10
    writes); the post-snapshot logs (10–19) replicate via AppendEntries on
    top. A follower that reached its current state by log replay alone would
    have `snapshot_index = None`. A follower that built a snapshot locally
    after replicating everything would have `snapshot_index ≈
    last_applied_index`. Only InstallSnapshot followed by AppendEntries
    produces the strict inequality.

    `--max-applied-log-to-keep 0` lets the leader purge its post-snapshot
    logs once safe, so the follower has no choice but to use InstallSnapshot
    for the pre-snapshot range. This assertion is the test's load-bearing
    cross-version check — do not weaken it without replacing the proof.

    Run in both directions so InstallSnapshot is exercised with each version
    on each side: old→new tests new-version reading old-version's snapshot
    payload; new→old tests old-version reading new-version's payload.
    """

    def run(self, ctx: TestContext) -> dict[str, str]:
        ctx.leader.start(["--single"])
        # Confirm the leader is up as the sole voter and is itself the leader.
        ctx.leader.wait_status(
            lambda s: s["state"] == "Leader"
            and s["current_leader"] == 1
            and s["voters"] == [1],
            "leader is up as a single-voter cluster",
        )

        fed = ctx.leader.feed_data(range(0, 10))

        ctx.leader.trigger_snapshot()
        # Block until the leader's state matches the precondition the snapshot
        # transfer test depends on: a snapshot exists, post-snapshot logs are
        # applied on top, and old logs were purged (`--max-applied-log-to-keep
        # 0` plus the snapshot make `purged_index >= snapshot_index`). If any
        # of these fail, the follower can catch up via log replay alone and
        # the cross-version InstallSnapshot path is never exercised.
        fed.update(ctx.leader.feed_data(range(10, 20)))
        ctx.leader.wait_status(
            lambda s: (s["snapshot_index"] or 0) > 0
            and (s["last_applied_index"] or 0) > (s["snapshot_index"] or 0)
            and (s["purged_index"] or 0) >= (s["snapshot_index"] or 0),
            "leader has snapshot, post-snapshot logs applied, old logs purged",
        )

        ctx.follower.start(["--join", ctx.leader.raft_addr])
        ctx.follower.wait_status(
            lambda s: sorted(s["voters"]) == [1, 2],
            "follower joined cluster as voter",
        )
        # Strict-inequality proof of cross-version snapshot transfer:
        # snapshot covers an earlier index, post-snapshot logs replicated
        # on top. See class docstring for the failure modes this rules out.
        ctx.follower.wait_status(
            lambda s: (s["snapshot_index"] or 0) > 0
            and (s["last_applied_index"] or 0) > (s["snapshot_index"] or 0),
            "follower received snapshot via InstallSnapshot, "
            "with later logs applied on top (snapshot_index < last_applied_index)",
        )
        return fed


class RestartReplication(TestCase):
    """Cross-version AppendEntries replication survives a full restart.

    Both nodes are stopped and restarted with their persistent state intact.
    On recovery the cluster must still replicate writes from leader to
    follower — exercising AppendEntries against state loaded from disk.
    Run in both directions so log decoding is exercised on each version.

    Note: openraft's leader-continuity optimization lets a node with a
    committed Vote resume leadership at the same term without a fresh
    RequestVote, so this restart pattern does not reliably trigger the vote
    RPC. Forcing a real cross-version vote needs leader transfer or a 3-node
    quorum-loss scenario.
    """

    def run(self, ctx: TestContext) -> dict[str, str]:
        ctx.leader.start(["--single"])
        ctx.follower.start(["--join", ctx.leader.raft_addr])
        ctx.follower.wait_status(
            lambda s: sorted(s["voters"]) == [1, 2] and s["current_leader"] is not None,
            "initial 2-voter cluster formed with a leader",
        )

        fed = ctx.leader.feed_data(range(0, 5))
        pre_restart_applied = ctx.follower.wait_status(
            lambda s: (s["last_applied_index"] or 0) >= 5,
            "follower applied first 5 writes before restart",
        )["last_applied_index"]

        ctx.stop_all()

        ctx.leader.start(["--single"])
        ctx.follower.start(["--join", ctx.leader.raft_addr])
        ctx.follower.wait_status(
            lambda s: s["current_leader"] is not None
            and (s["last_applied_index"] or 0) >= pre_restart_applied,
            f"cluster recovered after restart with applied >= {pre_restart_applied}",
        )

        fed.update(ctx.leader.feed_data(range(5, 10)))
        ctx.follower.wait_status(
            lambda s: (s["last_applied_index"] or 0) >= pre_restart_applied + 5,
            "follower received cross-version AppendEntries for post-restart writes",
        )
        return fed


TESTS: list[TestCase] = [
    SnapshotReplication(),
    RestartReplication(),
]


# Direction polarity. Each value resolves an old version into the
# `(leader_version, follower_version)` pair to use for that direction.
# Keeping this as a dict-of-callables (rather than a fixed pair table) lets
# us model "current vs every old version" without explicitly listing every
# combination.
DIRECTIONS: dict[str, Callable[[str], tuple[str, str]]] = {
    "current-to-old": lambda old: (CURRENT_VERSION, old),
    "old-to-current": lambda old: (old, CURRENT_VERSION),
}


def build_binaries(skip_build: bool) -> None:
    """Build every discovered compat workspace.

    Iterates `BINARIES` rather than a hardcoded list, so newly added
    `bin-v<tag>/` directories are picked up automatically.
    """
    if skip_build:
        step("build", "skipped")
        return

    for version, binary in BINARIES.items():
        # `binary` is `<crate_dir>/target/debug/databend-meta-compat`, so
        # the workspace root is three parents up.
        crate_dir = binary.parents[2]
        step("build", f"{version} ...")
        result = subprocess.run(
            ["cargo", "build", "--manifest-path", str(crate_dir / "Cargo.toml")],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"Failed to build {version}")
        step("build", f"{version} ✓")


def main():
    if not OLD_VERSIONS:
        raise RuntimeError(
            f"no old versions discovered under {SCRIPT_DIR}; expected at "
            f"least one `{BIN_DIR_PREFIX}v<tag>/` directory alongside "
            f"`{BIN_DIR_PREFIX}{CURRENT_VERSION}/`"
        )

    parser = argparse.ArgumentParser(description="E2E compat test for databend-meta")
    parser.add_argument("--skip-build", action="store_true", help="Skip cargo build step")
    parser.add_argument(
        "--test",
        choices=[t.name() for t in TESTS],
        help="Run only the specified test (default: run all)",
    )
    parser.add_argument(
        "--old-version",
        choices=OLD_VERSIONS,
        help=(
            "Restrict to one old version (default: all discovered "
            f"`{BIN_DIR_PREFIX}v*` directories: {OLD_VERSIONS})"
        ),
    )
    parser.add_argument(
        "--direction",
        choices=[*DIRECTIONS.keys(), "both"],
        default="both",
        help=(
            "Restrict which direction(s) to run. `current-to-old` puts the "
            "current build as leader and the old version as follower; "
            "`old-to-current` swaps. Default `both` runs each test in both "
            "directions."
        ),
    )
    parser.add_argument(
        "--keep-data",
        action="store_true",
        help=(
            "Don't delete the .test-data directory after the run; useful for "
            "post-mortem inspection of exports, raft dirs, and node logs. "
            "Note: each (test, old version, direction) tuple wipes data at "
            "its own start, so only the last run's data is preserved. Pair "
            "with --test, --old-version, and --direction to pin which run's "
            "artifacts are kept."
        ),
    )
    args = parser.parse_args()

    os.environ["RUST_BACKTRACE"] = "full"

    build_binaries(args.skip_build)

    selected_tests = (
        TESTS if args.test is None else [t for t in TESTS if t.name() == args.test]
    )
    selected_olds = (
        OLD_VERSIONS if args.old_version is None else [args.old_version]
    )
    selected_dirs = (
        list(DIRECTIONS.keys()) if args.direction == "both" else [args.direction]
    )

    # Test-major, then old-version-major ordering: each test sweeps every
    # old version, and within each old version both directions run before
    # moving on. Reads in the log as a coherent block per (test, old).
    last_ctx: Optional[TestContext] = None
    total = len(selected_tests) * len(selected_olds) * len(selected_dirs)

    try:
        for test in selected_tests:
            for old in selected_olds:
                for direction in selected_dirs:
                    leader_v, follower_v = DIRECTIONS[direction](old)
                    ctx = TestContext(leader_v, follower_v)
                    last_ctx = ctx
                    test.execute(ctx)
        hr(f"ALL TESTS PASSED ({total}/{total})")
    finally:
        # `stop_all` always runs so we don't leak processes; data wipe is
        # gated on --keep-data so failures or explicit requests can be
        # inspected after the run.
        if last_ctx is not None:
            last_ctx.stop_all()
            if args.keep_data:
                banner(f"test data preserved at {last_ctx.data_dir}")
            else:
                last_ctx.cleanup()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        banner("INTERRUPTED")
        sys.exit(1)
    except Exception as e:
        banner(f"FAILED: {e}")
        sys.exit(1)
