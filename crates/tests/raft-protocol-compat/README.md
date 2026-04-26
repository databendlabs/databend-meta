# Raft Protocol Backward Compatibility Test

Verifies that the working source tree (`bin-current`) stays
**raft-protocol-compatible** with one or more pinned released versions
(`bin-v<TAG>`): the wire formats for `RequestVote`, `AppendEntries`,
`InstallSnapshot`, the persisted on-disk raft log, and the on-disk
state-machine snapshot must all interoperate across versions. Each pair
runs **in both directions** — either version may be leader, with the other
as follower — to catch forward- and backward-compatibility regressions that
single-version unit tests cannot detect.

## Run

```bash
make test-raft-protocol-compat                               # build + run full matrix
make raft-protocol-compat-build                              # build only

python3 test_meta_meta.py --skip-build                       # run without rebuild
python3 test_meta_meta.py --test SnapshotReplication          # one test, all old versions
python3 test_meta_meta.py --test RestartReplication
python3 test_meta_meta.py --test ElectionAfterLeaderLoss
python3 test_meta_meta.py --test LaggedFollowerLogCatchup
python3 test_meta_meta.py --old-version v1.2.873              # one old version, both directions
python3 test_meta_meta.py --old-version v260205.4.0
python3 test_meta_meta.py --direction current-to-old          # current is leader
python3 test_meta_meta.py --direction old-to-current          # old version is leader
python3 test_meta_meta.py --keep-data                         # preserve .test-data
```

Each test is a `TestCase` subclass; the class name is the canonical
identifier used by `--test`, in the run banner, and in source. A full
run produces `2 × len(OLD_VERSIONS) × len(TESTS)` outcomes; the banner
appends a `[leader→follower]` direction tag, e.g.
`SnapshotReplication[current→v260205.4.0]` and
`SnapshotReplication[v260205.4.0→current]`.

## Structure

```
crates/tests/raft-protocol-compat/
├── bin-current/         # standalone workspace, path deps on the working source tree
├── bin-v1.2.873/        # standalone workspace, git deps pinned to v1.2.873
├── bin-v260205.4.0/     # standalone workspace, git deps pinned to v260205.4.0
├── bin-v<TAG>/          # add more old versions by creating directories here
└── test_meta_meta.py    # orchestrator (auto-discovers bin-current + bin-v*)
```

Every binary exposes the same CLI (`serve`, `upsert`, `export`) and an
admin HTTP server with `GET /v1/ctrl/status` returning a stable JSON view
of `RaftMetrics` (`current_term`, `last_applied_index`, `snapshot_index`,
`voters`, …) so the orchestrator can wait for specific raft events instead
of sleeping. `/v1/ctrl/status` lives on **every** binary — it is what
lets the orchestrator drive the test from either end, regardless of which
version is leader.

Each `main.rs` is independent, so divergent APIs across versions are
handled by editing each file separately. Do not use `include!` to share a
shim implementation across version directories; re-create the file for the
target tag so API divergence stays local and reviewable. `bin-current`
always tracks the working tree; `bin-v<TAG>` files only need to match the
API as it existed at that tag.

## Tests

Each test runs once per `(old version, direction)` pair. In
`current→<old>`, node 1 starts with the current binary and node 2 starts
with the old binary; `<old>→current` swaps the assignment. Most tests use
that 2-node leader/follower shape. Tests that need quorum behavior may add
more voters, but they still verify compatibility through cluster state and
exported data rather than by asserting that a specific RPC endpoint was
called. After the test body, live nodes are exported and checked for every
expected KV pair plus cross-node KV agreement.

**`SnapshotReplication`** — leader builds a snapshot; follower installs
it via cross-version InstallSnapshot RPC.

1. Start leader; feed 10 KV pairs.
2. Trigger snapshot on leader; feed 10 more.
3. Start follower (`--join`).
4. Wait for `voters == [1, 2]` and the strict inequality
   `0 < snapshot_index < last_applied_index` on the follower.

`--max-applied-log-to-keep 0` lets the leader purge its pre-snapshot logs,
so the follower can only catch up via InstallSnapshot for that range. The
strict inequality proves both: a snapshot was actually transferred
(`snapshot_index > 0`) and post-snapshot AppendEntries replayed on top of
it (`last_applied_index > snapshot_index`). Running in both directions
exercises snapshot serialization both ways: each version must be able to
read the other version's on-disk snapshot format.

**`RestartReplication`** — cluster survives a full restart and continues
to replicate cross-version.

1. Start leader + follower; feed 5 KV pairs.
2. Stop both; restart both with the same data dirs.
3. Wait for follower's `last_applied_index` to match pre-restart value.
4. Feed 5 more; wait for follower to apply them.

This restart pattern doesn't reliably trigger a vote RPC — openraft's
leader-continuity optimization lets a restarted leader resume at the same
term without a fresh `RequestVote`. A real cross-version vote test would
need leader transfer or a 3-node quorum-loss scenario.

**`ElectionAfterLeaderLoss`** — a 3-voter mixed-version cluster elects a
new leader after the original leader is stopped.

1. Start leader, follower, and a third voter.
2. Write data and wait for the two non-leader voters to apply it.
3. Stop the original leader.
4. Wait for the two surviving voters, one current and one old, to elect a
   new leader with a higher term.
5. Write through the new leader and verify the remaining survivor applies
   the post-failover data.

This is the black-box compatibility check for cross-version `RequestVote`:
the test never calls or inspects the vote RPC directly, but a successful
post-failover write requires the surviving mixed-version quorum to complete
an election and then continue log replication.

**`LaggedFollowerLogCatchup`** — a follower joins after the leader has
already committed data, and catches up via raft log replay without a
snapshot.

1. Start a single leader.
2. Write enough data to create historical raft log entries, but do not
   trigger a snapshot.
3. Start the follower and wait for it to apply the leader's existing log.
4. Write more data and verify live replication still works.

The load-bearing assertion is `snapshot_index == 0` on both sides while the
follower reaches the leader's applied index. That distinguishes historical
AppendEntries log catch-up from snapshot installation.

## Adding a new old version

The orchestrator auto-discovers any `bin-v*` directory, so adding a version
to the matrix is a standalone workspace creation:

```bash
cd crates/tests/raft-protocol-compat
mkdir -p bin-v<NEW_TAG>/src
$EDITOR bin-v<NEW_TAG>/Cargo.toml
$EDITOR bin-v<NEW_TAG>/src/main.rs
make test-raft-protocol-compat
```

`src/main.rs` must be a full independent file for that tag. Do not point it
at another version with `include!`; use existing shims only as reference
material when re-creating the new file.

Then add the new workspace to the CI cache scope so subsequent runs reuse
its target/: edit `.github/workflows/ci.yml`, find the
`raft-protocol-compat` job's `Swatinem/rust-cache` step, and append
`crates/tests/raft-protocol-compat/bin-v<NEW_TAG>` to the `workspaces:`
list. The job itself picks up the new directory automatically (it just
runs `make test-raft-protocol-compat`); only the cache scope needs the
explicit entry.

The version label in test output and CLI flags is taken from the directory
name with the `bin-` prefix stripped — keep the `v` prefix and dotted
version style so labels remain unambiguous (e.g. `v260308.5.0/2` in
`[v260308.5.0/2]`).

## Replacing the old version (single-version setups)

When there is exactly one `bin-v<TAG>` and you want to bump it instead of
adding a new entry, edit `Cargo.toml` in place to change the tag. If the
API at the new tag has diverged from the previous one, edit `src/main.rs`
to match. Optionally rename the directory so the on-disk name matches the
tag.
