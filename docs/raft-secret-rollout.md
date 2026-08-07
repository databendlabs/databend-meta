# Raft Secret Rollout

The raft port carries no authentication of its own: whoever can open a
connection to it can forward arbitrary writes to the leader, read the whole
store, or install a snapshot over the state machine. The cluster shared secret
closes that hole. Each node attaches the secret to every raft RPC it sends, and
checks the secret on every raft RPC it receives.

Turning the check on has to happen after every node is already sending the
secret. This document is the order that makes that safe, and what to watch at
each step.

## Configuration

| Key | Meaning |
|:--- |:------- |
| `raft_secret` | The single secret this node attaches to the RPCs it sends. Unset sends nothing. |
| `raft_accepted_secrets` | The list of secrets this node accepts on the RPCs it receives. Empty accepts everything. |
| `raft_secret_strict` | Whether to reject a received RPC carrying no or an unaccepted secret. Default `false`. |

All three live under `[raft_config]` in the config file:

```toml
[raft_config]
raft_secret = "s3cr3t"
raft_accepted_secrets = ["old-secret", "s3cr3t"]
raft_secret_strict = true
```

Each also has a command line argument, which overrides the config file:

```bash
databend-meta --raft-secret s3cr3t \
              --raft-accepted-secrets old-secret --raft-accepted-secrets s3cr3t
```

Prefer the config file. A secret passed as a command line argument is visible
to every user on the host through `ps`.

A secret is visible ASCII with no space: letters, digits and punctuation in the
range `!` to `~`. It travels verbatim as an HTTP header value, which cannot
carry a control character at all, renders a byte above 127 in a way HTTP has
deprecated, and may gain or lose a space at either edge.

Three settings are refused at startup, so a node cannot come up half
configured: an empty secret in either key, a secret outside that character
range, and `raft_secret_strict` with an empty `raft_accepted_secrets` — a node
in that state would reject every incoming raft RPC, including its own peers'.

## Phase 1: everyone sends, nobody requires

Set on every node, then restart the nodes one at a time:

```toml
[raft_config]
raft_secret = "s3cr3t"
raft_accepted_secrets = ["s3cr3t"]
raft_secret_strict = false
```

The cluster keeps serving throughout. A node that has already restarted sends
the secret to peers that do not know the key yet; an HTTP/2 header nobody reads
is not an error, so those peers ignore it. A node that has not restarted yet
sends nothing, and the restarted peers let it through because `strict` is off.

Every RPC that got through without an accepted secret is counted:

```
metasrv_raft_network_unauthenticated_passed_total{reason="missing"}
metasrv_raft_network_unauthenticated_passed_total{reason="unaccepted"}
```

Scrape it from the admin API of each node, `/v1/metrics` (default
`127.0.0.1:28002`). The peer responsible is not a metric label — that would let
an unauthenticated caller drive the label cardinality — it goes to the log
instead, at WARN, once per peer per 10 seconds:

```
raft secret is missing: from:10.0.0.7:41244: accepted because `raft_secret_strict` is off; ...
```

`reason="missing"` means the peer sent no secret, which during phase 1 is the
expected reading for nodes not yet restarted. `reason="unaccepted"` means the
peer sent a secret this node does not accept, which is never expected: either
the configuration disagrees between nodes, or the caller is not part of this
cluster.

## Phase 2: require the secret

The precondition is that both counters have stopped growing on every node.
Compare a scrape against one taken a few minutes earlier; equal totals mean no
raft RPC is arriving without an accepted secret any more. A counter that never
fired is absent from the scrape rather than reported as zero, so a missing
series is the reading to hope for.

Then set `raft_secret_strict = true` on every node and restart them one at a
time. Since every node is already sending an accepted secret, the flip changes
nothing observable.

## Why phase 1 cannot be skipped

A node that refuses an RPC answers `Unauthenticated`. The raft network layer
turns every gRPC status into `Unreachable` without inspecting the code — see
`status_to_unreachable_at()` in `crates/server/service/src/network.rs` — so raft
never sees an authentication failure. It sees a peer that is down.

The symptom of turning on `raft_secret_strict` too early is therefore not a log
full of authentication failures on an otherwise healthy cluster. It is a cluster
whose nodes all consider each other unreachable and elect leaders in a loop. No
data is lost, but the cluster stops serving until the configuration is
corrected.

This is also why the counter must read zero before phase 2, rather than merely
low: whatever is still being counted is exactly what phase 2 would evict.

## Rollback

From phase 1, roll back freely — to the previous binary, or by removing the
keys. Nothing requires the secret yet.

From phase 2, set `raft_secret_strict = false` first and roll that out, which
returns the cluster to the phase 1 state. Only then remove the secret or
downgrade the binary. Rolling back a node to a binary that does not send the
secret while its peers are still strict is the failure described above.

## Rotating the secret

Rotation is the same shape as the initial rollout: make the new secret
acceptable everywhere before anyone sends it. Each step is a rolling restart of
the cluster.

1. Add the new secret to `raft_accepted_secrets` on every node, keeping the old
   one. Every node now accepts both; every node still sends the old one.
2. Change `raft_secret` to the new secret, node by node.
3. Remove the old secret from `raft_accepted_secrets` on every node.

Do not merge steps. Between step 1 and step 3, `reason="unaccepted"` counting up
means some node is sending a secret that another node has not been told to
accept, and step 3 would turn that into an eviction.

## Handling the secret

The secret is a credential for full write access to the cluster. The config
file holding it must not be committed to version control: generate or template
it at deploy time from whatever the deployment uses to hold secrets, and keep
it out of any config that is checked in.

`databend-meta` will not print it. The config is logged at startup and dumped by
`--cmd show-config`, and both render the secret as `***`. Nothing logs the value
a peer presented either, since on a misconfigured peer that value is a valid
secret of some other cluster.
