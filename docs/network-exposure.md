# Network Exposure

A `databend-meta` node listens on three ports. They differ in what they check,
and none of them is safe to reach from outside the cluster's trusted network.

| Port  | Config key          | Serves                                | Checks the caller           |
|-------|---------------------|---------------------------------------|-----------------------------|
| raft  | `raft_api_port`     | replication between nodes             | the shared secret, if configured |
| gRPC  | `grpc_api_address`  | the key-value API, watch, export      | a handshake token -- see below |
| admin | `admin_api_address` | health, config, metrics, control      | nothing                     |

## The raft port

Raft RPCs carry a shared secret once the cluster is configured for one. See
[raft-secret-rollout.md](raft-secret-rollout.md) for what that secret covers,
how to roll it out without downtime, and why it does not replace a trusted
network: raft connects over cleartext `http://`, so an adversary who can read
the wire takes the secret out of any RPC and replays it.

This port has no TLS option at all.

## The gRPC port

Every RPC except `handshake` requires the token that `handshake` returns, so a
client that never handshakes reaches nothing -- not the key-value API, not
`watch`, not `export`.

That is a smaller guarantee than it sounds. `handshake` issues a token to any
caller presenting the username `root`. The password travels in the request and
is never compared against anything, because there is no user table to compare
it with. The token gate therefore stops a caller that does not speak the
handshake -- a port scanner, a misconfigured tool, an old client -- and stops
nobody who chooses to speak it.

Until that changes, treat this port as reachable-means-full-access: whoever can
open a connection can read and write every key in the store, and can stream the
whole store out with `export`.

## The admin port

Nothing on this port is authenticated. Alongside the read-only endpoints
(`/v1/health`, `/v1/config`, `/v1/cluster/status`, `/v1/cluster/nodes`,
`/v1/metrics`) it serves three that change the cluster:

- `/v1/ctrl/trigger_snapshot`
- `/v1/ctrl/trigger_transfer_leader` -- moves leadership to a node the caller names
- `/v1/features/set`

and `/debug/pprof/profile`, which profiles the running process on request.

A single unauthenticated GET can therefore move the leader. Keep this port on a
management network that no workload can reach.

## What TLS buys here

The gRPC and admin ports each accept a server certificate and key. Both
configure the server identity only; neither asks the client for a certificate.
TLS on these ports is transport encryption and nothing more -- it stops someone
reading the wire, and does not narrow who may connect.

## Data at rest and in backups

Nothing the meta service writes is encrypted. That covers the raft log and
state machine on disk, the snapshot files (including the ones shipped to a
joining node over the cleartext raft port), and the JSON that
`databend-metactl export` produces, which contains every key and value
verbatim.

Databend keeps stage and connection definitions in meta, and those hold
object-storage credentials. A snapshot file or an export dump is therefore a
credential file. Hold it to the same standard as the credentials inside it, and
do not park backups anywhere the meta cluster's own trust boundary does not
already cover.
