# Network Exposure

A `databend-meta` node listens on three ports and, when configured, a second
Raft port for TLS. They differ in what they check, and none of them is safe to
reach from outside the cluster's trusted network.

| Port     | Config key          | Serves                                | Checks the caller           |
|----------|---------------------|---------------------------------------|-----------------------------|
| raft     | `raft_api_port`     | replication between nodes             | the shared secret, if configured |
| raft TLS | `raft_tls_port`     | the same replication service, over TLS | the same shared secret, if configured |
| gRPC     | `grpc_api_address`  | the key-value API, watch, export      | a configured username/password pair |
| admin    | `admin_api_address` | health, config, metrics, control      | nothing                     |

## The raft ports

The plaintext and TLS listeners serve the same Raft RPCs and apply the same
shared-secret check. TLS changes only the transport; it does not replace caller
authentication.

See [raft-secret-rollout.md](raft-secret-rollout.md) for how to configure the
secret. The plaintext listener uses cleartext `http://`, so an adversary who
can read the wire takes the secret out of any RPC and replays it. See
[raft-tls-rollout.md](raft-tls-rollout.md) for how to add the TLS listener
without downtime.

## The gRPC port

Every RPC except `handshake` requires the token that `handshake` returns. When
gRPC credentials are configured, the server verifies the username/password
pair before signing that token. A strict server rejects a missing or wrong
password. A permissive server lets it through, warns, and counts it so that old
clients can be upgraded without downtime.

With no credentials configured, the server keeps the old behavior: username
`root` is accepted and the password is ignored. This compatibility default is
not access control. See [grpc-auth-rollout.md](grpc-auth-rollout.md) for the
configuration and the order that safely turns strict checking on.

Configured credentials are shared by query nodes. They block a caller that
cannot read query configuration, but a caller that steals that configuration
can reuse its credential. Keep this port inside the trusted cluster network
even after authentication is strict.

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
TLS stops someone reading the wire. On gRPC, the separate shared-password check
narrows who may obtain a token; TLS itself still does not authenticate the
client. The admin port remains unauthenticated.

## Data at rest and in backups

Nothing the meta service writes is encrypted. That covers the raft log and
state machine on disk, the snapshot files (including the ones shipped to a
joining node), and the JSON that
`databend-metactl export` produces, which contains every key and value
verbatim.

Databend keeps stage and connection definitions in meta, and those hold
object-storage credentials. A snapshot file or an export dump is therefore a
credential file. Hold it to the same standard as the credentials inside it, and
do not park backups anywhere the meta cluster's own trust boundary does not
already cover.
