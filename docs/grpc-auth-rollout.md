# gRPC Authentication Rollout

The gRPC handshake can verify configured username and password pairs before it
issues the token used by the key-value, watch, and export APIs. Roll the check
out in permissive mode first so query nodes that still send no password keep
working.

## Configuration

The `databend-meta` binary reads an optional `grpc_auth` table. Its credential
list must not be empty, every username and password must be nonempty, and each
username must be unique. Invalid configurations stop the server at startup.

Generate a long random password and store it in the deployment's secret
manager. Render the meta-server config with restrictive file permissions:

```toml
[grpc_auth]
strict = false

[[grpc_auth.credentials]]
username = "root"
password = "<plain password from the secret manager>"
```

Authentication is config-file only, so plaintext passwords cannot leak through
command-line process listings. Config logs, the admin config endpoint, and
`--cmd show-config` render every password as `***`.

Query nodes keep using their existing client settings and send the plain
password in the handshake:

```toml
[meta]
username = "root"
password = "<plain password from the secret manager>"
```

Protect the connection with gRPC TLS. Basic authentication carries the
password in the request; plaintext gRPC exposes it to anyone who can read the
network.

## Phase 1: permissive server

Configure the table above with `strict = false`, then restart the meta nodes one
at a time. A correct password is verified normally. A known
username with a missing or wrong password is still accepted, and each such
handshake increments one of these counters:

```text
metasrv_meta_network_unauthenticated_passed_total{reason="missing"} 7
metasrv_meta_network_unauthenticated_passed_total{reason="incorrect"} 2
```

Each accepted mismatch also writes a warning containing the reason and remote
address. It never writes the configured or received password.

Configure the same username and plain password on every query node, then
restart those nodes. Unknown usernames are rejected throughout the rollout, so
keep `root` as the server username until every existing client is ready to use
another name.

Scrape `/v1/metrics` from every meta node twice, several minutes apart. Phase 1
is complete only when both counters stop growing on every node. A series that
never fired is absent rather than reported as zero.

## Phase 2: strict server

After the permissive counters stop growing, change `strict` to `true` in the
existing `[grpc_auth]` table.

Restart the meta nodes one at a time. Correctly configured query nodes continue
to connect. A known username with a missing or wrong password now receives
`Unauthenticated`, and no token is issued. An unknown username is rejected in
both modes.

## Credential rotation

Strict mode can rotate credentials without downtime. Add a second pair with a
new username while keeping the current pair:

```toml
[[grpc_auth.credentials]]
username = "root-next"
password = "<new plain password from the secret manager>"
```

Roll the server config through the meta nodes, then update query nodes to use
the new pair. Successful handshakes increment a counter for the matched
username:

```text
metasrv_meta_network_authenticated_total{username="root"} 42
metasrv_meta_network_authenticated_total{username="root-next"} 9
```

Remove the old pair only after its counter stops growing across every meta node
for a normal client reconnect window. Existing connections do not handshake
again until they reconnect.

## Rollback

Change `strict` to `false` in the existing `[grpc_auth]` table and roll that
change through every meta node before removing client passwords or credentials
from the servers. Removing the entire `grpc_auth` table restores the old
`root`-only username check and ignores passwords, so it must not be the first
rollback step.

## Limitation

These are shared credentials, not per-query-node identities. They stop direct
access by a caller without a configured pair. They do not stop a caller that
can read a query node's configuration and replay its username and password.
