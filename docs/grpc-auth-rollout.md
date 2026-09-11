# gRPC Authentication Rollout

The gRPC handshake can verify one server-side username and password before it
issues the token used by the key-value, watch, and export APIs. Roll the check
out in permissive mode first so query nodes that still send no password keep
working.

## Configuration

The `databend-meta` binary reads these top-level settings:

| Key | Meaning |
|:--- |:------- |
| `grpc_auth_username` | The username accepted by the server. |
| `grpc_auth_password_hash` | Lowercase hexadecimal SHA-256 hash of the accepted password. |
| `grpc_auth_strict` | Whether a missing or wrong password is rejected. Default `false`. |

The username and hash must be set together. Neither may be empty, and strict
mode cannot be enabled without both. Invalid combinations stop the server at
startup.

Generate a long random password, store the plain value in the deployment's
secret manager, and hash the exact bytes without a trailing newline:

```bash
read -r -s PASSWORD
printf '%s' "$PASSWORD" | openssl dgst -sha256 -r | cut -d' ' -f1
unset PASSWORD
```

Put only the resulting hash in the meta-server config:

```toml
grpc_auth_username = "root"
grpc_auth_password_hash = "<64 lowercase hexadecimal characters>"
grpc_auth_strict = false
```

The settings also have `--grpc-auth-*` command-line forms. Prefer the config
file: a hash passed on the command line is visible through `ps`. Config logs,
the admin config endpoint, and `--cmd show-config` render the hash as `***`.

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

Set the three server values above with `grpc_auth_strict = false`, then restart
the meta nodes one at a time. A correct password is verified normally. A known
username with a missing or wrong password is still accepted, and each such
handshake increments one of these counters:

```text
metasrv_meta_network_unauthenticated_passed_total{reason="missing"} 7
metasrv_meta_network_unauthenticated_passed_total{reason="incorrect"} 2
```

Each accepted mismatch also writes a warning containing the reason and remote
address. It never writes the password or configured hash.

Configure the same username and plain password on every query node, then
restart those nodes. Unknown usernames are rejected throughout the rollout, so
keep `root` as the server username until every existing client is ready to use
another name.

Scrape `/v1/metrics` from every meta node twice, several minutes apart. Phase 1
is complete only when both counters stop growing on every node. A series that
never fired is absent rather than reported as zero.

## Phase 2: strict server

After the permissive counters stop growing, set:

```toml
grpc_auth_strict = true
```

Restart the meta nodes one at a time. Correctly configured query nodes continue
to connect. A known username with a missing or wrong password now receives
`Unauthenticated`, and no token is issued. An unknown username is rejected in
both modes.

## Rollback

Set `grpc_auth_strict = false` and roll that change through every meta node
before removing client passwords or credentials from the servers. Removing the
credentials restores the old `root`-only username check and ignores passwords,
so it must not be the first rollback step.

## Limitation

This is one shared credential, not per-query-node identity. It stops direct
access by a caller without the credential. It does not stop a caller that can
read a query node's configuration and replay the same username and password.
