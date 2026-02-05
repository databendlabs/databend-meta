# Compatibility Algorithm

This document describes how minimum compatible versions are calculated between
meta-client and meta-server during feature negotiation.

## What is Databend Meta?

Databend Meta is a distributed metadata service that stores and manages metadata
for the Databend database system. It provides a key-value store with transaction
support, watch capabilities, and cluster management features.

The system consists of two main components:

- **meta-server**: The server process that stores metadata and handles requests
- **meta-client**: A library used by other services (like query nodes) to communicate with meta-server

## Why Version Compatibility Matters

Databend Meta supports **rolling upgrades**, where servers and clients may run
different versions simultaneously during deployment. This creates compatibility
challenges:

- A newer client may require features that an older server doesn't provide
- A newer server may have removed features that an older client still needs

The compatibility algorithm calculates the **minimum version** that the other
side must have for successful communication.

### Deployment Order

There is **no fixed deployment order** - either servers or clients can be upgraded
first. The compatibility algorithm ensures that as long as versions are within
the supported range, communication works regardless of upgrade order.

## Feature Negotiation

Feature negotiation happens during the **gRPC handshake** when a client connects
to the server. The process uses bidirectional streaming:

```
Client                                    Server
  |                                         |
  |------- HandshakeRequest --------------->|
  |        (client protocol_version)        |
  |                                         |
  |        [Server checks: client_ver >= min_client_ver]
  |                                         |
  |<------ HandshakeResponse ---------------|
  |        (server protocol_version)        |
  |                                         |
  [Client checks: server_ver >= min_server_ver]
  |                                         |
  |------- Authenticated requests --------->|
```

See the `handshake()` function in [`grpc_client.rs`](../../client/src/grpc_client.rs)
for implementation details.

### What Happens When Negotiation Fails

When versions are incompatible, the handshake fails with a `MetaHandshakeError`:

```
MetaHandshakeError: Invalid: server protocol_version((1, 2, 500)) < client required((1, 2, 677)) for feature watch/initial_flush
```

The client **will not send any further requests** after a failed handshake.
The connection is effectively rejected.

### What is a Feature?

A feature represents a specific capability in the protocol. Examples:

| Feature | Description |
|---------|-------------|
| `KvReadV1` | Stream API for reading key-value pairs |
| `Transaction` | Support for multi-key atomic operations |
| `WatchInitialFlush` | Watch stream flushes existing keys at start |
| `ExportV1` | Enhanced export API with configurable chunk size |

See [`features.rs`](spec.rs) for the complete feature list and their definitions.

### Feature Lifecycle

Each feature has a **lifetime** defined by two versions:

- `since`: The version when the feature was added (inclusive)
- `until`: The version when the feature was removed (exclusive), or `Version::max()` if still active

A feature is **active** at version V when: `since <= V < until`

This uses **half-open interval** notation `[since, until)`:
- `[1.2.100, 1.2.200)` means versions 1.2.100 through 1.2.199 (not including 1.2.200)
- `[1.2.100, ∞)` means version 1.2.100 and all later versions

### Why Features Get Removed

Features are removed when:
- The API is replaced by a better alternative (e.g., `KvApiGetKv` → `KvReadV1`)
- The feature is deprecated and no longer needed
- Protocol simplification removes legacy support

### Deprecation Policy

The deprecation window is approximately **3-6 months** between when a client stops
using a feature and when the server removes it. This gives users sufficient time
to upgrade their clients before server support is dropped.

Typical deprecation timeline:
1. **T+0**: New API introduced (e.g., `KvReadV1` replaces `KvApiGetKv`)
2. **T+1-2 months**: Client updated to use new API, stops requiring old feature
3. **T+3-6 months**: Server removes old feature support

## Algorithm Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Compatibility Check                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Client v1.2.800                      Server v1.2.750          │
│   ┌─────────────────┐                  ┌─────────────────┐      │
│   │ Requires:       │                  │ Provides:       │      │
│   │  - KvReadV1     │    ─────────►    │  - KvReadV1 ✓   │      │
│   │  - Transaction  │    negotiate     │  - Transaction ✓│      │
│   │  - WatchFlush   │                  │  - WatchFlush ✓ │      │
│   └─────────────────┘                  └─────────────────┘      │
│                                                                 │
│   min_server_version = max(server.since for required features)  │
│   min_client_version = max(client.until for removed features)   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Minimum Compatible Server Version

**Question:** For a client at version C, what's the minimum server version S that can serve it?

**Answer:** The server must provide all features the client requires.

### Algorithm

```
min_server = Version::min()  // (0, 0, 0)

for each feature F:
    if client requires F at version C:      // client.since <= C < client.until
        min_server = max(min_server, server.since[F])

return min_server
```

### Example

*Illustrative example with a subset of features:*

| Feature      | Client requires       | Server provides       |
|--------------|----------------------|-----------------------|
| KvReadV1     | [1.2.176, ∞)         | [1.2.163, ∞)         |
| Transaction  | [1.2.259, ∞)         | [1.2.258, ∞)         |
| WatchInitFlush| [1.2.726, ∞)        | [1.2.677, ∞)         |

For a hypothetical client version 1.2.800 with only these features:
- Client requires KvReadV1 → server needs ≥ 1.2.163
- Client requires Transaction → server needs ≥ 1.2.258
- Client requires WatchInitFlush → server needs ≥ 1.2.677

Result: `max(1.2.163, 1.2.258, 1.2.677)` = **1.2.677**

*Note: The actual computed value includes all features. At version 1.2.873,
`min_compatible_server_version()` returns 1.2.770 due to features like
`ExpireInMillis` and `PutSequential` which the client requires and server
provides starting from that version.*

## Minimum Compatible Client Version

**Question:** For a server at version S, what's the minimum client version C that can connect?

**Answer:** For features the server has removed, the client must have stopped requiring them.

### Algorithm

```
min_client = Version::min()  // (0, 0, 0)

for each feature F:
    if server removed F at version S:       // S >= server.until
        min_client = max(min_client, client.until[F])

return min_client
```

### Example

*Illustrative example showing features that have been removed:*

| Feature      | Server provides       | Client requires       |
|--------------|----------------------|-----------------------|
| KvApiGetKv   | [1.2.163, 1.2.663)   | [1.2.163, 1.2.287)   |
| TxnReplyErr  | [1.2.258, 1.2.755)   | [1.2.258, 1.2.676)   |

For server version 1.2.873:
- Server removed KvApiGetKv (at 1.2.663) → client must have stopped requiring it
- Client stopped at 1.2.287, so clients ≥ 1.2.287 are compatible
- Server removed TxnReplyErr (at 1.2.755) → client must have stopped at 1.2.676

Result: `max(1.2.287, 1.2.676)` = **1.2.676**

*At version 1.2.873, `min_compatible_client_version()` returns exactly 1.2.676,
matching this example since these are the only features removed by that version.*

## Usage

### Calculating Compatible Versions

```rust
use databend_meta_version::Spec;

// Load feature history
let spec = Spec::load();

// For the current build version, find minimum compatible versions
let min_server = spec.min_compatible_server_version();
let min_client = spec.min_compatible_client_version();

println!("Current version: {:?}", spec.version());
println!("Minimum compatible server: {:?}", min_server);
println!("Minimum compatible client: {:?}", min_client);
```

### Adding a New Feature

To add a new feature to the system:

1. Add a variant to the `Feature` enum in [`features.rs`](spec.rs)
2. Add the feature to `Feature::all()` and implement `as_str()`
3. Add server and client lifetimes in `Spec::new()` using `server_adds()` / `client_adds()`

Example: Adding a hypothetical `BatchDelete` feature:

```rust
// In Feature enum:
pub enum Feature {
    // ... existing features ...
    BatchDelete,
}

// In Feature::all():
&[
    // ... existing features ...
    Feature::BatchDelete,
]

// In Feature::as_str():
Feature::BatchDelete => "batch_delete",

// In Spec::new():
// Server provides it from 1.2.900
chs.server_adds(F::BatchDelete, ver(1, 2, 900));
// Client requires it from 1.2.910 (after testing)
chs.client_adds(F::BatchDelete, ver(1, 2, 910));
```

### Deprecating a Feature

To deprecate and eventually remove a feature:

1. **First release**: Client stops using the feature (`client_removes()`)
2. **Wait 3-6 months** for users to upgrade their clients
3. **Later release**: Server removes the feature (`server_removes()`)

Example: Deprecating the `OldApi` feature:

```rust
// In Spec::new():

// Phase 1: Client stops using OldApi (release 1.2.800)
chs.client_removes(F::OldApi, ver(1, 2, 800));

// Phase 2: After 3-6 months, server removes support (release 1.2.850)
chs.server_removes(F::OldApi, ver(1, 2, 850));
```

## Implementation Notes

- The current version is obtained from `CARGO_PKG_VERSION` at compile time
- Both methods iterate over all features defined in `Feature::all()`
- Features where the client uses `Version::max()` as `since` are "not yet used" and don't affect calculations
- Features where the server uses `Version::max()` as `until` are "still provided" and don't affect client minimum
- Version `(0, 0, 0)` is used as the initial minimum; no real version should be this low

### Relationship to Static Version Constants

The crate exports static version constants in [`lib.rs`](./lib.rs):

```rust
pub static MIN_CLIENT_VERSION: Version = Version::new(1, 2, 676);
pub static MIN_SERVER_VERSION: Version = Version::new(1, 2, 770);
```

**These static constants are the authoritative source of truth** for version
compatibility checks during handshake. They are hardcoded because Rust cannot
compute them at const initialization time for static variables.

The `min_compatible_*_version()` functions exist to **verify** these values
are correct. Unit tests assert that the static constants exactly match the
computed values:

```rust
#[test]
fn test_min_client_version_matches_computed() {
    let spec = Spec::load();
    let computed = spec.min_compatible_client_version();
    assert_eq!(MIN_CLIENT_VERSION, computed);
}
```

| Constant | Static Value | Computed Value | Status |
|----------|-------------|----------------|--------|
| MIN_SERVER_VERSION | 1.2.770 | 1.2.770 | ✓ |
| MIN_CLIENT_VERSION | 1.2.676 | 1.2.676 | ✓ |

**Maintenance**: When feature changes affect compatibility:
- Update `MIN_CLIENT_VERSION` when server removes features
- Update `MIN_SERVER_VERSION` when client requires new features
- Run tests to verify the values match the computed results

### Handshake Limitations

The current handshake only verifies `server.since` (when feature was added),
not `server.until` (when feature was removed). This design relies on:

1. **Deprecation policy**: Client stops requiring a feature before server removes it
2. **Static MIN_CLIENT_VERSION**: Server rejects clients below this version

If MIN_CLIENT_VERSION is outdated, old clients may:
- Pass the server's version check
- Fail at runtime when accessing removed features

## Edge Cases

| Scenario | Behavior |
|----------|----------|
| No features required by client | `min_server = (0, 0, 0)` |
| No features removed by server | `min_client = (0, 0, 0)` |
| Feature not yet used by client (`since = MAX`) | Ignored in server calculation |
| Feature still provided by server (`until = MAX`) | Ignored in client calculation |
| Calculated min_version > peer version | `MetaHandshakeError` returned, connection rejected |

## Related Files

- [`features.rs`](spec.rs) - Feature definitions and compatibility methods
- [`changes.md`](./changes.md) - Changelog of feature additions and removals
- [`lib.rs`](./lib.rs) - Version constants `MIN_CLIENT_VERSION` and `MIN_SERVER_VERSION`
- [`grpc_client.rs`](../../client/src/grpc_client.rs) - Handshake implementation
- [`meta_handshake_errors.rs`](../../types/src/errors/meta_handshake_errors.rs) - Error types

## Glossary

| Term | Definition |
|------|------------|
| **Feature** | A named capability in the meta-service protocol |
| **Feature span** | The version range `[since, until)` when a feature is active |
| **Active feature** | A feature where `since <= current_version < until` |
| **Required feature** | A feature the client needs from the server |
| **Provided feature** | A feature the server offers to clients |
| **Rolling upgrade** | Upgrading servers/clients incrementally without full downtime |
| **Handshake** | The initial gRPC exchange that verifies version compatibility |
| **MetaHandshakeError** | Error returned when client and server versions are incompatible |
