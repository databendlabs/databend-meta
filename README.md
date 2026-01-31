# databend-meta

Standalone meta-service for [Databend](https://github.com/databendlabs/databend), built on [OpenRaft](https://github.com/databendlabs/openraft).

## Crates

| Crate | Description |
|-------|-------------|
| `service` | Meta service server |
| `client` | Client library for connecting to meta service |
| `types` | Shared types and data structures |
| `kvapi` | Key-value API abstraction |
| `raft-store` | Raft log and state machine storage |
| `sled-store` | Sled-based persistent storage |

## Build

```bash
# Install dev tools
make setup

# Build
make build

# Run tests
make test

# Lint
make lint
```

## License

Apache-2.0
