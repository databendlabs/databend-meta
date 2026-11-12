# Upgrading to Openraft 0.10

This release moves databend-meta from `openraft` `0.10.0-alpha.29` to `0.10.0`.
The on-disk formats are unchanged, so nodes need no data migration. Nodes of
this release run in one cluster with nodes of earlier releases: the raft
compatibility tests cover v1.2.873, v260205.4.0 and v260629.7.0, with either
version as leader.

## Upgrading a cluster

Upgrade one node at a time, followers first and the leader last. After each
follower, check `/v1/cluster/status` on the admin port to confirm that the
leader has not changed.

Why: when a leader of this release loses leadership, it can report a write as
discarded although the write may have committed on another node. An older node
that forwarded such a write cannot decode the error and returns gRPC
`Internal`, which clients retry, so the write may be applied twice. While the
leader runs an older release, no older node forwards to an upgraded leader. If
the leader moves to an upgraded node early, for example after a failure,
upgrade the remaining nodes promptly; until then the risk is the same as before
the upgrade.

Avoid adding or removing nodes until every node is upgraded. If the leader
rejects such a change, an older node cannot decode the new membership errors
and reports a decode error instead of the reason.

## Clients

After the upgrade, a write whose outcome is unknown fails with gRPC `Unknown`,
which clients do not retry. Before retrying a non-idempotent write that failed
this way, read the current state to see whether it was applied.

## Rust code that depends on databend-meta crates

- `raft_types` in `databend-meta-client-types` no longer exports
  `InstallSnapshotRequest`, `InstallSnapshotResponse`, `InstallSnapshotError`
  or `SnapshotMismatch`. Remove these imports; snapshots are sent with the
  V003/V004 messages.
- `MetaDataError` has a new variant, `WriteRequestError`, for a write that was
  rejected or whose outcome is unknown. Handle it in exhaustive matches, and do
  not retry it.
- `pb::VoteRequest` has a new field `leadership_transfer_from`, and
  `pb::AppendResponse` has `conflict_hint`. Set them, for example to `None`,
  where these messages are built with struct literals.
- `PbAppendResponseExt` no longer has `from_stream_result()` or
  `into_stream_result()`. Use `from_append_result()` and
  `into_append_result()`, which convert `AppendResult` and keep the conflict
  hint.
- `SnapshotChunkRequestV003::new_end_chunk()` takes the snapshot ID as a third
  argument, because `SnapshotMeta` no longer carries it. Read it from
  `DB::snapshot_id`.
- For changes in openraft's own types, see the openraft
  [0.9 to 0.10 upgrade guide][openraft-upgrade].

[openraft-upgrade]: https://github.com/databendlabs/openraft/blob/release-0.10/openraft/src/docs/upgrade_guide/upgrade-v09-v10.md
