# Upgrading to Openraft 0.10

This change pins `openraft` to `0.10.0`, up from `0.10.0-alpha.29`. It does not
enable the new quorum policies, Pre-Vote, or extra write retries. The toolchain
is unchanged.

## Types and storage

`RaftTypeConfig` explicitly selects the existing majority `Membership`,
`ChangeMembers` and the existing `EntryPayload`. `Entry`, `StoredMembership`,
`SnapshotMeta` and `Snapshot` are adapted to the new generic parameters of the
stable release. The application log payload and the persisted codecs are
unchanged.

The client types drop four public aliases that nothing in this repository uses:
`InstallSnapshotRequest`, `InstallSnapshotResponse`, `InstallSnapshotError` and
`SnapshotMismatch`. Snapshot RPCs use this repository's own V003/V004 protobuf
messages and `install_full_snapshot()`, so `openraft-legacy` is not needed.
Code outside this repository that still references these aliases must drop them
or move to the existing snapshot API. Removing them does not change the
V003/V004 wire protocol.

The stable release no longer stores `snapshot_id` in `SnapshotMeta`. It still
writes a JSON field with that name, but the value is empty, and deserialization
discards it. Databend still names snapshot files by this ID, so the code that
handles it cannot simply be deleted.

- `snapshot-db::DB` keeps `snapshot_id` itself. Creating, copying and opening a
  DB and the rename after installation all preserve it.
- The last V003 chunk writes the original non-empty ID explicitly, and the
  receiver extracts it before Openraft discards the field. Callers of
  `new_end_chunk` now pass the snapshot file ID.
- V004 `Commit.snapshot_id` still comes from the DB's file ID.
- The snapshot messages in `.proto`, the file ID format and the rotbl file
  format are unchanged.

## AppendEntries boundary

The stable release uses `AppendResult`, which is not a serializable wire type.
The unary RPC still sends the existing `AppendEntriesResponse`, which carries
no conflict hint. The stream RPC (`AppendV002`) sends `pb::AppendResponse`,
which now carries the conflict hint as well as conflicts and higher votes; see
"Protobuf messages" below.

When the sender transmits only a prefix of the entries because of the message
size limit, the acknowledged range is computed from the request actually sent.
The sender must not assume the prefix was written before it decodes the
response; otherwise a conflict or a higher vote would be reported as success.

## Protobuf messages

Two raft messages gain a field to carry data that openraft 0.10.0 added. Both
are new field numbers, so either side of a mixed-version pair decodes the
other: an older peer ignores the field, and a message from an older peer reads
as if the field were absent.

- `VoteRequest.leadership_transfer_from` (field 3) is the vote of the Leader
  that authorized the election as a leadership transfer. A voter that follows
  that Leader handles the request even while the Leader's lease holds. Before
  this field, `VoteV001` dropped the value, and a transfer relied only on the
  `TransferLeaderRequest` broadcast releasing each voter's lease; that is still
  what happens with an older peer.
- `AppendResponse.conflict_hint` (field 4, message `ConflictHint`) samples the
  follower's log after a conflict, so the leader picks the next probe instead
  of searching for the matching index. A wrapper message keeps presence: an
  absent hint means the follower sent none, and an empty `log_ids` means the
  follower has no log. Without a hint, as from an older follower, the leader
  falls back to binary search. The hint may describe entries that are not
  flushed and acknowledges no replication.

Rust code that builds `pb::VoteRequest` or `pb::AppendResponse` with a struct
literal must set the new field, for example to `None`.

## Error compatibility and upgrade order

The stable release adds `ClientWriteError::LogEntryDiscarded`: a node accepted
a proposal as leader, then lost leadership, and the new leader truncated the
entry or a snapshot superseded it. The write may have committed on another
node, so its outcome is unknown. It must not be merged with `ForwardToLeader`,
which means the entry was not appended and the request is safe to forward, and
it must not be retried automatically.

The server keeps this error in the new `MetaDataError::WriteRequestError`
variant, and peel does not treat it as a forward. Client and server each have a
JSON round-trip regression test for it. The client-facing write RPCs
(`Transaction`, `KvTransaction`, and UpsertKV through the legacy `kv_api`)
return gRPC `Unknown` for `LogEntryDiscarded` and `FailedPrecondition` for
`PreconditionFailed`. Clients decide whether to retry by status code alone, and
no released client retries either code.

`PreconditionFailed` cannot occur in this repository. Only Openraft APIs that
take preconditions raise it, such as `change_membership_if()` and
`append_membership()`, and this repository calls only `client_write()` and
`change_membership()`.

### Mixed-version clusters

`WriteRequestError` extends the JSON format of `MetaDataError`, and alpha.29
nodes cannot decode it. When an alpha.29 node forwards a write to a 0.10.0
leader and gets `LogEntryDiscarded` back:

1. `reply_to_api_result()` fails to decode `RaftReply.error` and returns
   `InvalidReply`, which is classified as `MetaAPIError::NetworkError`.
2. `MetaForwarder::forward()` wraps it as `ForwardRPCError::RemoteError`, and
   `handle_forwardable_request()` returns a `RemoteError` without retrying.
3. The old node maps every forwarding error to `Status::internal`. Old and new
   clients both retry `Internal`, so the write is replayed.

This settles the following:

- **A non-retryable error wrapper that old versions can decode is not needed,
  and it would not work.** The old node ignores the error content and maps
  every forwarding failure to `Internal`. Nothing the new leader puts in
  `RaftReply.error` can make an old node return a non-retryable status.
- **The replay risk while versions are mixed is the same as before the upgrade,
  not worse.** alpha.29 reports the same situation as `ForwardToLeader`. When
  the leader handles the write locally, `handle_forwardable_request()` forwards
  it again to the new leader. `KvTransaction` returns
  `Unavailable("not leader")`, which clients retry. When forwarding is
  impossible, the result is `CanNotForward`, which maps to `Internal`, and
  clients retry it too. All three paths replay the write. So while old nodes
  remain, the status mapping above protects only the writes that a new-version
  leader handles locally. `KvTransaction` runs only on the leader and is never
  forwarded, so it is protected once the leader is upgraded. Protection is
  complete only when every node is upgraded.
- **Upgrade the leader last.** Only the leader that proposed an entry produces
  `LogEntryDiscarded`, after it loses leadership, and an old node receives it
  only if it forwarded the write to a new-version leader. Upgrading the
  followers one at a time and the leader last keeps old nodes forwarding only
  to the old leader. After upgrading each follower, confirm that the leader has
  not changed. If an unplanned leader change, such as a leader failure, elects
  a new-version node, the risk returns to the pre-upgrade level; upgrade the
  remaining nodes promptly.
- **Do not add or remove nodes while versions are mixed.** The new
  `ChangeMembershipError` variants come only from membership requests such as
  join and leave. An old node that cannot decode one treats it as a network
  error: join retries 3 times at 1-second intervals and then fails, leave fails
  at once, and the error message includes the decode error. A rolling upgrade
  does not need to add or remove nodes.

The new membership errors are classified per variant. `UncommittedLeaderLog`
and `InProgress` may be retried after a wait. Invalid memberships, metadata
conflicts and user-defined rejections must not be retried just because the new
version added variants.

## Changes in test semantics

- When the old and new quorums are guaranteed to intersect, a membership change
  may write a single log entry instead of always writing a joint and a final
  entry. Exact index assertions in tests follow the actual sequence.
- A restarted leader must win a new election and commit an entry of its new
  term instead of reusing its old vote. Tests write only after the new term is
  committed and a quorum is established, without adding write retries to hide
  the timing.
- The dependency update covers only Openraft and the dependencies the stable
  release requires. There is no unrelated workspace-wide upgrade.
