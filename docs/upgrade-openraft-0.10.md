# Openraft 0.10 稳定版升级说明

本次将 `openraft` 从 `0.10.0-alpha.29` 固定升级到 `0.10.0`，不启用新的 quorum 策略、Pre-Vote 或额外写重试。现有工具链保持不变。

## 类型与存储

`RaftTypeConfig` 显式选择既有多数派 `Membership`、`ChangeMembers` 以及原有 `EntryPayload`。`Entry`、`StoredMembership`、`SnapshotMeta` 和 `Snapshot` 按稳定版的新泛型参数适配，不更换应用日志 payload 或持久化 codec。

删除 client types 中仓库内未使用的四个公开别名：`InstallSnapshotRequest`、`InstallSnapshotResponse`、`InstallSnapshotError` 和 `SnapshotMismatch`。实际快照 RPC 使用本仓库的 V003/V004 protobuf 消息及 `install_full_snapshot()`，不需要 `openraft-legacy`。仓库外若仍引用这些别名，需要删除引用或迁移到现有快照接口；删除这些别名不改变 V003/V004 wire 协议。

稳定版不再在 `SnapshotMeta` 保存 `snapshot_id`：它仍写入同名 JSON 字段，但值为空，反序列化时也会丢弃。Databend 的文件名仍依赖此 ID，不能直接删掉相关逻辑。

- `snapshot-db::DB` 单独保存 `snapshot_id`，建库、复制、打开及安装后重命名均保留该值。
- V003 最后一个 chunk 显式写入原有非空 ID；接收端在 Openraft 忽略该字段前提取它。`new_end_chunk` 的调用者需要额外传入 snapshot 文件 ID。
- V004 的 `Commit.snapshot_id` 仍来自该 DB 的文件 ID。
- `.proto` 定义、文件 ID 格式与 rotbl 文件格式不变。

## AppendEntries 边界

稳定版使用 `AppendResult`，它不是可直接序列化的 wire 类型。Unary RPC 仍使用原有 `AppendEntriesResponse`；stream RPC 仍使用既有 protobuf 响应。新增的 conflict hint 不在当前协议中传输，但 conflict 与 higher vote 仍保留。

当客户端因为 message size 只发送 entries 前缀时，确认范围按实际请求计算；不能在解析响应前假定前缀写入成功，否则会把 conflict 或 higher vote 隐藏成成功。

## 错误兼容性与升级顺序

稳定版新增 `ClientWriteError::LogEntryDiscarded`：节点作为 leader 接受了 proposal，之后失去 leadership，这条日志被新 leader 截断或被快照覆盖。该写入可能已在其他节点提交，结果未知。它不能与“尚未 append，可安全转发”的 `ForwardToLeader` 合并，也不能自动重试。

服务端用新增的 `MetaDataError::WriteRequestError` 保留这类错误，peel 时不当作转发处理；客户端和服务端分别覆盖 JSON round-trip 回归。面向客户端的写 RPC（`Transaction`、`KvTransaction`、旧 `kv_api` 的 UpsertKV）对 `LogEntryDiscarded` 返回 gRPC `Unknown`，对 `PreconditionFailed` 返回 `FailedPrecondition`。客户端只按 status code 决定是否重试，已发布的各版本客户端都不重试这两个 code。

`PreconditionFailed` 在本仓库不会出现：它只来自带 precondition 的 Openraft API（如 `change_membership_if()`、`append_membership()`），而本仓库只调用 `client_write()` 和 `change_membership()`。

### 混合版本集群

`WriteRequestError` 扩展了 `MetaDataError` 的 JSON 格式，alpha.29 节点无法解码。alpha.29 节点把写请求转发给 0.10.0 leader 并收到 `LogEntryDiscarded` 时：

1. `reply_to_api_result()` 解码 `RaftReply.error` 失败，得到 `InvalidReply`，归为 `MetaAPIError::NetworkError`。
2. `MetaForwarder::forward()` 把它包成 `ForwardRPCError::RemoteError`，`handle_forwardable_request()` 对 `RemoteError` 不重试，直接返回。
3. 旧节点把所有转发错误都映射为 `Status::internal`。新旧客户端都会重试 `Internal`，于是重放这次写入。

据此确定以下几点：

- **不需要旧版本可识别的非重试错误封装，它也不起作用。** 旧节点不看错误内容，所有转发失败都映射为 `Internal`；新 leader 在 `RaftReply.error` 里放什么，都不能让旧节点返回不可重试的 status。
- **混合版本期间的重放风险与升级前相同，不会更差。** alpha.29 把同样的情况报告为 `ForwardToLeader`：leader 本地处理时，`handle_forwardable_request()` 会把写入再转发给新 leader；`KvTransaction` 返回 `Unavailable("not leader")`，客户端重试；无法转发时得到 `CanNotForward`，映射为 `Internal`，客户端同样重试。这三条路径都会重放。因此只要还有旧节点，上述 status 映射只保护由新版本 leader 本地处理的写入（`KvTransaction` 只在 leader 上执行、从不转发，leader 升级后即受保护）；所有节点升级后保护才完整。
- **最后升级 leader。** `LogEntryDiscarded` 只由提出该日志的原 leader 在失去 leadership 后产生；旧节点只有把写入转发给新版本 leader 时才会收到它。先逐个升级 follower、最后升级 leader，旧节点就始终只向旧 leader 转发。每升级一个 follower，确认 leader 没有变化；若发生非计划的 leader 切换（例如 leader 故障）并选出了新版本节点，风险回到升级前的水平，应尽快升级其余节点。
- **混合版本期间不要增删节点。** 新增的 `ChangeMembershipError` variant 只来自 join/leave 这类成员变更请求。旧节点无法解码时按网络错误处理：join 以 1 秒间隔重试 3 次后失败，leave 直接失败，错误信息里带有解码错误。滚动升级本身不需要增删节点。

新增 membership 错误分别分类：`UncommittedLeaderLog` 与 `InProgress` 可等待后重试；非法 membership、元数据冲突及自定义拒绝不能因为新版本增加了 variant 就一概重试。

## 测试语义变化

- quorum 必然相交时，membership 变更可能只写一个日志，而不是固定写入 joint/final 两个日志；相关精确 index 断言按实际序列更新。
- 重启后的 leader 必须重新选举并提交新 term 的日志，不再复用旧 vote。测试在提交新 term / 建立 quorum 后再写，不增加写重试来掩盖时序。
- 整体依赖更新仅包括 Openraft 及稳定版新增的必要依赖；不执行 workspace 范围的无关升级。
