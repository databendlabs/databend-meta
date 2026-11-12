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

## 错误兼容性：发布前必须单独评审

稳定版新增 `ClientWriteError::LogEntryDiscarded`：节点曾接受 proposal，但切换 leader 时丢弃本地日志，其提交结果可能未知。它不能与“尚未 append，安全转发”的 `ForwardToLeader` 合并，也不能自动重试。`PreconditionFailed` 同样不应盲目重试。

这些错误通过新增的 `MetaDataError::WriteRequestError` 保留，服务端 peel / 转换及客户端分类均不转发、不重试；客户端和服务端分别覆盖 JSON round-trip 回归。

**这会扩展公开错误枚举及其 JSON 格式，不能宣称旧客户端或旧服务端能够识别新错误。** 尚未升级的调用方可能把无法解码的错误视为连接故障；不能据此批准自动重放非幂等写。发布前需单独决定客户端/转发节点的升级顺序与版本门禁，并评审是否需要一个旧版本可识别的非重试错误封装。本次只完成本地适配，没有发布、部署或放开混合版本错误兼容门禁。

新增 membership 错误分别分类：`UncommittedLeaderLog` 与 `InProgress` 可等待后重试；非法 membership、元数据冲突及自定义拒绝不能因为新版本增加了 variant 就一概重试。

## 测试语义变化

- quorum 必然相交时，membership 变更可能只写一个日志，而不是固定写入 joint/final 两个日志；相关精确 index 断言按实际序列更新。
- 重启后的 leader 必须重新选举并提交新 term 的日志，不再复用旧 vote。测试在提交新 term / 建立 quorum 后再写，不增加写重试来掩盖时序。
- 整体依赖更新仅包括 Openraft 及稳定版新增的必要依赖；不执行 workspace 范围的无关升级。
