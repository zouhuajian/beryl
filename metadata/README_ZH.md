# Vecton Metadata 当前实现说明

本文只描述当前仓库代码已经落地的 `metadata` 实现。未闭环能力会明确标为“未实现”“部分实现”“历史残留”或“设计目标”，不要把本文当作未来设计承诺。

## 1. Metadata 当前定位

`metadata` 是 Vecton 的文件系统元数据权威面，负责 inode、dentry、attrs、mount、Raft mutation、worker descriptor、block metadata、delete intent 和请求一致性错误语义。

`metadata` 不做数据面 IO。读写数据应由 client 直接访问 worker；metadata 只返回或维护数据路径所需的控制面信息，例如 read plan、write handle、fencing token、block metadata、worker soft state 和 refresh hint。

当前主链路是：

- 进程启动：`metadata/src/bin/main.rs` -> `metadata/src/runtime.rs::MetadataServer::build()` -> `serve()`。
- 文件系统 RPC：`MetadataFileSystemServiceImpl` -> `GuardChain` / `PathResolver` -> `FsCore` -> `AppRaftNode` / `RocksDBStorage`。
- worker metadata RPC：`MetadataWorkerServiceImpl` 处理 register、heartbeat、block report、task ack。
- 后台任务：`MaintenanceService`、`DeleteExecutor`、worker background tasks 在同一进程内启动并由 `RuntimeHandles` 持有。

当前非主链路或未闭环能力包括：UFS metadata proxy、ACL/Ranger authz、advanced direct-read scheduling / load-aware / fault-domain placement、完整 repair/move/evict/rebalance 自治闭环、多 shard migration、follower read 全路径语义。

## 2. 当前实现总览

```mermaid
flowchart TB
    Main["bin/main.rs"] --> Runtime["MetadataServer::build()"]
    Runtime --> Authority["MetadataAuthority"]
    Authority --> Storage["RocksDBStorage"]
    Authority --> Mount["MountTable"]
    Authority --> Raft["AppRaftNode"]
    Raft --> SM["AppRaftStateMachine"]
    SM --> Storage
    SM --> Mount

    Runtime --> WorkerRt["WorkerRuntime"]
    WorkerRt --> WorkerSvc["MetadataWorkerServiceImpl"]
    WorkerRt --> WorkerMgr["WorkerManager"]
    WorkerRt --> Repair["RepairQueue / OrphanQueue / RepairPlanner"]

    Runtime --> Ready["RootReadinessGate + HealthService"]
    Runtime --> FS["MetadataFileSystemServiceImpl"]
    FS --> Guard["GuardChain"]
    FS --> Resolver["PathResolver"]
    FS --> Core["FsCore"]
    Guard --> Mount
    Resolver --> Mount
    Resolver --> Storage
    Core --> Raft
    Core --> Storage
    Core --> WorkerMgr

    Runtime --> Maint["MaintenanceService"]
    Runtime --> DeleteExec["DeleteExecutor"]
    Maint --> Repair
    Maint --> WorkerMgr
    DeleteExec --> WorkerMgr
    DeleteExec --> Raft

    Runtime -. "构造存在，主链路未使用" .-> UFS["UfsRegistry / UfsMetadataProxy"]
```

图中实线是当前已接入主链路或启动链路的对象；虚线表示 runtime 已构造但 FileSystemService namespace read/write 主路径未使用。

## 3. 启动链路

`metadata/src/bin/main.rs` 仍是薄入口。它只调用：

1. `load_config()`
2. `init_observability(config.as_ref())`
3. `MetadataServer::build(config).await`
4. `server.serve().await`

`MetadataServer::build()` 当前实际构造的长期对象：

| 阶段 | 输入 | 输出 | 副作用 | 边界 |
| --- | --- | --- | --- | --- |
| `load_config()` | `VECTON_CONFIG` 或 `conf/core-site.yaml` | `Arc<MetadataConfig>` | 读取配置 | 不构造 runtime。 |
| `init_observability()` | `MetadataConfig` | `Observability` | 初始化 tracing/metrics guard | 只保活观测资源。 |
| `build_authority()` | config | `MetadataAuthority` | 打开 RocksDB，加载 `MountTable`，构造 `AppRaftStateMachine` / `AppRaftNode`，执行 root mount bootstrap，构造 `RaftStateStore`、UFS registry/proxy | authority bootstrap 顺序仍是 `RocksDBStorage -> MountTable -> AppRaftStateMachine -> AppRaftNode -> ensure_root_mount -> RaftStateStore`。 |
| `build_worker_runtime()` | config、authority | `WorkerRuntime`、`MetadataWorkerServiceImpl` | 创建 `WorkerManager`、repair/orphan queue、planner，初始化 metadata epoch | worker 是 required component，不存在 optional/disabled worker runtime。 |
| `build_readiness()` | config、authority | `Readiness` | 启动 root readiness watcher，创建 HealthService | readiness gate 被 FileSystemService guard 使用。 |
| `build_filesystem_service()` | config、authority、worker manager、readiness | `MetadataFileSystemServiceImpl` | 创建 write session manager、inode lease manager、worker commit hook、permission checker、FsCore、GuardChain | 只构造 filesystem RPC service，不启动 server。 |
| `build_maintenance()` | authority、worker runtime | `Maintenance` | 启动 lease runtime、MaintenanceService、DeleteExecutor | 后台维护能力同进程运行。 |
| `build_worker_background()` | worker runtime、worker service、maintenance | `WorkerBackground` | 给 worker service 注入 DeleteExecutor，启动 worker background tasks | 启动 worker 相关后台循环。 |
| `compose_services()` | filesystem、worker、readiness、background、maintenance | `RpcServices`、`RuntimeHandles` | 无新增启动副作用 | 分离可注册 RPC service 和需保活 handle。 |
| `serve()` | config、services、handles | gRPC server | 注册 FileSystemService、MetadataWorkerService、HealthService，等待 Ctrl-C/SIGTERM | 只负责注册和持有 handles，不构造 runtime。 |

`RuntimeHandles` 当前持有 `WorkerBackgroundHandle`、`MaintenanceHandle`、`DeleteExecutorHandle`、`ReadinessHandle`，语义是保留后台 `JoinHandle`。代码没有 cancellation token、逐 task stop 或 join 流程，因此不能写成完整 graceful shutdown；目前只是 server shutdown 后进程/Tokio runtime 结束后台循环。

root mount bootstrap 当前行为：

- 若已有 `/` mount，必须满足 `ROOT_INODE_ID`、`MountKind::Internal`、无 `ufs_uri`、`DataIoPolicy::Forbid`。
- 若缺失且当前节点是 leader，通过 `Command::CreateMount` 创建。
- 若当前节点不是 leader，`ensure_root_mount()` 直接返回，后续由 readiness watcher 持续等待/尝试。
- root mount 不能删除。

## 4. 请求主链路

当前对外 filesystem RPC 入口是 `MetadataFileSystemServiceImpl`，实现 `proto::metadata::FileSystemServiceProto`。该 service 是 external metadata/control-plane API：namespace 仍以 path 作为入口，读写一致性身份由 inode、data_handle、block、lease、fencing 和 epoch 承载。

对外 client-facing 删除 API 统一是 `FileSystemService.Delete(DeleteRequestProto)`。public filesystem service 不再暴露 `Unlink` / `Rmdir` RPC，也没有 `DeletePath` API。`DeleteRequestProto` 使用 `path` + `recursive`：regular file 和 symlink 走内部 non-directory delete，empty directory delete 走内部 directory delete；`recursive=true` 删除目录当前返回 `NotSupported("recursive delete not yet implemented")`。

```mermaid
flowchart LR
    RPC["FileSystemService RPC"] --> Header["RequestContext / header"]
    Header --> Guard["GuardChain"]
    Guard --> Resolver["PathResolver"]
    Resolver --> Core["FsCore"]
    Core --> Read["RocksDB reads"]
    Core --> Write["Raft propose Command"]
    Write --> Apply["AppRaftStateMachine::apply"]
    Apply --> Rocks["RocksDBStorage"]
    Core --> Resp["CoreSuccess / CoreFailure"]
    Resp --> Wire["ResponseHeader.error / payload"]
```

`path_service.rs` 仍然偏大。它当前承担的 adapter 职责包括：tonic request/response、header/context 提取、guard 调用、path resolve、permission target 选择、proto/domain 转换、FsCore 调用、ResponseHeader 构造。核心 freshness、session、lease、fencing、mutation orchestration 已在 `FsCore` 内，但 adapter 还不是很薄。

`FsCore` 当前子模块：

- `mod.rs`：共享 core state、route/write context、dedup、error/header helper、Raft propose 辅助。
- `read.rs`：GetStatus/ListStatus 所需 attrs、readdir，以及 GetBlockLocations/OpenFile 所需 read plan。
- `mutation.rs`：create、mkdir、内部 `unlink` / `rmdir`、rename、mount mutation 等。这里的 `Unlink` / `Rmdir` 是 domain mutation 名称，用于区分 non-directory delete 和 empty-directory delete，不是 public RPC。
- `write_session.rs`：CreateFile/AppendFile/AddBlock/CommitFile/AbortFileWrite/RenewLease/Hflush/Hsync 背后的内部 `WriteSession` 链路；`WriteSession` 不是 public RPC 命名。
- `freshness.rs`：mount_epoch、route_epoch、state_id 校验。
- `tests.rs`：FsCore 局部合同测试。

Guard 链路当前实际生效内容：

- `check_meta_read()`：readiness。
- `check_meta_write()`：readiness + leadership。
- `check_data_read()`：readiness + data IO policy。
- `check_data_write()`：readiness + leadership + data IO policy。
- `check_perm()` / `check_parent_perm()` / `check_super()` / `check_set_attr_perm()`：委托 `PermissionChecker`。

mount/route/write-handle/fencing freshness 仍在 `FsCore` / `WriteSessionCoordinator`，不在 guard 中。`GuardChain` 不检查 `mount_epoch`、`route_epoch`、`state_id`、write handle、lease、fencing token 或 `worker_epoch`。

Authz 当前状态：

- 配置枚举包含 `NONE`、`ACL`、`RANGER`。
- 当前可启用实现只有 `NONE`，行为是 allow-all，并记录 `AUTHZ_ALLOW_NONE_TOTAL`。
- `ACL` 和 `RANGER` 在 `filesystem_permission_checker()` 中直接返回 `InvalidArgument`，不是 ACL MVP，也不是 Ranger allow-all stub。

`MOVED` 当前仍被 FileSystemService de-scope。`core_util.rs` 明确把 `RefreshReason::Moved` 映射成 `RefreshReasonUnknown`，client 侧也把 `ShardMoved` code 映射为 `RouteEpochMismatch` 行为。

## 5. Authority model

metadata 当前 authority 是 inode-centric：

- inode 是文件系统对象身份。
- dentry 是 parent inode + name 到 child inode 的持久映射。
- attrs 是 inode 属性事实。
- path 只是 `PathResolver` 的输入适配，不是持久 authority。

mount / owner group / route epoch：

- `MountTable` 启动时从 RocksDB `mounts` CF 加载，Raft apply `CreateMount` / `DeleteMount` 后同步更新内存表。
- `MountEntry::namespace_owner_group_id` 是 mount 内 namespace mutation owner group。
- `mount_epoch` 使用 `MountEntry::config_version`。
- `route_epoch` 存在 RocksDB `meta` CF 中，`CreateMount` / `DeleteMount` 会推进；`AddShardGroup` 当前不推进 filesystem-facing `route_epoch`。
- `state_id` 来自 state-machine applied `RaftLogId`，并且只通过 `GroupStateWatermark { group_id, state_id }` 出现在 header state vector 中。

metadata state freshness 当前规范收敛为 group-scoped watermark vector：client 维护 `group_id -> state_id` cache；leader 和 production single-group msync 可以用 `ResponseHeader.state` 推进 cache；当 follower read 路径被启用或被调用时，follower 必须先校验 `RequestHeader.state` 是否已被本地 state machine apply 覆盖；follower 成功响应的 `response.state` 必须为空，表示不更新 client cache；stale 必须通过 stale-state error 触发 refresh；multi-group msync 仍是 future work。`applied_seq` 已从 runtime state、storage meta、snapshot header/payload 和 client freshness 中移除；当前 snapshot V1 是最新标准格式，不包含 `applied_seq`，开发期不保留旧 snapshot `applied_seq` 兼容。metadata freshness 只依赖 `GroupStateWatermark` / `last_applied_log_id`。

read freshness 当前在 `FsCore` 中统一：`GetStatus`、`ListStatus`、`OpenFile`、`GetBlockLocations` 都会按目标 inode 的 mount 校验 request header 中的 `mount_epoch`，按 authoritative route epoch 校验 `route_epoch`，并在请求携带 `GroupStateWatermark` 时校验本地 applied state。成功响应在已知 mount/group 时返回 `group_id`、`mount_epoch`、`route_epoch`；只有 leader 且有 applied state 时才会返回 `ResponseHeader.state` 推进 client cache，follower success 仍必须保持 state 为空。

data identity / session identity：

- `data_handle_id` 是数据面身份，`BlockId = data_handle_id + block_index`。
- `WriteHandleProto.handle_id` 是一次写生命周期身份，不是持久文件身份；内部实现仍使用 `WriteSession` 管理该生命周期。
- `fencing_token` 保护 direct client->worker 与 CommitFile 的一致性。
- `CreateFile` 当前会通过内部 `Command::Create` 和持久 `next_data_handle_id` 分配 `current_data_handle_id` 并写入 `data_handle_owner` 映射，然后进入写生命周期。
- `CreateFile` / `AppendFile` / `GetBlockLocations` 路径使用 inode 上的 `current_data_handle_id` 并校验 `data_handle_owner` 映射。`GetBlockLocations(data_handle_id)` 会先通过 `data_handle_owner` 找到 owner inode，再显式校验请求的 handle 是否仍等于 inode 的 `current_data_handle_id`；不匹配时返回 structured `NEED_REFRESH` / `StaleState`，因为这代表 client 持有过期 metadata。
- `Hflush` / `Hsync` 保留 public RPC，但当前明确返回 structured NotSupported；在 visible/durable barrier 设计完成前，不更新 file size、layout 或 version。

block metadata 与 worker soft state 边界：

- block metadata、lease、block refcount、delete intent 是 RocksDB/Raft 管理状态。
- worker identity、worker id allocator 和 descriptor 通过 `Command::RegisterWorker` 在 Raft apply 边界持久化。
- heartbeat、capacity/load、block locations、full-report sync state 是 `WorkerManager` 内存软状态。

Raft / RocksDB 现状：

- 主 filesystem mutation、mount mutation、worker descriptor、delete intent 创建都通过 `Command` propose 到 Raft apply。
- worker register 成功路径会把 identity mapping、`next_worker_id` allocator、worker descriptor 和 `AppliedResult` 放进同一个 RocksDB `WriteBatch`；propose 成功后才更新 `WorkerManager` runtime soft state。
- `RaftStateStore` 读调用 `AppRaftNode::read(false, ...)`，当前是 leader-read 检查，不是 follower read；`AppRaftNode::read(true, ...)` 有 linearizable read 分支，但主 `RaftStateStore` 路径没有使用。
- snapshot build/install 基于 `STATE_CFS` 的 RocksDB snapshot/payload，包含 replicated state CF；install 时先 clear 对应 CF，再批量恢复。当前 snapshot V1 header 只保存 snapshot 解释所需的 route epoch，不读写 `applied_seq`，也不提供旧 `applied_seq` bytes 的兼容读取。
- inode/data handle allocator 使用 RocksDB meta key 持久推进；destructive file layout apply path 的 zero-ref delete intent id 使用 replicated `next_delete_intent_id`，当 allocator key 缺失或落后于已有 delete intent 时，会在同一个 apply `WriteBatch` 内 deterministic bump 到 `max(existing_intent_id)+1` 后再按本次新建 pending intent 数量推进。
- `Create`、`Mkdir`、`Rmdir`、empty-file `Unlink`、extent-bearing file `Unlink`、`Rename`（包括 overwrite target cleanup）、内部 layout/write-handle mutation、`CreateDeleteIntents`、`AllocateDeleteIntents`、`UpdateDeleteIntentStatus`、`CreateMount`、`DeleteMount`、`AddShardGroup`、`RegisterWorker`、`AcquireLease`、`ReleaseLease` 已把业务 mutation 和 `AppliedResult` 放进同一个 RocksDB `WriteBatch`。

## 6. Worker metadata 链路

worker RPC 入口是 `MetadataWorkerServiceImpl`。

register 当前行为：

- 根据 endpoint + labels 计算 identity。
- 通过 `Command::RegisterWorker` propose 到 Raft apply。
- apply 内如果 identity 已存在则复用原 worker_id；否则从 replicated `next_worker_id` 分配新 worker_id。
- identity mapping、`next_worker_id`、descriptor、`AppliedResult` 同 batch 持久化。
- propose 成功后再调用 `WorkerManager::register_worker()` 写入内存 descriptor；propose 失败不会留下 runtime descriptor。

heartbeat 当前行为：

- 所有节点都更新 `WorkerManager` runtime soft state，不走 Raft。
- leader 检测 descriptor 变化会返回 non-OK gRPC `failed_precondition` 要求 re-register。
- leader 分配 full block report lease，处理 task ack，并从 DeleteExecutor/RepairQueue 拉取 worker command。
- follower 不分配 full report lease，不下发 repair/delete command。

block report 当前行为：

- full report 需要在 worker 需要 full sync 时携带 lease token。
- full report 替换该 worker 的 block set；incremental report 在已有 full sync 基础上应用 delta。
- block locations 是 `WorkerManager` 内存 soft state，不通过 Raft 持久化。
- leader 对新增 block 做 orphan 检测和 replication planning。
- `report_presence` RPC 仍存在，是 deprecated/no-op；强一致 presence 来源是 `block_report`。

task ack 当前行为：

- heartbeat 中携带 `TaskAckProto`。
- DeleteExecutor 先尝试按 task id 处理 delete ack。
- RepairQueue 再处理 replicate/move_copy/evict ack；MoveCopy 成功会 enqueue follow-up Evict task。

## 7. Maintenance / Repair / Delete 当前状态

`MaintenanceService::start()` 当前启动 7 个后台任务：

- GC task。
- GC refcount reload self-healing。
- lease cleanup。
- orphan cleanup。
- rebalance task。
- repair timeout requeue。
- over-replication cleanup。

这些任务都是 leader-only 执行核心动作，但模块成熟度不同：

- 已接入：MaintenanceService 在 runtime 启动，DeleteExecutor 在 runtime 启动并接入 worker heartbeat command/ack。
- 部分闭环：GC/orphan/overrep 可以通过 `Command::AllocateDeleteIntents` 创建 delete intent，DeleteExecutor 可读取 pending intents、执行 destructive gate、生成 `DeleteBlocksCommandProto`，并根据 ack + 后续 block report reconcile 通过 `Command::UpdateDeleteIntentStatus` 更新 Completed/Failed 状态。
- 框架/部分闭环：RepairPlanner/RepairQueue 支持 Replicate、MoveCopy、Evict 的 planning、queue、dispatch、ack/backoff；实际 copy/verify/evict 是否端到端成功取决于 worker 实现和数据面配合，metadata 侧不能单独证明完整自治 repair。
- MVP/硬编码：replication factor 多处仍硬编码为 `3u8`；rebalance 是简单 load heuristic；placement/fault-domain 策略未闭环。
建议文档和瘦身时把 maintenance 作为“可保留框架、默认保守运行或可降级”的后台能力，不要描述成完整自治 repair 系统。

## 8. UFS 与 External Mount 当前状态

runtime 当前构造了 `UfsRegistry` 和 `UfsMetadataProxy`，并把它们保存在 `MetadataAuthority` 私有字段里。

`MountKind::External` 和 `ufs_uri` 可以通过 mount entry 表达，`MountTable::resolve_path()` 可以把统一路径映射到 UFS URI + relative path。`UfsMetadataProxy` 也实现了 stat/list/rename/delete/exists 等代理方法。

但 FileSystemService 主路径没有注入或调用 `UfsMetadataProxy`。当前 namespace read/write 仍走 inode/dentry/attrs/Raft/RocksDB 链路；external mount 只在 mount metadata 和 data IO policy 层部分存在，不能写成已经支持完整 UFS-backed namespace。

当前必须明确的边界：UFS proxy 构造存在，但 namespace read/write 主链路未使用。

## 9. Error model 与 refresh 闭环

当前 wire contract 仍是：

- recoverable business/protocol/consistency failure 使用 gRPC OK + `ResponseHeader.error`。
- transport/auth/framework failure 使用 non-OK gRPC status。

metadata 侧：

- `metadata/src/error.rs` 把 `MetadataError` 显式映射为 canonical error。
- `LeaderChanged`、`MountEpochMismatch`、`RoutingStale`、`StaleState`、`LeaseFenced`、`ServiceUnavailable` 等会变成可机器处理的 header error。
- FileSystemService handler 多数错误走 response header；部分 worker RPC 输入错误和 descriptor changed 使用 non-OK gRPC status。

client 侧：

- `client/src/canonical.rs` 明确解析 non-OK gRPC、gRPC OK + header error、gRPC OK + no error 三种 envelope。
- `client/src/meta/filesystem.rs` 的 action machine 根据 `RefreshReason` 做 route/mount/worker/state refresh/replay；session invalid、lease expired、fencing mismatch、authz denial 等 terminal failure 不盲目 replay。
- `client/src/meta/rpc_helper.rs::resolve_path_to_group()` 仍返回 `None`，group route 尚未完成。
- mount refresh 没有专用 API，当前 fallback 到 route/status refresh。
- `GetBlockLocations` 可返回 `route_epoch`/`mount_epoch` hint，并在 `WorkerManager` 有 live block locations 时填充 external `FileBlockLocationProto` worker route hints；这些 locations 是 soft route hint，不是 block presence authority。
- MOVED 仍 de-scope，`ShardMoved` 只走 route refresh 行为。

## 10. 当前已完成整理

以下只列当前代码事实已经完成的整理：

- `main.rs` 已保持薄入口，runtime composition 集中在 `metadata/src/runtime.rs`。
- `MetadataServer::build()` 统一构造 authority、required worker runtime、readiness、filesystem service、maintenance、worker background、services 和 runtime handles。
- worker runtime 不是 optional subsystem。
- `RuntimeHandles` 持有后台 task handles，但未实现完整 graceful shutdown。
- FileSystemService 对外入口统一为 `MetadataFileSystemServiceImpl`。
- FileSystemService public delete API 统一为 `Delete`；`Unlink` / `Rmdir` 只保留为 FsCore/Raft 内部 domain mutation。
- `FsCore` 已拆成 read/mutation/write_session/freshness 子模块。
- `GuardChain` 与 domain freshness 分离：guard 做 readiness/leadership/data IO/authz，mount/route/session/fencing 在 FsCore。
- `MOVED` 在 FileSystemService 中显式 de-scope。
- worker descriptor 与 heartbeat/block location 的持久态/软状态边界已在代码中分离。
- block report 是当前 block presence 主来源；`report_presence` 只是 deprecated/no-op。

## 11. 当前风险、历史包袱、TODO

Dedup / fingerprint / AppliedResult 当前边界：

- `DedupKey` 只表示一个逻辑 mutation 请求身份，当前是 `client_id + call_id`；不能混入 path、inode、payload、epoch 或 `CommandFingerprint`。
- `CommandFingerprint` 表示 command type + 语义 payload 的稳定指纹，用于校验同一 `DedupKey` 下 payload 是否一致；不能合并进 `DedupKey`。
- `AppliedResult` 是 Raft state machine 已 apply mutation 的持久 replay record，用于 retry/replay；不是通用 RPC response cache。
- read-only RPC 不写 `AppliedResult`；读路径依赖 `state_id`、`mount_epoch`、`route_epoch`、`worker_epoch` 和 `ResponseHeader` refresh hint。
- `file_version` 当前不是独立 inode schema 字段；metadata 使用文件 inode 中已持久化的 file lease epoch 作为最小安全版本来源。`CommitFile` 成功 apply 时把本次 `lease_epoch` 作为返回的 `file_version`，同时写入 committed extents；`OpenFile` / `GetBlockLocations` 返回当前 committed file 的版本。后续如果需要独立 durable file version，应在 inode/layout schema 中显式建模，而不是伪造与 correctness 无关的值。
- `Create`、`Mkdir`、`Rmdir`、empty-file `Unlink`、extent-bearing file `Unlink`、`Rename`（包括覆盖目标 cleanup）、内部 layout/write-handle mutation、`CreateDeleteIntents`、`AllocateDeleteIntents`、`UpdateDeleteIntentStatus`、`CreateMount`、`DeleteMount`、`AddShardGroup`、`RegisterWorker`、`AcquireLease`、`ReleaseLease` 已把 business mutation 和 `AppliedResult` 放进同一个 RocksDB `WriteBatch`。
- CommitFile 背后的 internal close-write apply 成功时，inode 中的 committed extents/size/mtime/lease_epoch、block refcount increment/decrement、稳定 `FileLayout` record 和 `AppliedResult` 通过 `close_write_with_apply_result_atomic()` 同批提交；OVERWRITE 会替换 committed extents 并释放旧 block 引用；确定性业务错误仍持久化 `AppliedResult` 以保持 replay 语义。
- `Truncate` shrink 成功 apply 时，apply 层会先按 inode persisted `lease_epoch` 和 deterministic `lease_id=(inode_id<<64)|lease_epoch` 校验 fencing authority；通过后，inode size/mtime/lease_epoch、稳定 `FileLayout` record、被释放完整 block 的 refcount decrement、zero-ref block 的 pending delete intent 和 `AppliedResult` 通过 `truncate_file_with_apply_result_atomic()` 同批提交；grow 仍返回 structured `NotSupported`。
- `Truncate` same-size no-op 仍进入 Raft mutation dedup/replay 路径，持久化 `AppliedResult`，但不改 inode/layout/refcount/delete intent。
- Extent-bearing file `Unlink` 成功 apply 时，dentry、inode、`FileLayout`、`data_handle_owner`、被引用 block 的 refcount decrement、zero-ref block 的 pending delete intent 和 `AppliedResult` 通过 `delete_file_with_extents_and_apply_result_atomic()` 同批提交；active internal write session / active lease 会在 FsCore 层返回 `EBusy`，不强删。
- `Rename` 覆盖 regular file target 时，FsCore 会先拒绝 active internal write session / active lease；Raft apply 成功路径会在同一个 RocksDB `WriteBatch` 内移动 source dentry，删除 old target dentry/inode、old target `FileLayout`、old target `data_handle_owner`，递减 old target committed extents 的 block refcount，并为 zero-ref block 创建 pending delete intent。source file 的 inode/data_handle/layout 成为 dst path 的身份；old dst layout 不再可达。覆盖 non-empty directory target 返回 structured `DirectoryNotEmpty`，不会局部修改 namespace。
- `CreateDeleteIntents` 成功 apply 时，所有 pending delete intents 和 `AppliedResult` 通过 `create_delete_intents_with_apply_result_atomic()` 同批提交；同一 command 内 duplicate intent id 或 DB 中已有 intent id 会被 deterministic structured error 拒绝，且不会覆盖 existing intent。
- GC/orphan/overrep 等 maintenance authoritative intent creation 通过 `AllocateDeleteIntents` 进入 Raft apply；command payload 中的 intent_id 必须为 `0`，apply 内使用 replicated `next_delete_intent_id` 分配真实 intent id。
- `UpdateDeleteIntentStatus` 成功 apply 时，Pending/InFlight -> Completed/Failed 等允许的状态推进、finished timestamp / error msg、`AppliedResult` 同 batch 提交；missing intent、invalid transition 或 fingerprint mismatch 不更新状态。
- `RegisterWorker` 成功 apply 时，worker identity mapping、worker descriptor、`next_worker_id` allocator、`AppliedResult` 同 batch 提交；same identity 复用原 worker_id 并原子更新 descriptor。
- destructive file layout path 中 block refcount decrement 与 zero-ref delete intent creation 已跟触发它们的 namespace/layout mutation 同 batch，包括 extent-bearing `Unlink`、truncate shrink、close-write overwrite 和 rename overwrite target cleanup。zero-ref intent id 来自 replicated `next_delete_intent_id`，allocator key 缺失或落后时同 batch deterministic advance above existing delete intents，same `DedupKey` replay 不重复分配 id、不重复 decrement，也不重复创建 intent。

Mutation command apply-level atomicity inventory：

| 分类 | Command |
| --- | --- |
| DONE | `Create`, `Mkdir`, `Rmdir`, empty-file `Unlink`, extent-bearing file `Unlink`, `Rename` with overwrite target cleanup, internal write-handle commit, `CreateDeleteIntents`, `AllocateDeleteIntents`, `UpdateDeleteIntentStatus`, `CreateMount`, `DeleteMount`, `AddShardGroup`, `RegisterWorker`, `AcquireLease`, `ReleaseLease` |
| COMPLEX_NEXT | recursive directory delete |
| LEGACY_OR_UNUSED | `UpdateCommittedLength` |
| DIRECT_ROCKSDB_TODO | maintenance block refcount compatibility writes outside the file-layout mutation path |

高优先级 correctness 风险：

- Recursive directory delete 未实现；`Delete(recursive=true)` 对目录返回 `NotSupported`，不遍历、不局部删除、不创建 delete intent。

当前实现限制：

- `OpenFile` / `GetBlockLocations` 是 public read-plan API；响应只返回 external `FileBlockLocation`，内部 extents 不通过 public schema 暴露。当 `WorkerManager` 有 live block locations 时会填充 workers 和 `worker_epoch` route hints。range filter 使用 checked arithmetic；`offset + len` 溢出返回 structured invalid argument，`len = 0` 返回空 locations 且保持 OK。它不做 load-aware、fault-domain、nearest-worker 调度，也不触发 repair。
- ACL/Ranger：配置枚举存在，但两者均未实现且 fail fast；不要写成 ACL MVP 或 Ranger stub。
- UFS proxy：runtime 构造存在，FileSystemService 主链路未使用。
- repair/move/evict：metadata 侧有 planner/queue/command/ack 框架，但不能证明完整端到端 repair 系统已经闭环。
- over-rep cleanup/rebalance：任务已启动，但策略仍简单，默认 replication factor 仍多处硬编码。
- report_presence：RPC 仍残留，但 deprecated/no-op。
- client path/group route：`resolve_path_to_group()` 仍返回 None；route refresh/replay 能力仍不完整。
- route owner group / namespace owner group：namespace owner group 参与 write route context 和 header hint；多 group 实际转发/迁移不是完整闭环。
- external mount：mount metadata 支持 external 表达，但 namespace 操作仍不代理到 UFS。
- follower read：代码有 linearizable read 分支，但主 `RaftStateStore` 使用 leader-read；follower read 全路径语义未落地。

## 12. Metadata 瘦身建议

必须保留并优先修正的主链路 correctness。

- 保留 `FsCore`、`PathResolver`、`MountTable`、`AppRaftStateMachine`、`RocksDBStorage`、`RaftStateStore`、`ResponseHeader.error` contract、内部 write session/fencing 主链路。
- 继续收敛 recursive directory delete 等复杂或后台 RocksDB mutation 的原子性；在具备完整 bounded traversal、批量 apply 和恢复语义前，不实现半成品递归删除。

最小读写闭环。

- 保留 FileSystemService external metadata/control-plane RPC、guard、readiness、leadership、NONE authz、mount/route/write-handle freshness。
- 保留 GetStatus/ListStatus/CreateDirectory/Delete/Rename/OpenFile/GetBlockLocations/CreateFile/AppendFile/AddBlock/CommitFile/AbortFileWrite/RenewLease/Hflush/Hsync/Msync 的最小链路。
- `Hflush` / `Hsync` 当前只是 reserved RPC，返回 NotSupported，不描述为可见性或持久性 barrier。
- `GetBlockLocations` 如实返回 external block locations/epoch、`file_version` 和最小 worker route hints；data_handle target 必须匹配 inode 当前 handle。不要把 worker locations 写成 metadata authority，也不要描述成完整调度系统。
- client action machine 保留 refresh/replay，但承认 path->group route 和 mount refresh API 未完成。

可保留框架但默认关闭或降级的后台维护能力。

- MaintenanceService、RepairQueue、OrphanQueue、RepairPlanner、DeleteExecutor 可以保留为框架，但生产默认策略应保守。
- GC、orphan cleanup、over-rep cleanup、rebalance、repair timeout requeue 可按风险分级启用。
- DeleteExecutor 已能下发 delete command；metadata apply 可以创建 pending delete intent，但 physical deletion 仍取决于 worker ack 和后续 block report reconcile，不代表 metadata apply 本身执行了数据面 IO。
- replication factor、placement、fault-domain、rebalance 策略可以先保持 MVP，不要扩展成复杂策略。

暂缓实现的高级能力。

- Ranger。
- 完整 ACL。
- UFS metadata proxy 接入 FileSystemService 主路径。
- 完整 repair/move/evict 端到端自治系统。
- over-rep cleanup 的完整策略化。
- rebalance 的生产级调度。
- 多 shard migration。
- follower read 全路径语义。
- dedicated mount refresh API 和完整 path/group route cache。

不能为了瘦身删除的模块或合同：

- `FsCore`，因为它承载 domain freshness、mutation、内部 write session/fencing。
- `MountTable`，因为 mount resolve、mount_epoch、namespace owner group 依赖它。
- `AppRaftStateMachine` / `AppRaftNode` / `RaftStateStore`，因为 authoritative mutation 依赖 Raft apply。
- `RocksDBStorage`，因为持久 state 和 snapshot/install 依赖它。
- `common` header/error contract，尤其 gRPC OK + `ResponseHeader.error`。
- `MetadataFileSystemServiceImpl` 外部入口。
- write session manager、inode lease manager、fencing token 链路；其中 write session manager 是内部实现对象，不是 public RPC surface。

## 13. 快速阅读路径

推荐按以下顺序阅读当前真实代码：

1. `metadata/src/bin/main.rs`
2. `metadata/src/runtime.rs`
3. `metadata/src/bootstrap.rs`
4. `metadata/src/readiness.rs`
5. `metadata/src/service/path_service.rs`
6. `metadata/src/service/guard.rs`
7. `metadata/src/service/auth.rs`
8. `metadata/src/path_resolver.rs`
9. `metadata/src/service/fs_core/mod.rs`
10. `metadata/src/service/fs_core/freshness.rs`
11. `metadata/src/service/fs_core/read.rs`
12. `metadata/src/service/fs_core/mutation.rs`
13. `metadata/src/service/fs_core/write_session.rs`
14. `metadata/src/raft/command.rs`
15. `metadata/src/raft/state_machine.rs`
16. `metadata/src/raft/storage.rs`
17. `metadata/src/raft/state_machine_store.rs`
18. `metadata/src/state/raft_store.rs`
19. `metadata/src/mount/mod.rs`
20. `metadata/src/worker/service.rs`
21. `metadata/src/worker/manager.rs`
22. `metadata/src/worker/repair/`
23. `metadata/src/worker/delete_executor.rs`
24. `metadata/src/maintenance/service.rs`
25. `metadata/src/maintenance/gc.rs`
26. `metadata/src/maintenance/orphan.rs`
27. `metadata/src/maintenance/overrep.rs`
28. `metadata/src/ufs_proxy.rs`
29. `client/src/canonical.rs`
30. `client/src/meta/filesystem.rs`
31. `client/src/meta/rpc_helper.rs`

阅读时不要从旧设计文档反推能力；以这些代码路径的当前行为为准。
