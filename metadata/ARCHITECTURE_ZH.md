# Vecton Metadata 架构边界与重组计划

本文是 metadata P0 架构边界文档。它描述后续重组方向和阶段边界，不代表当前 P0 要移动文件、改 Rust 代码、改 proto，或重新设计 filesystem authority 主链路。

## 1. 架构目标

metadata 的目标定位是：

- filesystem authority + worker ingress + maintenance controller。
- filesystem authority 负责路径、inode、mount、state、raft、write-session 语义。
- worker 负责 worker RPC、注册、心跳、运行态、容量/负载、block report、block locations、heartbeat command transport。
- maintenance 负责后台维护：GC、orphan cleanup、over-replica cleanup、lost-worker repair、repair queue/planner、delete intent execution、destructive safety gates。

边界约束：

- 不新增 `block_lifecycle` 顶层模块。
- 不新增 `safety` 顶层模块。
- repair、delete、safety 都是 maintenance 内部能力。
- worker 不拥有 repair/delete command polling/ack routing，只通过 command router 暴露 heartbeat command transport。

这次重组的核心目标不是把所有相关文件一次性搬到理想目录，而是先把 ownership 写清楚：filesystem authority 主链路保持稳定，worker 只做 worker ingress 和运行态，maintenance 统一承载后台维护、repair、delete 与 destructive safety。

## 2. 当前基线：暂不重构的内容

当前 P0-P4.6 阶段不应重开这些大模块：

- `service/path_service.rs`
- `service/fs_core/*`
- `service/guard.rs` 的现有服务 guard 语义
- `raft/*`
- `state/*`
- `mount/*`
- `write_session.rs`
- `inode_lease.rs`
- error/header/canonical error 合约

原因：

- 这些模块属于 filesystem authority 主链路，承载路径适配、inode/dentry/attrs authority、mount/state freshness、Raft apply、RocksDB authority、write-session、inode lease、fencing、recoverable error header 等核心语义。
- 当前基础相对可接受，继续重开会扩大风险面，并把本轮工作从边界整理变成 filesystem API 重新设计。
- metadata 本轮重组重点是清理 worker、maintenance、repair、delete 的边界，而不是重新设计 FileSystemService API。

## 3. 目标模块边界

目标方向如下。该树只表达分层边界，不要求 P0 立即移动文件，也不要求一次性完成 import 调整：

```text
metadata/src/
  lib.rs
  runtime.rs
  config.rs
  error.rs
  bootstrap.rs
  readiness.rs

  fs/
    mod.rs
    path_service.rs
    path_resolver.rs
    guard.rs
    auth.rs
    msync.rs
    domain.rs
    core_util.rs
    core/
    write/

  raft/
  mount/
  state/

  worker/
    mod.rs
    service.rs
    manager.rs
    block_report.rs
    full_report_lease.rs
    command_router.rs
    metrics.rs

  maintenance/
    mod.rs
    service.rs
    gate.rs
    safety.rs
    gc.rs
    orphan.rs
    overrep.rs
    lost_worker.rs
    lease_cleanup.rs
    repair/
      mod.rs
      actions.rs
      planner.rs
      queue.rs
      signal.rs
      types.rs
    delete/
      mod.rs
      intent.rs
      executor.rs
```

分阶段原则：

- P0 只写架构文档，不移动文件。
- P1/P2/P3/P4 分阶段推进低风险清理、command router 解耦和机械移动。
- 当前 `raft/`、`mount/`、`state/` 暂时保留顶层，避免 import churn。
- 不引入 `block_lifecycle` 顶层模块；block 相关生命周期动作应落在 maintenance 内部的 repair/delete/gc/orphan/overrep/safety 边界下。

## 4. worker 边界

worker 应该拥有：

- worker register / heartbeat / block report RPC adapter。
- `WorkerManager` / `WorkerRegistry`：descriptor、runtime、liveness、capacity、load、health。
- block report full/delta 解析与 soft-state block locations。
- full report lease / storm control。
- heartbeat command transport。
- 将 block report delta 交给 maintenance/repair signal handler。

worker 不应该拥有：

- `RepairPlanner`。
- `RepairQueue`。
- `DeleteExecutor`。
- GC/orphan/overrep 策略。
- lost worker 后的 repair scheduling。
- orphan detection、replication planning 或 repair enqueue。

当前已引入：

- `WorkerCommandRouter` / `WorkerCommandSource`，集中承接 maintenance 内部各 command source。
- `WorkerService` 只面向 command router，不直接执行 repair/delete polling 或 ack routing。
- `WorkerService` 的 block report 只更新 `WorkerManager` soft-state block locations，并调用 `maintenance/repair/signal.rs` 处理 repair signal。
- worker heartbeat ack 必须带 source namespace，避免 repair/delete 使用同一 `task_id` 空间时发生冲突或误确认。

预期依赖方向：

```text
WorkerService
-> WorkerManager
-> maintenance/repair RepairSignalHandler
-> WorkerCommandRouter
-> maintenance repair/delete command sources
```

`WorkerService` 可以承载 transport 形态的 command pull/ack，但不能成为 repair/delete 的 owner。

## 5. maintenance 边界

maintenance 是后台维护总模块，内部包含：

- gc
- orphan
- overrep
- lost_worker
- lease_cleanup
- repair
- delete
- safety

maintenance 的职责：

- 周期性扫描。
- 发现异常。
- 触发 repair/delete。
- 处理 block-report repair signal。
- 扫描 lost worker 并为 affected blocks 规划 repair。
- 做 destructive gate。
- 做 inflight conflict protection。
- 维护后台任务 retry、backoff、timeout、ack、reconcile 等状态。
- 不直接承担 worker RPC 接入。

maintenance 可以调用 worker command transport 的抽象接口，但不应该把 worker register、heartbeat、block report adapter 吸入自己模块。

## 6. repair 边界

repair 属于 maintenance 内部子系统，而不是 worker 子模块。

repair 负责：

- under-replica 规划。
- over-replica 副本移除规划。
- move/copy repair 规划。
- `maintenance/repair/RepairQueue`：dedup、retry、backoff、inflight、ack、timeout。
- `maintenance/repair/signal.rs`：处理 worker block report delta 中的 repair/orphan signal。
- `worker/command_router.rs` 中的 repair source adapter：将 repair task 转成 worker heartbeat command。

repair 不负责：

- 通用物理删除。
- GC delete。
- `DeleteIntent` lifecycle。
- worker 注册/心跳。

命名与路径建议：

- `RepairTask::EvictReplica` 表达 repair/rebalance 语境下的副本驱逐，不是通用删除。
- 通用删除不应走 `RepairQueue`。
- repair command 通过 `WorkerCommandRouter` 交给 worker heartbeat transport；worker 模块只保留 transport adapter。
- 当前没有 `maintenance/repair/executor.rs`；repair command source adapter 仍在 `worker/command_router.rs`，后续是否抽出单独 executor 另行决定。

## 7. delete 边界

delete 属于 `maintenance/delete` 内部子系统。

delete 负责：

- `DeleteIntent` builder。
- `DeleteExecutor`。
- 轮询 pending `DeleteIntent`。
- destructive gate 校验。
- 生成 `DeleteBlocksCommand`。
- 处理 delete ack。
- 通过 Raft 更新 `DeleteIntent` terminal status。

目标闭环：

- GC/orphan/overrep 只创建 `DeleteIntent`。
- `DeleteExecutor` 是唯一消费 `DeleteIntent` 并执行物理删除的组件。
- 不再保留 GC -> `RepairQueue` generic delete task 的重复路径。

delete 不负责决定副本健康、lost worker repair 或 worker liveness；这些信号来自 maintenance repair/lost_worker 和 worker runtime。

## 8. safety 边界

safety 不作为顶层模块，而是 `maintenance/safety.rs` 或 `maintenance/safety/`。

safety 包含：

- `DestructiveGate`。
- `InflightRegistry`。
- blockreport convergence gate。
- `not_before` / grace window。
- `mount_epoch` / guard watermark / `state_id` 检查。
- per-block single-flight。

safety 是后台维护的横切能力，主要服务 GC、orphan、overrep、repair、delete 等 destructive 或 conflict-prone 后台流程。它不应该扩散成全局大模块，也不应该反向污染 filesystem authority 主链路。

## 9. 顶层 src 文件处理原则

顶层 `metadata/src` 只应保留：

- crate entrypoint / exports。
- runtime composition。
- config。
- error。
- bootstrap。
- readiness。

处理原则：

- subsystem-local 文件应迁入子模块。
- stale future abstractions 优先删除，不长期保留半接入状态。
- 迁移优先级应按风险排序：先删 stale、再缩小 public surface、再做机械移动、最后收敛 exports。
- 对 filesystem authority 主链路文件，优先保持语义不动，只在后续阶段做低风险路径归位。

初步分类：

| 文件 | 分类 | 说明 |
| --- | --- | --- |
| `file_handle.rs` | REMOVED_IN_P1 | 只剩自身定义和 crate export，没有真实 caller；write path 由 `WriteSessionManager` + `InodeLeaseManager` 承担。 |
| `data_io.rs` | REDUCED_IN_P1 | 只保留 GuardChain 当前实际使用的 Read/Write policy；后续可并入 `fs/guard` 或更明确的 policy 子模块。 |
| `lease_runtime.rs` | REMOVED_IN_P1 | 只有 runtime 创建/warmup/持有和 lease cleanup 半接入字段，没有真实消费链路；已删除名义 runtime。 |
| `destructive_gate.rs` | KEEP_BUT_MOVE | 后续归入 `maintenance/safety`。 |
| `inflight_registry.rs` | KEEP_BUT_MOVE | 后续归入 `maintenance/safety`。 |
| `path_resolver.rs` | KEEP_BUT_MOVE | 后续归入 `fs`，语义不动。 |
| `write_session.rs` / `inode_lease.rs` | KEEP_BUT_MOVE | 后续归入 `fs/write`，但 write-session、lease、fencing 语义不动。 |
| `metrics.rs` | KEEP_BUT_REDUCE | 后续按 subsystem 拆分到 fs/worker/maintenance/repair/delete metrics。 |

## 10. 分阶段计划

P0：

- 只写架构文档。
- README 增加链接。
- 不改 Rust 代码。

P1：

- 删除明显 stale 文件：`file_handle.rs` 已删除。
- 缩小 `data_io`：`DataIoOp` 只保留 Read/Write。
- `lease_runtime` 做 wire-or-delete 决策：未发现真实消费链路，已删除 standalone runtime warmup/持有路径。
- 只做低风险清理。

P2：

- 引入 `WorkerCommandRouter`。
- `WorkerService` 不再直接执行 repair/delete command polling 和 ack routing。
- 解决 ack source namespace / `task_id` 冲突。

P3：

- 已完成：worker/repair 移入 `maintenance/repair`。
- `RepairTask::Evict` / `RepairDedupKey::Evict` 已收窄命名为 `EvictReplica`。
- repair queue/planner 行为保持为 metadata 侧维护能力；worker 侧只保留 heartbeat command source adapter。

P4：

- 已完成：`worker/delete_executor.rs` 与 `maintenance/intents.rs` 收敛到 `maintenance/delete`。
- GC/orphan/overrep 统一只创建 `DeleteIntent`。
- `DeleteExecutor` 是唯一 `DeleteIntent` consumer；GC 不再保留 intent -> `RepairQueue` 的重复物理删除路径。

P4.6：

- 已完成：`WorkerService` 不再直接持有 `RepairQueue` / `RepairPlanner` / `OrphanQueue`。
- 已完成：block report repair signal detection/planning/enqueue 收敛到 `maintenance/repair/signal.rs`。
- 已完成：dead-worker scan、`remove_dead_worker` 和 affected block repair scheduling 收敛到 `maintenance/lost_worker.rs`，由 `MaintenanceService` 启动。
- `WorkerCommandRouter` 仍只负责 repair/delete command poll/ack，不承担 repair signal routing。
- `removed_blocks` 仍不触发 under-rep planning；本阶段只迁移边界，不做行为增强。

P5：

- metrics 拆分。
- `lib.rs` exports 收敛。
- metadata README 完整更新。

执行约束：

- 后续 Codex 不应一次性跨多个阶段大改。
- 每阶段都应先明确文件边界、语义不变项和验证命令。
- 涉及移动文件时，应优先保持行为等价，避免把架构归位和语义重写混在同一 diff。

## 11. 非目标

本轮不是：

- 重新设计 FileSystemService API。
- 改 proto。
- 改 error/header 合约。
- 改 raft apply 语义。
- 改 write-session fencing。
- 改 client refresh/replay 语义。
- 改 worker placement 策略。
- 做大规模性能优化。

同样，本轮不引入新的 external metadata service，不引入 path-as-authority，不把 repair/delete/safety 提升为顶层并列 authority，也不把 worker 模块写成后台维护 owner。

## 12. 验证命令

P0 只需要：

```bash
git diff --check
```

如果本阶段只修改 README/文档，不需要 `cargo test`。

后续代码阶段需要按风险选择并记录：

```bash
cargo fmt --all --check
cargo test -p metadata --all-targets
cargo clippy -p metadata --all-targets -- -D warnings
cargo test --workspace --all-targets
git diff --check
```

如果某阶段只做机械移动，仍应至少运行 metadata crate 的 fmt/test/clippy；如果跨 crate exports 或 proto caller 发生变化，必须升级到 workspace 级验证。
