# 2026-05-24 审计后稳定化行动计划

## 当前稳定化范围

本轮合并 PR-0、PR-1、PR-2 的基线稳定化工作，只处理仓库状态、文档追踪、Rust 1.95.0 工具链、统一验证入口、CI、默认配置收敛，以及已经失去当前生产消费方的配置/API/文档引用。

## 本 PR 包含

- 清理仓库基线：确认 `client/src/api/` 当前布局为 `fs_client.rs`、`handle.rs`、`options.rs`、`status.rs`、`tests.rs`，不恢复已删除的 split API 文件。
- 文档追踪显式化：`docs/` 不再被整目录忽略，架构边界、审计材料、行动计划和配置矩阵可正常提交。
- 工具链基线：固定 Rust 1.95.0，新增 `rust-toolchain.toml`、`Makefile` 验证入口和 GitHub Actions CI。
- 配置收敛：`conf/metadata.yaml`、`conf/worker.yaml` 和 `conf/client-site.yaml` 只保留当前 runtime 实际消费并校验的键。
- 删除无当前消费方的配置入口：移除 metadata 存储目录环境覆盖、worker 配置别名、client 单数 metadata group 配置入口，以及默认配置中的未接线 worker/client/observability 项。
- 文档一致性：README 只描述当前可运行基线和明确延期项，不把未实现功能写成可部署能力。

## PR-6 后当前状态

- worker startup register 已接入 worker 二进制：Worker 先解析稳定 `WorkerId`，每次进程启动生成 UUID `WorkerRunId`，按 metadata group 注册 advertised endpoint，成功后才开放对应 group 的数据面 readiness。
- MetadataWorkerService register 已改为结构化业务错误返回契约：业务/协议错误使用 gRPC OK + `ResponseHeader.error`；transport/framework failure 才使用非 OK gRPC status。
- worker heartbeat liveness 已接入 worker 二进制：Worker 在 register 成功后向 `worker.metadata.endpoints` 中所有 metadata peers fanout heartbeat；metadata 只更新 memory-only live state，不写 RocksDB、不提交 Raft；data-plane readiness 需要 registration 与本地 heartbeat lease 同时有效。

## 本 PR 明确延期

- worker block report / command ack 生产循环。
- WorkerDataService `block_stamp=0` 正确性修复。
- Raft 网络实现。
- QUIC、RDMA、io_uring、SPDK 生产实现。
- maintenance repair/delete 功能扩展。
- 真实 metadata + worker + client 端到端测试。

## 后续阶段

1. 系统闭环 PR：在 startup register 和 heartbeat liveness 基础上继续实现 worker 到 metadata 的 block report、command ack 生命周期。
2. 数据面正确性 PR：修复 worker public read 的 `block_stamp=0` 防线，并补 contract 测试。
3. MetadataWorkerService 后续错误契约 PR：继续将 heartbeat / block report 的可恢复业务/协议错误统一为 gRPC OK + `ResponseHeader.error`。
4. Raft 与真实 E2E PR：落地 Raft 网络后再引入真实 metadata + worker + client 集成测试。
5. 后端/协议扩展 PR：只有在实现、验证、文档齐备后，才把 QUIC/RDMA/io_uring/SPDK 放入可部署配置。
