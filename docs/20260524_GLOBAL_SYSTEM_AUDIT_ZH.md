# Vecton 全局系统审计报告

## 1. 审计概览

- 审计日期：2026-05-24
- 审计范围：仓库根目录、`Cargo.toml` 工作区、`README.md`、`AGENTS.md`、各子目录 `AGENTS.md`、`docs/ARCHITECTURE_BOUNDARIES.md`、`conf/`、`common`、`types`、`proto`、`client`、`metadata`、`worker`、`ufs`、`integration_tests`。
- 审计重点：功能完整性、模块边界、依赖方向、核心读写链路、配置与部署可信度、错误语义、测试覆盖、文档一致性。
- 审计基线：当前分支 `dev`，`HEAD=ba813d89c48d66cf775c89bd1d32689bbfb5f215`。
- 审计时工作区状态：`git status --porcelain -uall` 曾显示旧 client API split 文件为 `AD`，即已暂存新增但工作区删除；本轮稳定化重新检查后未发现这些 stale index entries。
- 稳定化后基线：Rust development baseline 固定为 Rust 1.95.0；根目录 `rust-toolchain.toml` pin `1.95.0` 并安装 `rustfmt`/`clippy`；workspace `rust-version` 为 `1.95.0`；根目录 `Makefile` 提供本地 verify 入口；`.github/workflows/ci.yml` 提供最小 CI；`conf/core-site.yaml` 和 `conf/client-site.yaml` 只保留当前 runtime 实际消费的 active 默认 key。
- 总体结论：Vecton 的分层方向基本清晰，`common`、`types`、`proto`、`client`、`metadata`、`worker` 的主要职责经过近期重构后比旧版更收敛；`types` 以纯领域值为主，`proto` 承担结构转换，`client` 侧读写、刷新、重试与缓存边界较完整，`metadata` 侧 inode/dentry/attrs、写会话、Raft apply、freshness 语义覆盖较强，`worker` 的本地块存储和 gRPC 数据面已形成可测闭环。系统仍未达到完整生产闭环，主要风险集中在 worker 到 metadata 的生命周期闭合、worker 公开读请求的 `block_stamp=0` 绕过、MetadataWorkerService 错误返回语义、未落地的 proto/API surface、以及端到端集成测试不足。
- 最高优先级问题摘要：
  - P1：`worker/src/bin/main.rs` 只启动 worker 数据面服务，未发现生产 worker 主动注册、heartbeat、block report 到 metadata 的闭环；`metadata/src/worker/service.rs` 和 `proto/metadata/worker.proto` 已有服务端契约，但 worker 二进制未接入。
  - P1：`worker/src/runtime/block.rs::validate_read` 仅在请求 `block_stamp != 0` 时校验元数据 stamp；正常 client 已拒绝零值，但公开 WorkerDataService 仍可被直接请求零值读，存在新鲜度绕过风险。
  - P1：`metadata/src/worker/service.rs` 对 worker 注册、心跳、block report 的业务/协议错误大量返回非 OK `tonic::Status`，与仓库约定的“可恢复业务/协议错误使用 gRPC OK + header error”不一致。
  - P2：`proto/admin/admin.proto`、`proto/metadata/peer.proto` 等生成并导出，但当前 Rust workspace 未发现对应生产实现、注册或调用方，外部 contract 状态待确认。审计时发现的 stale `BlockReportEntryProto` 已在本轮基线稳定化中删除。
  - 已解决：原始审计发现的 Rust baseline 不一致、local/CI verify baseline 尚未建立、默认配置暴露未接线 key、`docs/` 默认不可跟踪、client API split 文件 `AD` 状态，均已由本轮稳定化处理。

## 2. 项目结构总览

| 模块 / 路径 | 主要职责 | 当前状态 | 证据 | 备注 |
|---|---|---|---|---|
| `Cargo.toml` | Rust workspace 聚合、统一依赖版本、成员声明 | 已完成 | `Cargo.toml`、`cargo metadata --format-version 1 --no-deps` | 成员为 `common/types/proto/metadata/ufs/client/worker/integration_tests`；workspace `rust-version=1.95.0`。`common`/`types`/`proto` 使用 edition 2024，其他主要 crate 使用 edition 2021；该混用由 Rust 1.95.0 baseline 支持。 |
| `common/` | 通用配置加载、错误/header 域模型、观测、重试、时间工具 | 需要优化 | `common/src/lib.rs`、`common/src/config/flat.rs`、`common/src/header/codec.rs` | 职责大体符合边界；`FlatConfig` 偏宽松，`deadline_ms` 默认仍有 TODO。 |
| `types/` | 纯 Rust 共享领域值：ID、block/location、lease、watermark、worker endpoint、fs 基础对象 | 已完成 | `types/src/lib.rs`、`types/src/ids.rs`、`types/src/location.rs`、`types/src/group_watermark.rs` | 未依赖 workspace crate；有少量 future placeholder，如 symlink target。 |
| `proto/` | `.proto`、codegen、gRPC service 契约、共享结构转换 | 需要优化 | `proto/build.rs`、`proto/src/lib.rs`、`proto/src/convert.rs` | 主转换职责清晰；admin/metadata-peer service surface 状态待确认。 |
| `metadata/` | metadata authority、inode/dentry/attrs、mount、write session、Raft、worker membership、maintenance | 需要优化 | `metadata/src/lib.rs`、`metadata/src/runtime.rs`、`metadata/src/service/fs_core/`、`metadata/src/worker/service.rs` | 核心 metadata 语义强；worker service 错误语义、Raft 网络、repair/delete 部分仍有 TODO。 |
| `worker/` | 数据面执行、本地 block store、stream runtime、gRPC WorkerDataService、worker net peer/server | 需要优化 | `worker/src/lib.rs`、`worker/src/data/core.rs`、`worker/src/store/block.rs`、`worker/src/bin/main.rs` | 本地数据面可测；metadata lifecycle、公开读 stamp gate、placeholder backend 是主要缺口。 |
| `client/` | SDK facade、metadata gateway、layout/endpoint cache、retry/replay、worker data adapter | 已完成 | `client/src/lib.rs`、`client/src/api/fs_client.rs`、`client/src/data/worker.rs`、`client/src/runtime/executor.rs` | 公共 facade 收敛，普通读写路径覆盖较好；默认 client 配置已收敛到当前 active key。 |
| `ufs/` | 外部后端抽象、OpenDAL adapter、UFS registry/spec/capability | 需要优化 | `ufs/src/lib.rs`、`ufs/src/opendal_impl.rs`、`ufs/Cargo.toml` | 边界清晰；部分后端能力依赖 feature 或运行环境，测试有 ignored。 |
| `integration_tests/` | 跨 crate contract tests、mock metadata/worker | 需要优化 | `integration_tests/tests/client_contract.rs`、`integration_tests/tests/common/mock_metadata.rs`、`integration_tests/tests/common/mock_worker.rs` | 能验证 client contract，但不是完整真实 metadata+worker E2E。 |
| `conf/` | 示例/默认配置 | 已完成 | `conf/core-site.yaml`、`conf/client-site.yaml`、`docs/CONFIG_MATRIX_ZH.md` | 默认配置文件仅包含当前 runtime 实际消费的 active key；planned/unimplemented 能力只在文档中列为 deferred，不作为 deployable default。 |
| `docs/` | 架构边界与审计材料 | 已完成 | `docs/ARCHITECTURE_BOUNDARIES.md`、`docs/20260524_GLOBAL_SYSTEM_AUDIT_ZH.md`、`.gitignore` | `docs/` 不再被整目录忽略，重要 Markdown 文档默认可被 git 发现。 |
| `.github/workflows` / `Makefile` / `rust-toolchain.toml` | CI、统一验证入口、工具链 baseline | 已完成 | `.github/workflows/ci.yml`、`Makefile`、`rust-toolchain.toml` | CI 和本地 verify 均运行 Rust 1.95.0 下的 fmt-check、metadata、check、clippy、test。 |

## 3. 模块功能审计

### 3.1 common

#### 3.1.1 模块职责

`common` 实际承担通用基础设施职责，包括 `FlatConfig` 配置加载和扁平化、`CanonicalError`/`RequestHeader`/`ResponseHeader` 域模型、header 编解码、retry/deadline、审计和观测辅助。代码未直接依赖 `proto` 或产品 crate，符合边界约束。

#### 3.1.2 功能完成情况

| 功能点 | 状态 | 说明 | 证据 | 优先级 |
|---|---|---|---|---|
| Canonical error 和 header 域模型 | 已完成 | 可表达 OK、retryable、need refresh、fatal 等统一错误语义。 | `common/src/error/mod.rs`、`common/src/header/mod.rs` | N/A |
| gRPC metadata/header 编解码 | 需要优化 | 已支持 deadline/identity/state 编解码，但默认 deadline 来源仍为 TODO。 | `common/src/header/codec.rs` | P3 |
| FlatConfig 加载与类型读取 | 需要优化 | 作为通用 loader 可用，但数值/字符串转换较宽松，强校验依赖各模块 typed config。 | `common/src/config/flat.rs` | P2 |
| 观测与审计基础 | 需要优化 | metrics/tracing/audit 已存在；队列长度等仍有 placeholder。 | `common/src/observe/`、`common/src/audit.rs` | P3 |
| retry/deadline 通用工具 | 已完成 | Client/metadata 可复用基础工具，未发现产品策略上移。 | `common/src/retry/`、`common/src/time/mod.rs` | N/A |

#### 3.1.3 架构评价

`common` 当前更接近“通用基础设施”，没有把 metadata/worker/client 的业务策略塞入 shared crate。主要问题不是边界越界，而是基础配置 API 偏宽松，容易让消费方在未做 typed validation 时把错误配置静默回退为默认值。该风险在 `metadata/src/config.rs`、`client/src/config.rs` 中已有局部缓解，但 `worker/src/config.rs` 仍有默认回退语义。

#### 3.1.4 主要问题

| 问题 | 类型 | 影响 | 建议 | 优先级 | 证据 |
|---|---|---|---|---|---|
| `FlatConfig` 允许字符串/数字互转且消费方可 `unwrap_or` | 配置风险 | 错误类型配置可能被静默吞掉，部署问题难发现。 | 对生产 consumed keys 使用 strict helper；继续保持模块 typed config 的 wrong-type 测试。 | P2 | `common/src/config/flat.rs`、`worker/src/config.rs` |
| header codec 默认 deadline 来源未接入 typed config | 技术债 | 默认超时策略不清晰，可能出现无限 deadline 或模块重复设定。 | 明确由 client/metadata/worker typed config 注入，或移除 TODO 并记录无默认策略。 | P3 | `common/src/header/codec.rs` |
| audit queue size placeholder | 可观测性 | 指标可能给出 0，误导队列压力判断。 | 若该指标对生产有意义，改为显式“不支持”或接入计数。 | P3 | `common/src/audit.rs` |

#### 3.1.5 建议后续动作

1. 保持“strict typed config 由消费模块负责”的规则，并在新增 consumed key 时同步补 wrong-type 测试。
2. 决定 `deadline_ms` 默认策略：由调用方必须传入，还是由模块配置统一注入。
3. 清理或显式标注 audit placeholder 指标，避免被部署侧误用。

### 3.2 types

#### 3.2.1 模块职责

`types` 是纯 Rust 域模型 crate，拥有 typed IDs、inode/fs 基础对象、block/location/write target、lease/fencing、worker endpoint、`GroupStateWatermark`、Raft log id 等稳定跨模块值对象。`cargo tree -e normal -p types` 显示只依赖外部低层库，没有 workspace 依赖。

#### 3.2.2 功能完成情况

| 功能点 | 状态 | 说明 | 证据 | 优先级 |
|---|---|---|---|---|
| typed ID 和显示/序列化稳定性 | 已完成 | 避免 inode、block、data handle、worker 等 ID 混用。 | `types/src/ids.rs`、`types/src/lib.rs` | N/A |
| block/location/write target 域对象 | 已完成 | 承载 metadata 到 client/worker 的 block 布局、写目标、提交块信息。 | `types/src/location.rs`、`types/src/block.rs` | N/A |
| worker endpoint 和 worker_epoch | 已完成 | 表达 worker data endpoint、net protocol、epoch。 | `types/src/worker.rs` | N/A |
| GroupStateWatermark | 已完成 | `state_id` 语义为 state-machine applied `RaftLogId`，符合边界文档。 | `types/src/group_watermark.rs`、`types/src/raft_log_id.rs` | N/A |
| symlink target 领域表达 | TODO / 未完成 | 当前 `InodeData::Symlink` 中 target 标注为 placeholder，未见完整 symlink 创建接口。 | `types/src/fs.rs` | P3 |

#### 3.2.3 架构评价

`types` 没有生成 proto 类型、产品 runtime state 或测试 fixture，整体符合“纯共享领域值”定位。`FileBlockLocation`、`WriteTarget`、`WorkerEndpointInfo` 等结构有明确 runtime 使用，不是空泛抽象。需要注意 future placeholder 不应被外部理解为已完成能力。

#### 3.2.4 主要问题

| 问题 | 类型 | 影响 | 建议 | 优先级 | 证据 |
|---|---|---|---|---|---|
| symlink target placeholder | 功能缺失 | 如果上层暴露 symlink 语义，当前领域模型不足以支撑完整行为。 | 在实现 symlink 前保持未公开；若要公开，补齐 metadata/API/proto/test。 | P3 | `types/src/fs.rs` |
| mixed edition baseline | 已完成 | `common`、`types`、`proto` 使用 edition 2024，其他主要 crate 使用 edition 2021；Rust 1.95.0 baseline 支持该组合。 | 保持 `rust-toolchain.toml`、workspace `rust-version`、CI 和 README 同步。 | N/A | `rust-toolchain.toml`、`Cargo.toml`、`common/Cargo.toml`、`types/Cargo.toml`、`proto/Cargo.toml` |

#### 3.2.5 建议后续动作

1. 对 `types` 保持“有两个以上生产模块真实使用才进入”的门槛。
2. 将 symlink 标为未完成能力，避免被 API 文档误认为支持。
3. 后续若改变 crate edition，必须保持 Rust 1.95.0 baseline 文档、CI 和 workspace manifest 同步。

### 3.3 proto

#### 3.3.1 模块职责

`proto` 负责编译和导出 filesystem metadata、metadata worker、worker data、admin、metadata peer 等 proto 包，并通过 `proto/src/convert.rs` 提供共享结构转换。当前 `client`、`metadata`、`worker` 都依赖它，但 `proto` 本身只依赖 `types/common` 和 protobuf/gRPC 工具链。

#### 3.3.2 功能完成情况

| 功能点 | 状态 | 说明 | 证据 | 优先级 |
|---|---|---|---|---|
| filesystem metadata service schema | 已完成 | 覆盖 status/list/create/delete/rename/open/get locations/create/append/add/commit/abort/lease/sync/msync。 | `proto/metadata/filesystem.proto` | N/A |
| worker data service schema | 已完成 | 覆盖 read stream、write stream、commit、sync committed block、abort。 | `proto/worker/data.proto`、`proto/worker/data_header.proto` | N/A |
| 结构转换集中化 | 已完成 | ID、worker endpoint、write target、file location、header/error、水位线转换集中在 `proto/src/convert.rs`。 | `proto/src/convert.rs` | N/A |
| worker net protocol strict parser | 已完成 | `parse_known_worker_net_protocol` 拒绝 unspecified/unknown。 | `proto/src/convert.rs` | N/A |
| admin 和 metadata-peer proto surface | 待确认 | 已 codegen/export，但未发现 Rust service implementation、server registration 或 caller。 | `proto/admin/admin.proto`、`proto/metadata/peer.proto`、`proto/src/lib.rs` | P2 |
| stale block report entry | 已删除 | 审计时 `BlockReportEntryProto` 仍在 active proto 文件，当前实现已使用 full/delta entries。 | `proto/metadata/worker.proto`、`metadata/src/worker/service.rs` | P2 |

#### 3.3.3 架构评价

`proto` 当前承担 wire schema 和结构转换职责，未承载 client retry、metadata authority 或 worker store policy，边界基本正确。最大问题是 active surface 与实际实现不完全一致：`admin`、`metapeer` 等 schema 如果对外已承诺，需要文档化 contract；如果没有外部消费方，应收敛或隔离，避免误认为已支持。

#### 3.3.4 主要问题

| 问题 | 类型 | 影响 | 建议 | 优先级 | 证据 |
|---|---|---|---|---|---|
| `admin` / `metapeer` service 被生成导出但未落地 | 架构风险 | 外部用户可能把 generated API 当作可用管理面或 peer RPC。 | 确认外部 contract 承诺；若未承诺，标 planned/stale 或移出 active surface。 | P2 | `proto/src/lib.rs`、`proto/admin/admin.proto`、`proto/metadata/peer.proto` |
| worker net protocol 转换策略在 worker 本地出现宽松 fallback | 架构风险 | `proto` 严格拒绝 unspecified/unknown，但 `worker/src/net/protocol.rs` 可 fallback 到 gRPC。 | 统一策略：生产入口拒绝未知协议，测试 helper 才允许默认。 | P2 | `proto/src/convert.rs`、`worker/src/net/protocol.rs` |

#### 3.3.5 建议后续动作

1. 做一次 proto active surface 决策：active、planned、removed、delete 四类明确标注。
2. 将 worker protocol 解析策略统一到 `proto::convert` 的严格语义。
3. 对外部 schema 变更建立 contract review checklist，特别是 numeric enum 和 service 删除。

### 3.4 metadata

#### 3.4.1 模块职责

`metadata` 是文件系统 metadata authority，实际拥有 inode/dentry/attrs、mount、write session、lease/fencing、worker descriptor、Raft state machine、maintenance routing、readiness 和 filesystem gRPC service。`metadata/src/runtime.rs` 负责组合 storage、Raft、mount、worker runtime、maintenance、filesystem service、worker service 和 health service。

#### 3.4.2 功能完成情况

| 功能点 | 状态 | 说明 | 证据 | 优先级 |
|---|---|---|---|---|
| metadata server 启动组合 | 已完成 | 二进制入口薄，runtime 构建 authority、worker runtime、maintenance、gRPC service。 | `metadata/src/bin/main.rs`、`metadata/src/runtime.rs` | N/A |
| path service 与 FsCore | 已完成 | 读写/rename/delete/open/get locations/sync write 通过 guard、resolver、FsCore 处理。 | `metadata/src/service/path_service.rs`、`metadata/src/service/fs_core/` | N/A |
| inode/dentry/attrs 与 Raft apply | 已完成 | State machine 覆盖 create/delete/rename/write/truncate/refcount/dedup 等核心语义。 | `metadata/src/raft/state_machine.rs`、`metadata/src/raft/storage.rs` | N/A |
| freshness 语义 | 已完成 | route/mount/state watermark 分域，leader 成功才返回 state watermark。 | `metadata/src/service/fs_core/read.rs`、`metadata/tests/path_service_regression_tests.rs` | N/A |
| write session 与 fencing | 已完成 | create/append/add block/commit/abort/renew/sync 具备会话和 fencing 校验。 | `metadata/src/service/fs_core/write_session.rs`、`metadata/src/session/write_session.rs` | N/A |
| worker membership service | 需要优化 | 注册、心跳、block report 服务端存在，但错误语义与仓库约定不一致。 | `metadata/src/worker/service.rs`、`metadata/tests/service_error_contract_tests.rs` | P1 |
| maintenance repair/delete/overrep | 需要优化 | 队列、planner、executor 存在，但部分 epoch/state/fault-domain 仍 TODO。 | `metadata/src/maintenance/` | P2 |
| Raft 网络 | TODO / 未完成 | network module 仍为 placeholder/unreachable。 | `metadata/src/raft/network.rs` | P2 |

#### 3.4.3 架构评价

`metadata` 的 authority 边界清晰，未在生产依赖中依赖 `worker` 或 `client`。`FsCore` 将 metadata 语义集中，`path_service` 保持 gRPC adapter 角色，符合“路径是 adapter，不是持久真相”的方向。主要不足集中在 worker service 的 wire 错误语义和多节点/maintenance 完整闭环。当前 `metadata/tests/service_error_contract_tests.rs` 约束 filesystem service 不返回业务 `Status`，但 MetadataWorkerService 被 allowlist 覆盖，说明风险是已知但未消除。

#### 3.4.4 主要问题

| 问题 | 类型 | 影响 | 建议 | 优先级 | 证据 |
|---|---|---|---|---|---|
| MetadataWorkerService 使用非 OK `Status` 表达业务/协议错误 | 架构风险 | client/worker 无法统一按 `ResponseHeader.error` 处理 recoverable 错误，破坏跨服务错误契约。 | 为 worker service response 增加/使用 structured header；业务错误走 OK + header，transport/framework 错误才非 OK。 | P1 | `metadata/src/worker/service.rs`、`metadata/tests/service_error_contract_tests.rs` |
| Raft network 仍 placeholder | 功能缺失 | 多 metadata 节点复制/选主 RPC 不完整，生产多节点语义待确认。 | 明确当前只支持单节点/单组，或落地 peer RPC client/server。 | P2 | `metadata/src/raft/network.rs` |
| maintenance delete executor 缺 expected state/epoch 校验 | 架构风险 | 删除/repair 对 worker 命令的 fencing 语义不完整。 | 接入 block/worker epoch 和 state watermark；补端到端测试。 | P2 | `metadata/src/maintenance/delete/executor.rs` |
| repair planner fault domain/hotness 仍 TODO | 可扩展性 | 大规模复制/迁移策略可能无法满足生产放置约束。 | 定义 placement policy 输入，先覆盖 fault-domain。 | P3 | `metadata/src/maintenance/repair/planner.rs` |

#### 3.4.5 建议后续动作

1. 优先统一 MetadataWorkerService 错误契约，删除 allowlist 或收窄到真正 transport failure。
2. 明确 metadata 当前部署拓扑：单节点/单组可用，还是多节点 Raft 可用。
3. 补齐 maintenance executor 的 epoch/state/fencing，并用真实 worker command ack 流程验证。
4. 后续新增 metadata config key 时，同步更新默认配置和配置矩阵。

### 3.5 worker

#### 3.5.1 模块职责

`worker` 是数据面 runtime，拥有本地 block store、chunk/stream IO、gRPC WorkerDataService、worker peer/router、worker typed config 和数据面核心校验。当前 `WorkerCore` 可处理 open read、open write、write stream、commit、sync committed block、abort，`FullBlockFileStore` 管理 staging/ready block 文件和 meta。

#### 3.5.2 功能完成情况

| 功能点 | 状态 | 说明 | 证据 | 优先级 |
|---|---|---|---|---|
| 本地 block store | 已完成 | staging/ready meta、atomic publish、recover、bounds、corruption 校验覆盖较强。 | `worker/src/store/block.rs`、`worker/src/store/meta_codec.rs` | N/A |
| read/write stream runtime | 已完成 | open/write/commit/read/sync/abort 有核心实现和 gRPC adapter。 | `worker/src/data/core.rs`、`worker/src/net/server/grpc.rs` | N/A |
| metadata-assigned `block_stamp` 写入 | 已完成 | write/commit/sync 要求非零 stamp，store 持久化 metadata supplied stamp。 | `worker/src/data/core.rs`、`worker/src/store/block.rs` | N/A |
| 普通 client 读的 block_stamp 防线 | 已完成 | client data boundary 拒绝 planned segment 零 stamp。 | `client/src/data/worker.rs` | N/A |
| 公开 WorkerDataService 读的 block_stamp 防线 | 存在风险 | worker read 只在请求 stamp 非零时校验，零值可绕过 freshness。 | `worker/src/runtime/block.rs` | P1 |
| worker metadata 注册/心跳/report loop | TODO / 未完成 | worker 二进制未发现主动调用 MetadataWorkerService 的生产路径。 | `worker/src/bin/main.rs`、`proto/metadata/worker.proto` | P1 |
| io_uring/SPDK/RDMA/QUIC | TODO / 未完成 | 代码为 placeholder 或 explicit unimplemented；默认配置不再把这些能力列为 deployable default。 | `worker/src/store/io/io_uring.rs`、`worker/src/store/io/spdk.rs`、`worker/src/net/peer/quic.rs`、`worker/src/net/peer/rdma.rs` | P2 |
| worker config | 已完成 | 默认配置只保留当前 worker data service/runtime 实际消费的 active key；metadata lifecycle、replication、多协议等仍为 deferred work。 | `worker/src/config.rs`、`conf/core-site.yaml`、`docs/CONFIG_MATRIX_ZH.md` | N/A |

#### 3.5.3 架构评价

`worker` 内部数据面和 store 边界基本干净：wire conversion 在 adapter，core 不持有 proto 类型，store 不引入 metadata authority。真正的系统风险在“数据面能运行”与“worker 作为集群成员可被 metadata 管理”之间仍未闭合。另一个重要风险是 worker 服务是公开 gRPC 服务时，其核心校验不能只依赖 client 的前置验证。

#### 3.5.4 主要问题

| 问题 | 类型 | 影响 | 建议 | 优先级 | 证据 |
|---|---|---|---|---|---|
| worker 二进制未接入 metadata 注册/心跳/block report | 功能缺失 | metadata 不能自动获得 worker endpoint、capacity、block location，client 读写闭环依赖手工或测试注入。 | 实现 worker control-plane loop，或在 README/config 明确当前需要外部注册器。 | P1 | `worker/src/bin/main.rs`、`metadata/src/worker/service.rs` |
| `validate_read` 允许 `block_stamp=0` 跳过 freshness 校验 | 安全/一致性风险 | 直接调用 WorkerDataService 可能读取过期或错误 generation 的 block。 | worker open_read 强制非零 stamp；若需要 debug bypass，放到受控 internal-only 接口。 | P1 | `worker/src/runtime/block.rs`、`proto/worker/data.proto` |
| protocol fallback 到 gRPC | 架构风险 | unknown/unspecified protocol 被静默视为 gRPC，违背 proto conversion strictness。 | 删除生产 fallback，保持 unknown/unspecified 为错误。 | P2 | `worker/src/net/protocol.rs`、`proto/src/convert.rs` |
| placeholder backend 暴露为 enum 能力 | 可维护性问题 | `io_uring`/`spdk`/QUIC/RDMA 容易被误认为已支持。 | 文档标 unsupported；真正接入前不进入 active config。 | P2 | `worker/src/store/io/config.rs`、`worker/src/store/io/io_uring.rs`、`worker/src/store/io/spdk.rs` |

#### 3.5.5 建议后续动作

1. 先修 worker `open_read` 零 stamp 校验，这是小改动但直接影响一致性边界。
2. 做 worker control-plane PR：读取 metadata endpoint、register、heartbeat、full/delta block report、command ack。
3. 将 protocol parsing 改成严格失败，并补 public service contract 测试。

### 3.6 client

#### 3.6.1 模块职责

`client` 提供 Vecton SDK facade，负责 public API、metadata gateway、layout cache、worker endpoint cache、retry/replay 分类、refresh、read planner、direct worker data adapter 和 write session orchestration。`client/src/lib.rs` 通过 `#![deny(missing_docs)]` 和 facade re-export 控制公开 surface。

#### 3.6.2 功能完成情况

| 功能点 | 状态 | 说明 | 证据 | 优先级 |
|---|---|---|---|---|
| public facade | 已完成 | `FsClient`、handle、options、status、errors 对外收敛，避免暴露 worker route/stamp。 | `client/src/lib.rs`、`client/src/api/` | N/A |
| metadata gateway | 已完成 | 统一校验 metadata response header、身份、refresh hint。 | `client/src/metadata/gateway.rs` | N/A |
| read planner 和 layout cache | 已完成 | 支持多 block range、coverage/gap/overlap/stamp 校验。 | `client/src/planner/read_planner.rs`、`client/src/cache/layout.rs` | N/A |
| worker data gRPC adapter | 已完成 | 普通读写携带 metadata-provided `block_stamp`，拒绝零 stamp 和 unsupported protocol。 | `client/src/data/worker.rs` | N/A |
| retry/replay/refresh | 已完成 | 对 metadata read/mutation/session barrier、worker unknown outcome、typed refresh 有分类。 | `client/src/runtime/` | N/A |
| write API | 已完成 | create/append/write_all/sync visibility/sync durability/close/abort/renew lease 可用。 | `client/src/api/fs_client.rs`、`client/src/api/handle.rs` | N/A |
| OpenOptions/AppendOptions | 待确认 | 当前为空 options struct，可能符合最小 API，也可能表示未来参数未落地。 | `client/src/api/options.rs` | P3 |
| client config docs | 已完成 | `conf/client-site.yaml` 已收敛到当前 operation/cache/pool/retry/refresh/backoff active keys。 | `client/src/config.rs`、`conf/client-site.yaml`、`docs/CONFIG_MATRIX_ZH.md` | N/A |

#### 3.6.3 架构评价

`client` 在近期重构后边界较好：metadata/worker wire 细节被 adapter 吸收，public facade 不暴露 worker endpoint、route epoch、block stamp 等内部信息。`FsClient` 的读路径坚持 all-or-error，写路径把 UnknownOutcome 与 session barrier 明确区分。当前默认 client config 已与 typed config active key 收敛。

#### 3.6.4 主要问题

| 问题 | 类型 | 影响 | 建议 | 优先级 | 证据 |
|---|---|---|---|---|---|
| 空 options struct 状态未说明 | 待确认 | API 使用者不清楚 open/append 是否真的无参数。 | 在 docs 或 rustdoc 中说明当前无可配置项，或补齐预期字段。 | P3 | `client/src/api/options.rs` |

#### 3.6.5 建议后续动作

1. 保持 public facade 测试，继续防止 worker route/stamp 泄漏到 SDK API。
2. 后续若 OpenOptions/AppendOptions 增加字段，同步补 API docs 和 config matrix。

### 3.7 ufs

#### 3.7.1 模块职责

`ufs` 负责外部后端抽象、后端能力描述、OpenDAL 集成和 registry。它不依赖 metadata/worker/client，符合外部 backend adapter 的边界。

#### 3.7.2 功能完成情况

| 功能点 | 状态 | 说明 | 证据 | 优先级 |
|---|---|---|---|---|
| UFS trait/spec/registry | 已完成 | 定义后端 ID、capability、registry upsert/get/remove/apply。 | `ufs/src/lib.rs`、`ufs/src/registry.rs`、`ufs/src/spec.rs` | N/A |
| OpenDAL adapter | 需要优化 | 支持 fs/s3/oss/hdfs 配置路径；部分后端依赖运行环境或 feature。 | `ufs/src/opendal_impl.rs`、`ufs/Cargo.toml` | P2 |
| rename fallback | 已完成 | 测试覆盖 fallback 行为。 | `ufs/src/lib.rs` tests | N/A |
| 默认环境测试 | 需要优化 | 部分 fs backend tests 被 ignored，说明默认测试环境未覆盖真实 IO。 | `cargo test --workspace` 输出、`ufs/src/lib.rs` | P3 |

#### 3.7.3 架构评价

`ufs` 没有把 metadata/worker/client policy 拉入外部后端 adapter。当前主要风险在可验证性：真实后端能力、凭据和运行环境不在默认测试中覆盖，部署文档也不足。

#### 3.7.4 主要问题

| 问题 | 类型 | 影响 | 建议 | 优先级 | 证据 |
|---|---|---|---|---|---|
| 真实后端测试依赖环境且默认 ignored | 测试不足 | fs/s3/oss/hdfs 后端适配不能从默认 CI 得到保证。 | 增加 feature-gated 或容器化 backend integration profile。 | P3 | `ufs/src/lib.rs`、`cargo test --workspace` ignored tests |
| HDFS/JVM feature 部署说明不足 | 文档缺失 | 用户难以判断如何启用和验证 HDFS 后端。 | 增加 backend matrix 和 feature/env 示例。 | P3 | `ufs/Cargo.toml` |

#### 3.7.5 建议后续动作

1. 写 UFS backend support matrix：fs/s3/oss/hdfs 的 feature、必需配置、测试状态。
2. 为 fs backend 提供默认可运行的临时目录 integration test，减少 ignored 覆盖。

### 3.8 integration_tests

#### 3.8.1 模块职责

`integration_tests` 是测试 crate，包含 client contract tests 和 mock metadata/worker server。它通过 dev-dependencies 依赖生产 crate，生产 crate 不依赖它，方向正确。

#### 3.8.2 功能完成情况

| 功能点 | 状态 | 说明 | 证据 | 优先级 |
|---|---|---|---|---|
| client contract mock tests | 已完成 | 验证 client 对 metadata/worker structured response 的处理。 | `integration_tests/tests/client_contract.rs` | N/A |
| raw proto mock servers | 已完成 | mock metadata/worker 使用 proto 契约，适合 wire contract 验证。 | `integration_tests/tests/common/mock_metadata.rs`、`integration_tests/tests/common/mock_worker.rs` | N/A |
| 真实 metadata+worker+client E2E | TODO / 未完成 | 当前主要是 mock contract，不是完整真实多进程或 in-process E2E。 | `integration_tests/tests/client_contract.rs` | P2 |
| placeholder 语义残留 | 需要优化 | 测试注释仍称 direct worker reads/writes 为 placeholder，与当前 worker core 已实现不完全一致。 | `integration_tests/tests/client_contract.rs` | P3 |

#### 3.8.3 架构评价

测试 crate 的边界正确，适合作为跨 crate contract 守门。当前不足是 mock contract 与真实系统闭环之间缺少一层：真实 metadata server、真实 worker data service、client SDK 三者之间的 read/write 生命周期测试。

#### 3.8.4 主要问题

| 问题 | 类型 | 影响 | 建议 | 优先级 | 证据 |
|---|---|---|---|---|---|
| 缺少真实 E2E | 测试不足 | worker lifecycle、block report、metadata layout、client read/write 真实闭环无法由默认测试保障。 | 增加 in-process metadata+worker+client test profile，先覆盖 single-group create/write/read/delete。 | P2 | `integration_tests/tests/client_contract.rs` |
| placeholder 注释/行为可能过期 | 文档缺失 | 测试意图与当前实现状态不一致，影响后续维护判断。 | 更新 contract tests 注释，区分 mock placeholder 与真实 worker implementation。 | P3 | `integration_tests/tests/client_contract.rs` |

#### 3.8.5 建议后续动作

1. 新增真实 E2E smoke：启动 metadata runtime、worker data server、注册 worker、client create/write/close/read。
2. 用 mock tests 保留错误契约覆盖，不替代真实生命周期测试。

### 3.9 配置、脚本与文档边界

#### 3.9.1 模块职责

`conf/` 承载示例配置，`docs/` 承载架构边界和审计文档。稳定化后，根目录 `Makefile` 提供本地 verify 入口，`.github/workflows/ci.yml` 提供最小 CI，`rust-toolchain.toml` 固定 Rust 1.95.0。

#### 3.9.2 功能完成情况

| 功能点 | 状态 | 说明 | 证据 | 优先级 |
|---|---|---|---|---|
| 架构边界文档 | 已完成 | 明确 common/types/proto/product crate 边界和反模式。 | `docs/ARCHITECTURE_BOUNDARIES.md` | N/A |
| 示例配置 | 已完成 | 默认配置文件只包含当前 runtime active consumed keys；planned/unimplemented 能力不作为 deployable default。 | `conf/core-site.yaml`、`conf/client-site.yaml`、`docs/CONFIG_MATRIX_ZH.md` | N/A |
| CI / local verify / toolchain | 已完成 | CI 和本地 verify 均执行 fmt-check、metadata、check、clippy、test；toolchain pin Rust 1.95.0。 | `.github/workflows/ci.yml`、`Makefile`、`rust-toolchain.toml` | N/A |
| docs 跟踪状态 | 已完成 | `docs/` 不再被整目录忽略，重要 Markdown 文档可被普通 status 发现。 | `.gitignore`、`git status --porcelain -uall docs` | N/A |

#### 3.9.3 架构评价

文档的架构规则足够明确，稳定化后配置、工具链、本地 verify 和 CI baseline 已与当前代码收敛。剩余部署差距主要来自系统闭环尚未实现，而不是默认配置或验证入口缺失。

#### 3.9.4 主要问题

| 问题 | 类型 | 影响 | 建议 | 优先级 | 证据 |
|---|---|---|---|---|---|
| deferred features 不能回流到默认配置 | 配置风险 | 后续实现前若把 worker lifecycle、多协议、replication 等 key 放回默认配置，会重新制造部署误判。 | 保持 CONFIG_MATRIX 的 active/planned/removed 分类；新增 active key 必须先证明 runtime consumer。 | P2 | `docs/CONFIG_MATRIX_ZH.md`、`conf/core-site.yaml` |

#### 3.9.5 建议后续动作

1. 后续新增配置 key 时同步更新 `docs/CONFIG_MATRIX_ZH.md`。
2. 继续保持 CI/local verify 与 Rust 1.95.0 baseline 对齐。

## 4. 核心链路审计

| 链路 | 涉及模块 | 当前状态 | 风险点 | 建议 | 证据 |
|---|---|---|---|---|---|
| 配置加载链路 | `common`、`metadata`、`client`、`worker`、`conf` | 已完成 | 默认配置已收敛到当前 active consumed keys；typed validation 由各消费模块负责。 | 新增 key 时必须同步 owner/consumer/status 和 wrong-type validation。 | `common/src/config/flat.rs`、`metadata/src/config.rs`、`client/src/config.rs`、`worker/src/config.rs`、`docs/CONFIG_MATRIX_ZH.md` |
| metadata 启动链路 | `metadata`、`common`、`proto`、`ufs` | 已完成 | 单节点/多节点部署语义需说明，Raft network placeholder。 | 文档明确当前 deployment mode；若要多节点，落地 peer network。 | `metadata/src/bin/main.rs`、`metadata/src/runtime.rs`、`metadata/src/raft/network.rs` |
| client metadata 请求链路 | `client`、`proto`、`metadata`、`common` | 已完成 | 依赖 header/error contract，filesystem service 测试覆盖较强。 | 保持 service_error_contract_tests，禁止业务错误走 Status。 | `client/src/metadata/gateway.rs`、`metadata/src/service/path_service.rs`、`metadata/tests/service_error_contract_tests.rs` |
| client read 链路 | `client`、`metadata`、`worker`、`types`、`proto` | 需要优化 | 正常 client 拒绝零 `block_stamp`，但 direct worker public read 可绕过。 | worker `open_read` 也强制非零 stamp。 | `metadata/src/service/fs_core/read.rs`、`client/src/data/worker.rs`、`worker/src/runtime/block.rs` |
| client write 链路 | `client`、`metadata`、`worker`、`proto` | 需要优化 | create/add/write/commit/sync 实现较完整，但 worker lifecycle 未自动把 block location/report 回 metadata。 | 打通 worker register/report 与 metadata layout 可见性。 | `client/src/api/fs_client.rs`、`worker/src/data/core.rs`、`metadata/src/service/fs_core/write_session.rs` |
| worker 数据面执行链路 | `worker`、`proto`、`types` | 已完成 | gRPC control calls 走 structured header，streaming error taxonomy 较窄。 | 对 streaming 失败分类补 contract tests。 | `worker/src/net/server/grpc.rs`、`worker/src/data/core.rs` |
| worker 到 metadata 控制链路 | `worker`、`metadata`、`proto` | TODO / 未完成 | metadata 服务端存在，worker 生产调用方未发现。 | 实现 worker control-plane loop 或标明外部组件负责。 | `proto/metadata/worker.proto`、`metadata/src/worker/service.rs`、`worker/src/bin/main.rs` |
| protocol 序列化/转换链路 | `proto`、`types`、`common`、各产品 crate | 需要优化 | 主转换集中，但 worker net protocol 本地 fallback 破坏一致性。 | 删除宽松 fallback，统一 strict parse。 | `proto/src/convert.rs`、`worker/src/net/protocol.rs` |
| 错误处理与重试链路 | `common`、`client`、`metadata`、`worker` | 需要优化 | filesystem/client 链路成熟；MetadataWorkerService 仍大量非 OK business Status。 | worker metadata service 改为 header error。 | `common/src/error/mod.rs`、`client/src/runtime/`、`metadata/src/worker/service.rs` |
| maintenance/repair/delete 链路 | `metadata`、`worker`、`proto` | 需要优化 | metadata 侧队列/路由存在，但 worker command consumer 与 epoch/state fencing 待补。 | 先实现真实 command ack E2E，再完善 repair policy。 | `metadata/src/maintenance/`、`metadata/src/worker/command_router.rs` |

## 5. 架构问题汇总

### 5.1 模块边界

- 当前状态：`common`、`types`、`proto`、`client`、`metadata`、`worker`、`ufs` 的总体职责符合 `AGENTS.md` 和 `docs/ARCHITECTURE_BOUNDARIES.md`。`types` 未依赖 workspace crate，`proto` 只依赖 `types/common`，产品 crate 未出现明显生产循环依赖。
- 主要风险：`worker/src/net/protocol.rs` 在本地重新解释 proto protocol 并 fallback 到 gRPC；`proto` active surface 包含未实现 service。
- 建议改进：保持转换集中在 `proto/src/convert.rs`，产品 crate 只保留策略判断；对 generated API surface 做 active/planned/removed 标注。

### 5.2 依赖方向

- 当前状态：通过 `cargo metadata` 和 `cargo tree` 观察，`metadata` 生产依赖未依赖 `worker/client`，`worker` 未依赖 `metadata/client`，`client` 未依赖 `metadata/worker`，`ufs` 未依赖产品 runtime。
- 主要风险：`metadata` manifest 中存在 `client` dev-dependency，当前用于测试可以接受，但测试 helper 不应演化成生产工具。
- 建议改进：继续用 dependency boundary tests 或 `cargo tree` 检查防回归；如果引入新 shared helper，先确认 owner。

### 5.3 数据模型与协议

- 当前状态：`GroupStateWatermark`、`block_stamp`、`worker_epoch`、`route_epoch`、`mount_epoch` 等领域分界较清楚；proto/domain 转换有非零和 known enum 校验。
- 主要风险：direct worker read 的零 `block_stamp`、worker protocol fallback、admin/metapeer 未实现 surface。
- 建议改进：worker 服务入口也执行与 client/planner 等价的结构校验；对 schema contract 状态建立文档。

### 5.4 错误处理

- 当前状态：filesystem metadata service 和 client gateway 基本使用 gRPC OK + structured header 表达业务错误；client retry/replay 分类成熟。
- 主要风险：MetadataWorkerService 仍用 `Status::invalid_argument`、`Status::failed_precondition`、`Status::internal` 等承载业务/协议错误；worker streaming path 的错误分类不如 unary control path 完整。
- 建议改进：worker metadata service response proto 增加或统一 header；streaming error 保留可恢复业务错误信息。

### 5.5 配置与部署

- 当前状态：各模块 typed config 存在，默认 `conf/` 文件已收敛到当前 runtime active consumed keys；`docs/CONFIG_MATRIX_ZH.md` 记录 active/planned/test-only/removed 状态；`rust-toolchain.toml`、`Makefile`、CI 已建立 Rust 1.95.0 baseline。
- 主要风险：后续 deferred worker metadata lifecycle、storage backend、多协议或 replication key 在实现前被重新放入默认配置。
- 建议改进：保持配置矩阵作为默认配置 review gate；新增 active key 必须先有 runtime consumer 和 validation。

### 5.6 可观测性

- 当前状态：`common/src/observe`、metrics、tracing、audit 基础存在，metadata/worker/client 有部分 metrics/test。
- 主要风险：worker lifecycle 未闭合时，集群级健康、heartbeat lag、block report lag、repair backlog 无法形成端到端可观测闭环；audit queue size placeholder 可能误导。
- 建议改进：worker control-plane 落地后，统一暴露 register/heartbeat/report/repair metrics。

### 5.7 可测试性

- 当前状态：默认 `cargo test --workspace` 通过，client/metadata/worker 单元和 crate-level contract 覆盖较强。
- 主要风险：真实 metadata+worker+client E2E 缺失；UFS 真实后端和部分 lease/list 测试 ignored；mock placeholder 注释有漂移。
- 建议改进：增加 single-process E2E profile 和 backend integration profile；将 ignored 测试原因写入测试矩阵。

### 5.8 可扩展性

- 当前状态：架构上为 worker endpoint epoch、多协议、maintenance、multi-group msync、UFS 多后端预留了位置。
- 主要风险：过多 planned surface 未标明状态，会增加维护成本；Raft network 和 multi-group 语义未闭合。
- 建议改进：短期收敛 active surface，长期再扩展 QUIC/RDMA、multi-group、fault-domain aware repair。

## 6. 测试与质量审计

### 6.1 验证命令结果

| 命令 | 结果 | 说明 |
|---|---|---|
| `rustc --version` | 通过 | `rustc 1.95.0 (59807616e 2026-04-14)`。 |
| `cargo metadata --format-version 1 --no-deps` | 通过 | 成功识别 8 个 workspace members；输出显示 `common/types/proto` 为 edition 2024，其他主要 crate 为 edition 2021，均由 Rust 1.95.0 baseline 支持。 |
| `cargo fmt --all --check` | 通过 | 命令退出 0。 |
| `cargo check --workspace` | 通过 | `Finished dev profile`，当前源码树可编译。 |
| `cargo clippy --workspace --all-targets -- -D warnings` | 通过 | `Finished dev profile`，未触发 warning-as-error。 |
| `cargo test --workspace` | 通过 | workspace tests 和 doctests 通过；输出包含若干 ignored tests，如 metadata lease/list follow-up、UFS filesystem permission tests、doctest ignored。 |
| `git diff --check` | 通过 | 命令退出 0；`docs/` 已可被 git status 发现。 |

### 6.2 测试覆盖概览

| 模块 | 测试情况 | 风险 | 建议 |
|---|---|---|---|
| `common` | 单元测试覆盖 header/error/config/observe/retry 基础语义 | 部分 placeholder 指标和 deadline 默认未形成强 contract | 为 TODO 项补 policy test 或移除 TODO。 |
| `types` | ID、ACL、fs、layout、chunk、serde roundtrip 覆盖 | symlink/future fields 无完整流程 | 未公开前保持低优先级；公开前补完整测试。 |
| `proto` | conversion tests 覆盖 ID/header/watermark/location/protocol | active surface service 实现状态未测试 | 增加 static test 检查未实现 service 的状态标注。 |
| `client` | 单元/contract 覆盖 public facade、read/write、cache、retry/replay、worker adapter | 真实 worker lifecycle 不在 client tests 中 | 由 integration E2E 覆盖真实 metadata+worker+client。 |
| `metadata` | 单元和 regression tests 很强，覆盖 FsCore、Raft apply/storage、freshness、worker service、maintenance | Raft network placeholder、worker service error allowlist、ignored tests | 补 worker service structured error contract，明确 ignored tests 去留。 |
| `worker` | 本地 store、data core、gRPC adapter、config、proto shape、sync committed block 覆盖较强 | public zero stamp bypass 未被负例锁住；metadata lifecycle 缺测试 | 增加 direct WorkerDataService zero stamp rejection test；新增 control-plane tests。 |
| `ufs` | registry/capability/fallback 有覆盖 | 真实 backend tests ignored | 增加环境隔离的 fs backend integration，后端 matrix 分层运行。 |
| `integration_tests` | client contract mock tests | 缺真实 E2E | 新增 single-group end-to-end smoke。 |

## 7. 文档审计

| 文档 / 路径 | 当前状态 | 问题 | 建议 |
|---|---|---|---|
| `README.md` | 已完成 | 已描述 Rust 1.95.0 baseline、local verify、当前 active runtime 和明确 deferred items。 | 后续行为变化时同步更新。 |
| `AGENTS.md` | 已完成 | 架构边界和执行规则明确。 | 继续作为 normative agent contract。 |
| `common/AGENTS.md` 等子目录 agent 文件 | 已完成 | 本次审计已按触达模块读取；规则与根文档一致。 | 保持本地规则优先级。 |
| `docs/ARCHITECTURE_BOUNDARIES.md` | 已完成 | 内容与当前边界基本一致。 | 后续只在边界真实变化时更新。 |
| `docs/20260524_GLOBAL_SYSTEM_AUDIT_ZH.md` | 已完成 | 本报告已更新为 post-stabilization-aware audit record，不再把已解决的 baseline 问题列为当前风险。 | 后续审计需区分 historical finding 和 current state。 |
| `conf/core-site.yaml` | 已完成 | 仅保留 metadata/worker 当前 runtime active consumed keys。 | deferred features 不进入默认配置。 |
| `conf/client-site.yaml` | 已完成 | 仅保留 client 当前 runtime active consumed keys。 | deferred client modes 不进入默认配置。 |
| `metadata/README_ZH.md`、`metadata/ARCHITECTURE_ZH.md` | 需要优化 | metadata 局部文档存在，但需与当前 worker service、maintenance、single-group 语义保持同步。 | 对照 `metadata/src/runtime.rs` 和 FsCore 当前行为刷新。 |
| CI/local verify/toolchain | 已完成 | `.github/workflows/ci.yml`、`Makefile`、`rust-toolchain.toml` 已建立最小 baseline。 | 后续如新增验证 profile，不应让 default verify 依赖外部服务。 |

## 8. 风险分级汇总

| 优先级 | 问题 | 涉及模块 | 影响 | 建议 |
|---|---|---|---|---|
| P1 | worker metadata lifecycle 未闭合 | `worker`、`metadata`、`proto` | worker 无法自动注册/心跳/report，metadata block location 与 worker 状态依赖外部注入或测试路径。 | 实现 worker control-plane loop，或明确外部组件责任。 |
| P1 | WorkerDataService 公开读允许 `block_stamp=0` 绕过 | `worker`、`client`、`proto` | 直接 worker 请求可能绕过 freshness/generation 校验。 | worker `open_read` 强制非零 stamp。 |
| P1 | MetadataWorkerService 错误契约不一致 | `metadata`、`common`、`proto` | recoverable 业务/协议错误走非 OK Status，破坏统一 header error 语义。 | 改为 OK + structured header，transport/framework 才非 OK。 |
| P2 | admin/metapeer proto service 未实现但导出 | `proto`、`metadata` | 外部 API surface 状态不清。 | 标注 planned/stale 或删除/隔离。 |
| P2 | Raft network placeholder | `metadata` | 多节点 metadata 不完整。 | 明确单节点支持或实现 peer RPC。 |
| P2 | io_uring/SPDK/QUIC/RDMA placeholder | `worker` | 实现前不能作为 deployable capability。 | 继续排除在默认配置之外；实现和验证完成后再进入 active config。 |
| P2 | 缺少真实 metadata+worker+client E2E | `integration_tests`、`client`、`metadata`、`worker` | 默认测试无法证明完整系统闭环。 | 新增 single-group E2E smoke。 |
| P3 | header deadline 默认 TODO | `common` | 默认超时策略不清。 | 明确 config 注入或无默认。 |
| P3 | symlink target placeholder | `types`、`metadata` | symlink 能力未完整。 | 未公开前标未完成，公开前补全链路。 |
| P3 | UFS 后端默认测试不足 | `ufs` | 后端适配不能由默认测试证明。 | 增加 feature-gated/backend profile。 |

## 9. 后续路线图建议

### 9.1 短期：必须优先处理

1. 在 `worker/src/runtime/block.rs::validate_read` 或更早入口强制 `ReadOpenRequest.block_stamp != 0`。
2. 将 `metadata/src/worker/service.rs` 的 recoverable 业务/协议错误改为 structured header error，收敛 `service_error_contract_tests` allowlist。
3. 决定 worker metadata lifecycle 责任：若 worker 自身负责，后续 PR 实现 register/heartbeat/block report；若外部组件负责，必须在 README 和 config 中明确。

### 9.2 中期：架构与质量提升

1. 新增真实 E2E：metadata runtime + worker data service + worker registration + client create/write/read/delete。
2. 对 proto active surface 做清理：admin、metapeer 明确 active/planned/removed/delete。
3. 继续保持 README、配置文档、CI/local verify 与 Rust 1.95.0 baseline 同步。

### 9.3 长期：演进方向

1. 完成 metadata 多节点 Raft network 或明确生产只支持单节点/single-group 的阶段性 contract。
2. 将 maintenance repair/delete 与 worker command ack 打通，补齐 epoch/state/fencing。
3. 在明确需求后再实现 QUIC/RDMA、io_uring/SPDK，避免 placeholder 先污染 active surface。
4. 建立 UFS backend integration matrix，覆盖 fs/s3/oss/hdfs 的 feature、凭据、环境和测试状态。
5. 在 worker lifecycle 闭合后完善集群级可观测性：worker live set、heartbeat lag、block report lag、repair backlog、read/write error taxonomy。

## 10. 总结

Vecton 当前处于“核心模块边界较成熟、单模块功能测试较强、完整系统运行闭环仍需补齐”的阶段。近期对 `common`、`types`、`proto`、`client`、`metadata`、`worker` 的重构已经明显改善了结构：shared crate 没有明显承担产品 runtime policy，proto/domain 转换集中度较高，client public facade 较干净，metadata authority 和 worker local data-plane 的测试覆盖也较强。

主要已完成区域包括：typed domain model、共享 proto conversion、metadata filesystem service、FsCore freshness/write session、client read/write/retry/cache、worker local block store、worker gRPC data-plane、workspace 级 fmt/check/clippy/test 基线。

主要优化区域包括：worker control-plane lifecycle、worker 公开入口防御性校验、MetadataWorkerService 错误契约、proto active surface 状态、真实 E2E 测试。

主要 TODO 区域包括：metadata Raft network、worker register/heartbeat/block report 生产 loop、io_uring/SPDK/QUIC/RDMA、部分 maintenance fencing、UFS backend integration profile。

建议下一轮审计或重构聚焦在 `worker` 与 `metadata` 的系统闭环，而不是再次大规模调整 shared crate。优先把 worker 注册、心跳、block report、metadata block location、client read/write 形成真实端到端可验证链路；随后再处理 proto active surface 和部署文档的下一层细化。
