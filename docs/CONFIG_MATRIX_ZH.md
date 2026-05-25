# Vecton 配置矩阵

本文记录当前默认配置文件中可部署的 active 配置键，以及从默认配置中移除或延期的配置项。默认配置文件只包含 active 键。

## active 配置

| key | owner module | actual consumer | default value | status | validation behavior | notes |
| --- | --- | --- | --- | --- | --- | --- |
| `metadata.rpc.addr` | `metadata` | `MetadataConfig::from_core_config` / `serve` | `0.0.0.0` | active | 与 port 组合后必须是合法 `SocketAddr` | Metadata RPC bind host. |
| `metadata.rpc.port` | `metadata` | `MetadataConfig::from_core_config` / `serve` | `18080` | active | 必须在 `1..=65535` | Metadata RPC bind port. |
| `metadata.storage.dir` | `metadata` | `build_authority` / `RocksDBStorage::open` | `data/metadata` | active | 非空路径由 RocksDB 打开时校验 | 唯一当前 metadata 存储目录配置入口。 |
| `metadata.authz.filesystem.mode` | `metadata` | `filesystem_permission_checker` | `NONE` | active | 解析为已知枚举；当前只有 `NONE` 可启动 | `ACL`/`RANGER` 不在默认配置中。 |
| `metadata.authority.group_id` | `metadata` | `ensure_root_mount` / `MsyncHandler` | `1` | active | 解析为非负整数 | 当前生产基线是单 group。 |
| `metadata.raft.node_id` | `metadata` | `AppRaftNode::new` | `1` | active | 必须大于 0 | 当前节点在 authority group 内的 Raft ID。 |
| `metadata.raft.peers` | `metadata` | `MetadataConfig::from_core_config` / `AppRaftNode::new` | `""` | active | 逗号分隔，空字符串表示无 peer | 多节点网络仍是延期项。 |
| `metadata.bootstrap.root_ready_initial_backoff_ms` | `metadata` | `wait_for_root_ready_with_metrics` | `200` | active | 必须大于 0 | Root mount readiness 初始退避。 |
| `metadata.bootstrap.root_ready_max_backoff_ms` | `metadata` | `wait_for_root_ready_with_metrics` | `5000` | active | 必须大于 0 | Root mount readiness 最大退避。 |
| `metadata.bootstrap.root_ready_warn_after_ms` | `metadata` | `wait_for_root_ready_with_metrics` | `60000` | active | 必须大于 0 | Root mount readiness 告警阈值。 |
| `metadata.repair.max_queue_size` | `metadata` | `RepairQueue::with_config_and_metrics` | `10000` | active | 必须大于 0 | Metadata repair queue 容量。 |
| `metadata.repair.max_attempts` | `metadata` | `RepairQueue::with_config_and_metrics` | `3` | active | 必须大于 0 且能放入 `u32` | Repair task 最大尝试次数。 |
| `metadata.repair.inflight_timeout_ms` | `metadata` | `RepairQueue::with_config_and_metrics` | `300000` | active | 必须大于 0 | In-flight repair timeout。 |
| `metadata.repair.initial_backoff_ms` | `metadata` | `RepairQueue::with_config_and_metrics` | `1000` | active | 必须大于 0 | Repair retry 初始退避。 |
| `metadata.repair.max_backoff_ms` | `metadata` | `RepairQueue::with_config_and_metrics` | `60000` | active | 必须大于 0 | Repair retry 最大退避。 |
| `metadata.repair.worker_inflight_limit` | `metadata` | `RepairQueue::with_config_and_metrics` | `4` | active | 必须大于 0 | 单 worker in-flight repair 限制。 |
| `worker.id` | `worker` | `WorkerConfig::from_core_config` / `resolve_worker_id` | 无默认值 | active optional | 如果存在，必须是非 0 整数；非法时启动失败，不静默生成新 identity | 显式稳定 `WorkerId`，跨 worker 进程重启保持不变。 |
| `worker.identity.path` | `worker` | `resolve_worker_id` | `./data/worker.identity` | active | `worker.id` 缺省时使用；文件缺失时生成 UUID、写入并 fsync，后续启动复用同一文件 | 本地持久 WorkerId 来源；文件内容为 UUID，运行时折叠为当前 `WorkerId(u64)`。 |
| `worker.rpc.bind` | `worker` | `WorkerConfig::from_core_config` / `serve_worker_data_with_registration` | `0.0.0.0:9090` | active | gRPC listener 必须是合法 `SocketAddr` | Worker data service 本地监听地址，不作为 metadata 注册 endpoint。 |
| `worker.rpc.advertised_endpoint` | `worker` | `WorkerConfig::from_core_config` / `MetadataRegistrar` | `http://127.0.0.1:9090` | active | 必须显式存在，必须是合法 endpoint URI，包含可用 host 和 port，host 不能是 `0.0.0.0` 或 `::` | 注册到 metadata 并返回给 client 的 worker data endpoint。 |
| `worker.rpc.max_inflight` | `worker` | `WorkerConfig::from_core_config` / net listener config | `100` | active | 必须大于 0 | 每连接并发上限。 |
| `worker.default_frame_size` | `worker` | `WorkerCore::with_options` | `1MB` | active | 必须大于 0 且不超过 `max_frame_size` | Transport frame 默认载荷大小。 |
| `worker.max_frame_size` | `worker` | `WorkerCore::with_options` | `4MB` | active | 必须大于 0 | Transport frame 最大载荷大小。 |
| `worker.window_bytes` | `worker` | `WorkerCore::with_options` | `8MB` | active | 必须大于 0 | Per-stream 应用层 in-flight window。 |
| `worker.chunk_size` | `worker` | `WorkerCore::with_options` | `1MB` | active | 必须大于 0 | Worker-local StorageChunk 大小。 |
| `worker.stream.idle_timeout_ms` | `worker` | `WorkerCore::with_options` | `60000` | active | 必须大于 0 | Runtime stream idle timeout。 |
| `worker.storage.root` | `worker` | `WorkerCore::with_options` / local block store | `./data` | active | 路径字符串不能为空 | 当前只支持单 worker-local storage root。 |
| `worker.metadata.group_id` | `worker` | `MetadataRegistrar` / worker startup | `1` | active | 必须大于 0 | Worker 启动注册目标 metadata group；当前默认配置只声明一个 group。 |
| `worker.metadata.endpoint` | `worker` | `MetadataRegistrar` / worker startup | `http://127.0.0.1:18080` | active | 必须显式存在，且必须是合法 tonic endpoint URI，包含 `http://` 或 `https://` scheme | Worker 启动注册使用的 MetadataWorkerService leader endpoint。 |
| `worker.metadata.endpoints` | `worker` | `MetadataHeartbeatLoop` | `http://127.0.0.1:18080` | active | 逗号分隔；至少一个合法 tonic endpoint URI，每项包含 `http://` 或 `https://` scheme | Worker heartbeat fanout 目标 metadata peers；默认单节点与 `worker.metadata.endpoint` 相同，多 peer 时 heartbeat 会发送到所有配置项，包括 follower。 |
| `worker.metadata.register_timeout_ms` | `worker` | `MetadataRegistrar` / `MetadataHeartbeatLoop` | `5000` | active | 必须大于 0 | 单次 register 与当前 heartbeat RPC 连接/请求 timeout；独立 heartbeat RPC timeout 配置尚未接入。 |
| `worker.metadata.register_retry_initial_backoff_ms` | `worker` | `MetadataRegistrar` | `200` | active | 必须大于 0 | register retry 初始退避。 |
| `worker.metadata.register_retry_max_backoff_ms` | `worker` | `MetadataRegistrar` | `5000` | active | 必须大于 0，且不小于 initial backoff | register retry 最大退避。 |
| `client.id` | `client` | `ClientConfig::client_id` / `FsClient::new` | `1` | active | metadata 操作要求非 0，且不能为负数 | Client request identity。 |
| `client.metadata.endpoints` | `client` | `RefreshManager::from_config` | `127.0.0.1:18080` | active | 必须至少有一个非空 endpoint | 逗号分隔 metadata endpoint。 |
| `client.metadata.group_ids` | `client` | `RefreshManager::from_config` | `1` | active | 必须至少有一个非 0 group id | 与 endpoints 按顺序配对，缺少 endpoint 时复用第一个 endpoint。 |
| `client.retry.max_retry_attempts` | `client` | `OperationExecutor` retry budget | `3` | active | 必须非负 | 逻辑操作 retry 上限。 |
| `client.retry.metadata_budget` | `client` | `OperationExecutor` metadata retry budget | `3` | active | 必须非负 | 会被 `max_retry_attempts` 截断。 |
| `client.retry.worker_budget` | `client` | worker data boundary retry budget | `3` | active | 必须非负 | 会被 `max_retry_attempts` 截断。 |
| `client.retry.session_barrier_budget` | `client` | write-session barrier retry budget | `0` | active | 必须非负 | 默认不重试 session barrier。 |
| `client.refresh.max_attempts` | `client` | refresh/replay policy | `3` | active | 必须非负 | refresh 尝试上限。 |
| `client.operation.timeout_ms` | `client` | `AttemptContext::with_operation_timeout_ms` | `null` | active | 存在时必须非负整数 | `null` 表示不设置 per-operation deadline。 |
| `client.backoff.initial_ms` | `client` | `BackoffPolicy::from_config` | `100` | active | 必须非负 | Retry 初始退避。 |
| `client.backoff.max_ms` | `client` | `BackoffPolicy::from_config` | `5000` | active | 必须非负且不小于 initial | Retry 最大退避。 |
| `client.backoff.multiplier` | `client` | `BackoffPolicy::from_config` | `2.0` | active | 必须是有限数字且不小于 1.0 | 指数退避倍率。 |
| `client.cache.layout.enabled` | `client` | `LayoutCache::from_config` | `false` | active | 必须是 boolean | Validated read-layout cache 开关。 |
| `client.cache.layout.ttl_secs` | `client` | `LayoutCache::from_config` | `0` | active | 必须非负 | 0 表示命中立即失效。 |
| `client.cache.layout.max_entries` | `client` | `LayoutCache::from_config` | `1024` | active | cache 开启时必须大于 0 | Layout cache 容量。 |
| `client.cache.layout.singleflight.enabled` | `client` | `FsClient::load_layout` | `true` | active | 必须是 boolean | 合并并发 layout miss。 |
| `client.cache.worker_endpoint.enabled` | `client` | `WorkerEndpointCache::from_config` | `false` | active | 必须是 boolean | Metadata-authoritative worker endpoint cache 开关。 |
| `client.cache.worker_endpoint.ttl_secs` | `client` | `WorkerEndpointCache::from_config` | `0` | active | 必须非负 | 0 表示命中立即失效。 |
| `client.cache.worker_endpoint.max_entries` | `client` | `WorkerEndpointCache::from_config` | `1024` | active | cache 开启时必须大于 0 | Worker endpoint cache 容量。 |
| `client.cache.worker_endpoint.singleflight.enabled` | `client` | worker endpoint refresh path | `true` | active | 必须是 boolean | 合并并发 endpoint miss。 |
| `client.cache.worker_endpoint.health.enabled` | `client` | worker data boundary endpoint health | `true` | active | 必须是 boolean | 临时 endpoint penalty 开关。 |
| `client.cache.worker_endpoint.health.failure_threshold` | `client` | `WorkerEndpointCache::from_config` | `2` | active | health 开启时必须大于 0 | 连续失败阈值。 |
| `client.cache.worker_endpoint.health.ttl_secs` | `client` | `WorkerEndpointCache::from_config` | `5` | active | 必须非负 | endpoint penalty TTL。 |
| `client.channel_pool.metadata.enabled` | `client` | `MetadataGateway::from_config` | `true` | active | 必须是 boolean | Metadata channel pool 开关。 |
| `client.channel_pool.metadata.max_per_group` | `client` | `MetadataGateway::from_config` | `1` | active | 必须大于 0 | 每 group 最大 metadata channel 数。 |
| `client.channel_pool.metadata.singleflight.enabled` | `client` | metadata gateway channel creation | `true` | active | 必须是 boolean | 合并并发 metadata channel 创建。 |
| `client.channel_pool.worker.enabled` | `client` | `TonicWorkerDataClient::from_config` | `true` | active | 必须是 boolean | Worker channel pool 开关。 |
| `client.channel_pool.worker.max_per_worker` | `client` | `TonicWorkerDataClient::from_config` | `1` | active | 必须大于 0 | 每 worker 最大 channel 数。 |
| `client.channel_pool.worker.singleflight.enabled` | `client` | worker channel creation | `true` | active | 必须是 boolean | 合并并发 worker channel 创建。 |

## planned 配置

以下能力可以在设计文档中讨论，但当前不出现在默认配置文件中：

- worker block report / command ack 生产循环相关配置；本轮只激活 startup register 与 heartbeat liveness fanout，未激活 block report 或 command ack 配置。
- heartbeat timing 配置 `worker.heartbeat.interval`、`worker.heartbeat.rpc.timeout`、`worker.heartbeat.max_backoff`、`metadata.worker.heartbeat.timeout`、`metadata.worker.heartbeat.expire_scan_interval` 尚未接入 typed config。PR-6 当前使用保守内部默认：worker heartbeat loop 固定 1s，heartbeat RPC timeout 复用 `worker.metadata.register_timeout_ms`，metadata runtime liveness timeout 当前由 `WorkerManager` 构造参数注入；这些 key 不属于当前支持的 active 配置。
- worker replication、relocation、delete command 执行策略配置。
- QUIC、RDMA、io_uring、SPDK 数据路径配置。
- UFS per-instance 部署配置。
- client 读写模式、consistency mode、直接读开关等尚未接入 runtime 决策的配置。
- observability 文件化配置；当前 metadata/worker 进程使用代码默认 `ObservabilityConfig`。

## test-only 配置

当前默认配置没有 test-only key。针对已移除 key 的回归测试只用于证明这些 key 不会影响当前 runtime 配置，不代表它们是受支持的测试配置入口。

## 已移除配置

| key / pattern | status | reason |
| --- | --- | --- |
| `VECTON_METADATA_DB_PATH` | removed | Metadata storage 目录只通过 `metadata.storage.dir` 配置。 |
| `metadata.worker.max_commands_per_heartbeat` | removed | 当前 heartbeat command 上限在 `MetadataWorkerServiceImpl` 中固定，配置字段没有 runtime 消费方。 |
| `metadata.shard.*` | removed | 当前 authority group 使用 `metadata.authority.group_id`；测试仅证明已移除入口不会影响当前 authority 配置。 |
| `metadata.raft.storage.dir` | removed | 当前 metadata 持久化根目录是 `metadata.storage.dir`。 |
| `observe.*` | removed | 默认配置文件不再列出尚未从文件接入的 observability 键。 |
| `worker.storage.dirs`, `worker.storage.dir` | removed | 当前 worker 只消费 `worker.storage.root`。 |
| `worker.storage.chunk_size` | removed | 当前 worker 只消费 `worker.chunk_size`。 |
| `worker.stream.window_bytes` | removed | 当前 worker 只消费 `worker.window_bytes`。 |
| `worker.transport.default_frame_size`, `worker.transport.max_frame_size` | removed | 当前 worker 只消费 `worker.default_frame_size` / `worker.max_frame_size`；测试仅证明已移除入口不会影响当前 frame 配置。 |
| `worker.storage.kind` | removed | `io_uring`/`spdk` 不是当前可部署 storage backend。 |
| `worker.concurrency.*` | removed | 当前 worker runtime 不消费这些键。 |
| `worker.eviction.*` | removed | 当前 worker runtime 不消费这些键。 |
| `worker.orphan.*` | removed | 当前 worker runtime 不消费这些键。 |
| `worker.volume_health.*` | removed | 当前 worker runtime 不消费这些键。 |
| `worker.ufs.*`, `ufs.*` | removed | 当前默认配置不声明未接线的 UFS 部署项。 |
| `worker.replication.*` | removed | worker replication 执行策略尚未作为 active 配置接入。 |
| `worker.metadata.heartbeat.*`, `worker.metadata.block_report.*`, `worker.metadata.command_ack.*` | removed | 当前 worker heartbeat 只消费 `worker.metadata.endpoints` 和 register timeout；独立 heartbeat timing 配置尚未接入 typed config。block report / command ack 留到后续 PR。 |
| `client.default_timeout_ms` | removed | 当前 client 使用 `client.operation.timeout_ms`。 |
| `client.metadata.group_id` | removed | 当前 client 只消费 `client.metadata.group_ids`。 |
| `client.consistency.*` | removed | 当前 client runtime 不消费 consistency 配置。 |
| `client.read_mode.*` | removed | 当前 client runtime 不消费 read mode 配置。 |
| `client.write_mode.*` | removed | 当前 client runtime 不消费 write mode 配置。 |
| `client.cache.file_meta.*` | removed | 当前 client runtime 不消费 file metadata cache 配置。 |
| `client.cache.route.*` | removed | 当前 client runtime 不消费 route cache 配置。 |
| `client.worker.direct_read.*` | removed | 当前 direct-worker 行为由 metadata layout 和 worker endpoint 信息驱动，不通过这些键配置。 |
| `client.retry.max_retries`, `client.retry.initial_backoff_ms`, `client.retry.max_backoff_ms`, `client.retry.backoff_multiplier` | removed | 当前 retry/backoff 键为 `client.retry.max_retry_attempts` 与 `client.backoff.*`。 |
