// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Refresh manager entry point.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use types::{GroupStateWatermark, ShardGroupId};

use crate::cache::StateIdCache;
use crate::cache::{CacheInvalidationReason, LayoutCache, WorkerEndpointCache};
use crate::canonical::RefreshHint;
use crate::error::{ClientError, ClientResult};
use crate::runtime::classify::RefreshReason;
use crate::runtime::context::{AttemptContext, OperationContext};

/// Configured metadata group bootstrap target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfiguredMetadataGroup {
    /// Non-zero metadata group id.
    pub group_id: u64,
    /// Metadata endpoint for this group.
    pub endpoint: String,
}

#[derive(Debug)]
struct RefreshState {
    configured_groups: Vec<ConfiguredMetadataGroup>,
    leader_cache: HashMap<u64, String>,
    mount_route_cache: HashMap<String, u64>,
    mount_epoch_cache: HashMap<String, u64>,
    route_epoch_cache: HashMap<String, u64>,
}

/// Owns correctness cache updates after structured refresh signals.
#[derive(Clone, Debug)]
pub struct RefreshManager {
    state: Arc<RwLock<RefreshState>>,
    watermarks: StateIdCache,
    layout_cache: Option<LayoutCache>,
    worker_endpoint_cache: Option<WorkerEndpointCache>,
}

impl RefreshManager {
    /// Create a refresh manager from configured non-zero metadata groups.
    pub fn new(configured_groups: Vec<ConfiguredMetadataGroup>) -> ClientResult<Self> {
        if configured_groups.is_empty() || configured_groups.iter().any(|group| group.group_id == 0) {
            return Err(ClientError::InvalidArgument(
                "RefreshManager requires at least one non-zero metadata group".to_string(),
            ));
        }
        Ok(Self {
            state: Arc::new(RwLock::new(RefreshState {
                configured_groups,
                leader_cache: HashMap::new(),
                mount_route_cache: HashMap::new(),
                mount_epoch_cache: HashMap::new(),
                route_epoch_cache: HashMap::new(),
            })),
            watermarks: StateIdCache::new(300),
            layout_cache: None,
            worker_endpoint_cache: None,
        })
    }

    /// Attach correctness caches owned by the client runtime.
    pub(crate) fn with_caches(
        mut self,
        layout_cache: Option<LayoutCache>,
        worker_endpoint_cache: Option<WorkerEndpointCache>,
    ) -> Self {
        self.layout_cache = layout_cache;
        self.worker_endpoint_cache = worker_endpoint_cache;
        self
    }

    /// Build configured groups from parallel group id and endpoint lists.
    pub fn from_config(metadata_group_ids: &[u64], metadata_endpoints: &[String]) -> ClientResult<Self> {
        let fallback_endpoint = metadata_endpoints
            .first()
            .cloned()
            .ok_or_else(|| ClientError::Config("client.metadata.endpoints must not be empty".to_string()))?;
        let groups = metadata_group_ids
            .iter()
            .enumerate()
            .map(|(index, group_id)| ConfiguredMetadataGroup {
                group_id: *group_id,
                endpoint: metadata_endpoints
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| fallback_endpoint.clone()),
            })
            .collect();
        Self::new(groups)
    }

    /// Choose the owner group for a path, using owner cache before bootstrap config.
    pub fn choose_group_for_path(&self, path: &str) -> ClientResult<u64> {
        let state = self.state.read();
        if let Some(group_id) = state.mount_route_cache.get(path) {
            return Ok(*group_id);
        }
        state
            .configured_groups
            .first()
            .map(|group| group.group_id)
            .ok_or_else(|| ClientError::Config("metadata group configuration is empty".to_string()))
    }

    /// Choose the owner group for an operation.
    pub fn choose_group_for_operation(&self, operation: &OperationContext) -> ClientResult<u64> {
        if let Some(path) = operation.original_target_path() {
            self.choose_group_for_path(path)
        } else {
            self.choose_group_for_path("")
        }
    }

    /// Return cached mount epoch for a path or its best matching mount prefix.
    pub fn cached_mount_epoch(&self, path: &str) -> Option<u64> {
        cached_epoch_for_path(&self.state.read().mount_epoch_cache, path)
    }

    /// Return cached route epoch for a path or its best matching mount prefix.
    pub fn cached_route_epoch(&self, path: &str) -> Option<u64> {
        cached_epoch_for_path(&self.state.read().route_epoch_cache, path)
    }

    /// Select endpoint for the next attempt.
    pub fn endpoint_for_group(&self, group_id: u64) -> ClientResult<String> {
        let state = self.state.read();
        if let Some(endpoint) = state.leader_cache.get(&group_id) {
            return Ok(endpoint.clone());
        }
        state
            .configured_groups
            .iter()
            .find(|group| group.group_id == group_id)
            .or_else(|| state.configured_groups.first())
            .map(|group| group.endpoint.clone())
            .ok_or_else(|| ClientError::Config("metadata endpoint configuration is empty".to_string()))
    }

    /// Record a structured refresh decision and update correctness caches.
    pub fn record_refresh(
        &self,
        operation: &OperationContext,
        reason: RefreshReason,
        hint: &RefreshHint,
    ) -> ClientResult<()> {
        let mut state = self.state.write();
        match reason {
            RefreshReason::NotLeader => {
                if let (Some(group_id), Some(endpoint)) = (hint.group_id, hint.leader_endpoint.as_ref()) {
                    state.leader_cache.insert(group_id, endpoint.clone());
                }
            }
            RefreshReason::OwnerGroupMismatch => {
                let Some(group_id) = hint.group_id else {
                    return Err(ClientError::Metadata(
                        "owner group mismatch refresh missing group_id hint".to_string(),
                    ));
                };
                if let Some(path) = operation.original_target_path() {
                    state.mount_route_cache.insert(path.to_string(), group_id);
                }
                if let Some(endpoint) = hint.leader_endpoint.as_ref() {
                    state.leader_cache.insert(group_id, endpoint.clone());
                }
            }
            RefreshReason::MountEpochMismatch => {
                if let Some(mount_epoch) = hint.mount_epoch {
                    record_epoch_hint(
                        &mut state.mount_epoch_cache,
                        operation.original_target_path(),
                        hint.mount_prefix.as_deref(),
                        mount_epoch,
                    );
                }
            }
            RefreshReason::RouteEpochMismatch => {
                if let Some(route_epoch) = hint.route_epoch {
                    record_epoch_hint(
                        &mut state.route_epoch_cache,
                        operation.original_target_path(),
                        hint.mount_prefix.as_deref(),
                        route_epoch,
                    );
                }
            }
            RefreshReason::StaleState
            | RefreshReason::WorkerEpochMismatch
            | RefreshReason::BlockStampMismatch
            | RefreshReason::Unknown => {}
        }
        drop(state);
        self.invalidate_caches_after_refresh(reason);
        Ok(())
    }

    /// Add cached freshness hints to an attempt context without inventing defaults.
    pub fn enrich_attempt_context(&self, operation: &OperationContext, mut ctx: AttemptContext) -> AttemptContext {
        let Some(path) = operation.original_target_path() else {
            return ctx;
        };
        if let Some(mount_epoch) = self.cached_mount_epoch(path) {
            ctx = ctx.with_mount_epoch(mount_epoch);
        }
        if let Some(route_epoch) = self.cached_route_epoch(path) {
            ctx = ctx.with_route_epoch(route_epoch);
        }
        ctx
    }

    /// Record an msync state watermark.
    pub fn record_state_watermark(&self, watermark: proto::common::GroupStateWatermarkProto) -> ClientResult<()> {
        let watermark = GroupStateWatermark::try_from(watermark)
            .map_err(|err| ClientError::Metadata(format!("invalid state watermark: {err}")))?;
        self.watermarks.update_if_ahead(watermark);
        Ok(())
    }

    /// Return cached watermark as proto for a group.
    pub fn state_watermark_proto(&self, group_id: u64) -> Option<proto::common::GroupStateWatermarkProto> {
        self.watermarks
            .get(&ShardGroupId::new(group_id))
            .map(|watermark| (&watermark).into())
    }
}

impl RefreshManager {
    fn invalidate_caches_after_refresh(&self, reason: RefreshReason) {
        match reason {
            RefreshReason::RouteEpochMismatch => {
                self.invalidate_layout(CacheInvalidationReason::RouteEpoch);
                self.invalidate_worker_endpoints(CacheInvalidationReason::RouteEpoch);
            }
            RefreshReason::BlockStampMismatch => {
                self.invalidate_layout(CacheInvalidationReason::BlockStamp);
            }
            RefreshReason::WorkerEpochMismatch => {
                self.invalidate_layout(CacheInvalidationReason::WorkerEpoch);
                self.invalidate_worker_endpoints(CacheInvalidationReason::WorkerEpoch);
            }
            RefreshReason::OwnerGroupMismatch | RefreshReason::MountEpochMismatch => {
                self.invalidate_layout(CacheInvalidationReason::Owner);
                self.invalidate_worker_endpoints(CacheInvalidationReason::Owner);
            }
            RefreshReason::StaleState | RefreshReason::Unknown => {
                self.invalidate_layout(CacheInvalidationReason::MetadataRefresh);
            }
            RefreshReason::NotLeader => {}
        }
    }

    fn invalidate_layout(&self, reason: CacheInvalidationReason) {
        if let Some(cache) = &self.layout_cache {
            cache.invalidate_all(reason);
        }
    }

    fn invalidate_worker_endpoints(&self, reason: CacheInvalidationReason) {
        if let Some(cache) = &self.worker_endpoint_cache {
            cache.invalidate_all(reason);
        }
    }
}

fn record_epoch_hint(
    cache: &mut HashMap<String, u64>,
    operation_path: Option<&str>,
    mount_prefix: Option<&str>,
    epoch: u64,
) {
    if let Some(path) = operation_path {
        cache.insert(path.to_string(), epoch);
    }
    if let Some(prefix) = mount_prefix {
        cache.insert(prefix.to_string(), epoch);
    }
}

fn cached_epoch_for_path(cache: &HashMap<String, u64>, path: &str) -> Option<u64> {
    cache.get(path).copied().or_else(|| {
        cache
            .iter()
            .filter(|(prefix, _)| path_matches_prefix(path, prefix))
            .max_by_key(|(prefix, _)| prefix.len())
            .map(|(_, epoch)| *epoch)
    })
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    if prefix == "/" {
        return path.starts_with('/');
    }
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|remaining| remaining.starts_with('/'))
}

impl Default for RefreshManager {
    fn default() -> Self {
        Self::new(vec![ConfiguredMetadataGroup {
            group_id: 1,
            endpoint: "http://127.0.0.1:18080".to_string(),
        }])
        .expect("default metadata group must be non-zero")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{LayoutCache, LayoutCacheKey, WorkerEndpointCache};
    use crate::canonical::RefreshHint;
    use crate::metrics::NoopClientMetrics;
    use crate::planner::read_planner::PlannedReadRange;
    use crate::runtime::classify::RefreshReason;
    use crate::runtime::policy::OperationKind;
    use crate::runtime::{OperationContext, OperationIdentity};
    use proto::common::{
        BlockIdProto, GroupStateWatermarkProto, RaftLogIdProto, ShardGroupIdProto, WorkerEndpointInfoProto,
        WorkerNetProtocolProto,
    };
    use std::sync::Arc;
    use std::time::Duration;
    use types::{ClientId, DataHandleId, InodeId};

    fn manager() -> RefreshManager {
        RefreshManager::new(vec![ConfiguredMetadataGroup {
            group_id: 9,
            endpoint: "http://127.0.0.1:18080".to_string(),
        }])
        .expect("refresh manager")
    }

    fn path_operation() -> OperationContext {
        OperationContext::new(
            ClientId::new(7),
            OperationKind::MetadataRead,
            "OpenFile",
            OperationIdentity::path("/alpha/file"),
        )
        .expect("operation context")
    }

    fn metadata_attempt(operation: &OperationContext) -> AttemptContext {
        AttemptContext::for_metadata(operation, 9, 0).expect("metadata attempt")
    }

    #[test]
    fn path_operation_initially_uses_configured_default_group() {
        let manager = manager();

        assert_eq!(manager.choose_group_for_path("/alpha").expect("group"), 9);
    }

    #[test]
    fn owner_group_mismatch_updates_mount_route_cache() {
        let manager = manager();
        let op = path_operation();

        manager
            .record_refresh(
                &op,
                RefreshReason::OwnerGroupMismatch,
                &RefreshHint {
                    group_id: Some(11),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        assert_eq!(manager.choose_group_for_path("/alpha/file").expect("group"), 11);
    }

    #[test]
    fn owner_group_mismatch_records_leader_hint_for_owner_group() {
        let manager = manager();
        let op = path_operation();

        manager
            .record_refresh(
                &op,
                RefreshReason::OwnerGroupMismatch,
                &RefreshHint {
                    group_id: Some(11),
                    leader_endpoint: Some("http://127.0.0.1:18082".to_string()),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        assert_eq!(manager.choose_group_for_path("/alpha/file").expect("group"), 11);
        assert_eq!(
            manager.endpoint_for_group(11).expect("owner endpoint"),
            "http://127.0.0.1:18082"
        );
    }

    #[test]
    fn not_leader_hint_updates_leader_cache() {
        let manager = manager();
        let op = path_operation();

        manager
            .record_refresh(
                &op,
                RefreshReason::NotLeader,
                &RefreshHint {
                    group_id: Some(9),
                    leader_endpoint: Some("http://127.0.0.1:18081".to_string()),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        assert_eq!(
            manager.endpoint_for_group(9).expect("leader endpoint"),
            "http://127.0.0.1:18081"
        );
    }

    #[test]
    fn mount_epoch_hint_enriches_later_attempts() {
        let manager = manager();
        let op = path_operation();

        manager
            .record_refresh(
                &op,
                RefreshReason::MountEpochMismatch,
                &RefreshHint {
                    mount_epoch: Some(31),
                    mount_prefix: Some("/alpha".to_string()),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        let enriched = manager.enrich_attempt_context(&op, metadata_attempt(&op));
        let header = enriched.metadata_header().expect("metadata header");

        assert_eq!(header.mount_epoch, Some(31));
    }

    #[test]
    fn route_epoch_hint_enriches_later_attempts() {
        let manager = manager();
        let op = path_operation();

        manager
            .record_refresh(
                &op,
                RefreshReason::RouteEpochMismatch,
                &RefreshHint {
                    route_epoch: Some(23),
                    mount_prefix: Some("/alpha".to_string()),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        let enriched = manager.enrich_attempt_context(&op, metadata_attempt(&op));
        let header = enriched.metadata_header().expect("metadata header");

        assert_eq!(header.route_epoch, Some(23));
    }

    #[test]
    fn stale_state_watermark_keeps_highest_group_scoped_state_id() {
        let manager = manager();

        manager
            .record_state_watermark(watermark_proto(9, 10))
            .expect("watermark");
        manager
            .record_state_watermark(watermark_proto(9, 8))
            .expect("older watermark");
        manager
            .record_state_watermark(watermark_proto(11, 3))
            .expect("other group");

        assert_eq!(
            manager
                .state_watermark_proto(9)
                .and_then(|watermark| watermark.state_id.map(|state_id| state_id.index)),
            Some(10)
        );
        assert_eq!(
            manager
                .state_watermark_proto(11)
                .and_then(|watermark| watermark.state_id.map(|state_id| state_id.index)),
            Some(3)
        );
    }

    #[test]
    fn route_epoch_refresh_invalidates_attached_layout_and_worker_endpoint_caches() {
        let layout_cache = seeded_layout_cache();
        let endpoint_cache = seeded_worker_endpoint_cache();
        let manager = manager().with_caches(Some(layout_cache.clone()), Some(endpoint_cache.clone()));
        let op = path_operation();

        manager
            .record_refresh(
                &op,
                RefreshReason::RouteEpochMismatch,
                &RefreshHint {
                    route_epoch: Some(23),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        assert_eq!(layout_cache.len(), 0);
        assert_eq!(endpoint_cache.len(), 0);
    }

    #[test]
    fn block_stamp_refresh_invalidates_layout_without_dropping_worker_endpoint_cache() {
        let layout_cache = seeded_layout_cache();
        let endpoint_cache = seeded_worker_endpoint_cache();
        let manager = manager().with_caches(Some(layout_cache.clone()), Some(endpoint_cache.clone()));
        let op = path_operation();

        manager
            .record_refresh(&op, RefreshReason::BlockStampMismatch, &RefreshHint::default())
            .expect("refresh recorded");

        assert_eq!(layout_cache.len(), 0);
        assert_eq!(endpoint_cache.len(), 1);
    }

    #[test]
    fn worker_epoch_refresh_invalidates_layout_and_worker_endpoint_cache() {
        let layout_cache = seeded_layout_cache();
        let endpoint_cache = seeded_worker_endpoint_cache();
        let manager = manager().with_caches(Some(layout_cache.clone()), Some(endpoint_cache.clone()));
        let op = path_operation();

        manager
            .record_refresh(&op, RefreshReason::WorkerEpochMismatch, &RefreshHint::default())
            .expect("refresh recorded");

        assert_eq!(layout_cache.len(), 0);
        assert_eq!(endpoint_cache.len(), 0);
    }

    fn watermark_proto(group_id: u64, index: u64) -> GroupStateWatermarkProto {
        GroupStateWatermarkProto {
            group_id: Some(ShardGroupIdProto { value: group_id }),
            state_id: Some(RaftLogIdProto {
                term: 1,
                leader_node_id: 1,
                index,
            }),
        }
    }

    fn seeded_layout_cache() -> LayoutCache {
        let cache = LayoutCache::new(true, Duration::from_secs(60), 8, Arc::new(NoopClientMetrics));
        let span = PlannedReadRange { file_offset: 0, len: 4 };
        let key = LayoutCacheKey::new(InodeId::new(101), DataHandleId::new(202), 3, span);
        cache
            .insert_validated(
                key,
                proto::metadata::GetBlockLocationsResponseProto {
                    header: Some(proto::common::ResponseHeaderProto {
                        client: Some(proto::common::ClientInfoProto {
                            call_id: types::CallId::new().to_string(),
                            client_id: 7,
                            client_name: String::new(),
                        }),
                        group_id: 9,
                        ..proto::common::ResponseHeaderProto::default()
                    }),
                    inode_id: Some(proto::fs::InodeIdProto { value: 101 }),
                    data_handle_id: Some(proto::common::DataHandleIdProto { value: 202 }),
                    file_size: 4,
                    locations: vec![proto::metadata::FileBlockLocationProto {
                        block_id: Some(BlockIdProto {
                            data_handle_id: 202,
                            block_index: 0,
                        }),
                        file_offset: 0,
                        len: 4,
                        workers: vec![worker_endpoint()],
                        worker_epoch: Some(7),
                        block_stamp: Some(11),
                    }],
                    file_version: Some(3),
                },
            )
            .expect("seed layout cache");
        cache
    }

    fn seeded_worker_endpoint_cache() -> WorkerEndpointCache {
        let cache = WorkerEndpointCache::new(true, Duration::from_secs(60), 8, Arc::new(NoopClientMetrics));
        cache
            .get_or_insert_authoritative(&worker_endpoint())
            .expect("seed worker endpoint cache");
        cache
    }

    fn worker_endpoint() -> WorkerEndpointInfoProto {
        WorkerEndpointInfoProto {
            worker_id: 1,
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
            worker_epoch: 7,
        }
    }
}
