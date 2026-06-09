// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Refresh manager entry point.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use types::{GroupName, GroupStateWatermark};

use crate::cache::StateIdCache;
use crate::canonical::RefreshHint;
use crate::error::{ClientError, ClientResult};
use crate::runtime::classify::RefreshReason;
use crate::runtime::context::{AttemptContext, OperationContext};

/// Configured metadata group bootstrap target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfiguredMetadataGroup {
    /// Stable metadata group name.
    pub group_name: GroupName,
    /// Metadata endpoint for this group.
    pub endpoint: String,
}

#[derive(Debug)]
struct RefreshState {
    configured_groups: Vec<ConfiguredMetadataGroup>,
    leader_cache: HashMap<GroupName, String>,
    mount_route_cache: HashMap<String, GroupName>,
    mount_epoch_cache: HashMap<String, u64>,
    route_epoch_cache: HashMap<String, u64>,
}

/// Owns correctness cache updates after structured refresh signals.
#[derive(Clone, Debug)]
pub struct RefreshManager {
    state: Arc<RwLock<RefreshState>>,
    watermarks: StateIdCache,
}

impl RefreshManager {
    /// Create a refresh manager from configured metadata groups.
    pub fn new(configured_groups: Vec<ConfiguredMetadataGroup>) -> ClientResult<Self> {
        if configured_groups.is_empty() {
            return Err(ClientError::InvalidArgument(
                "RefreshManager requires at least one metadata group".to_string(),
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
        })
    }

    /// Build configured groups from parallel group name and endpoint lists.
    pub fn from_config(metadata_group_names: &[GroupName], metadata_endpoints: &[String]) -> ClientResult<Self> {
        let fallback_endpoint = metadata_endpoints
            .first()
            .cloned()
            .ok_or_else(|| ClientError::Config("client.metadata.endpoints must not be empty".to_string()))?;
        let groups = metadata_group_names
            .iter()
            .enumerate()
            .map(|(index, group_name)| ConfiguredMetadataGroup {
                group_name: group_name.clone(),
                endpoint: metadata_endpoints
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| fallback_endpoint.clone()),
            })
            .collect();
        Self::new(groups)
    }

    /// Choose the owner group for a path, using owner cache before bootstrap config.
    pub fn choose_group_for_path(&self, path: &str) -> ClientResult<GroupName> {
        let state = self.state.read();
        if let Some(group_name) = state.mount_route_cache.get(path) {
            return Ok(group_name.clone());
        }
        state
            .configured_groups
            .first()
            .map(|group| group.group_name.clone())
            .ok_or_else(|| ClientError::Config("metadata group configuration is empty".to_string()))
    }

    /// Choose the owner group for an operation.
    pub fn choose_group_for_operation(&self, operation: &OperationContext) -> ClientResult<GroupName> {
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
    pub fn endpoint_for_group(&self, group_name: &GroupName) -> ClientResult<String> {
        let state = self.state.read();
        if let Some(endpoint) = state.leader_cache.get(group_name) {
            return Ok(endpoint.clone());
        }
        state
            .configured_groups
            .iter()
            .find(|group| &group.group_name == group_name)
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
                if let (Some(group_name), Some(endpoint)) = (hint.group_name.as_ref(), hint.leader_endpoint.as_ref()) {
                    state.leader_cache.insert(group_name.clone(), endpoint.clone());
                }
            }
            RefreshReason::OwnerGroupMismatch => {
                let Some(group_name) = hint.group_name.as_ref() else {
                    return Err(ClientError::Metadata(
                        "owner group mismatch refresh missing group_name hint".to_string(),
                    ));
                };
                if let Some(path) = operation.original_target_path() {
                    state.mount_route_cache.insert(path.to_string(), group_name.clone());
                }
                if let Some(endpoint) = hint.leader_endpoint.as_ref() {
                    state.leader_cache.insert(group_name.clone(), endpoint.clone());
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
            | RefreshReason::WorkerRunMismatch
            | RefreshReason::BlockStampMismatch
            | RefreshReason::Unknown => {}
        }
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
    pub fn state_watermark_proto(&self, group_name: &GroupName) -> Option<proto::common::GroupStateWatermarkProto> {
        self.watermarks.get(group_name).map(|watermark| (&watermark).into())
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
            group_name: GroupName::parse("root").expect("default group name is valid"),
            endpoint: "http://127.0.0.1:18080".to_string(),
        }])
        .expect("default metadata group must be valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::RefreshHint;
    use crate::runtime::classify::RefreshReason;
    use crate::runtime::policy::OperationKind;
    use crate::runtime::{OperationContext, OperationIdentity};
    use proto::common::{GroupStateWatermarkProto, RaftLogIdProto};
    use types::{ClientId, GroupName};

    fn manager() -> RefreshManager {
        RefreshManager::new(vec![ConfiguredMetadataGroup {
            group_name: group_name("root"),
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
        AttemptContext::for_metadata(operation, group_name("root"), 0).expect("metadata attempt")
    }

    #[test]
    fn path_operation_initially_uses_configured_default_group() {
        let manager = manager();

        assert_eq!(
            manager.choose_group_for_path("/alpha").expect("group"),
            group_name("root")
        );
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
                    group_name: Some(group_name("analytics")),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        assert_eq!(
            manager.choose_group_for_path("/alpha/file").expect("group"),
            group_name("analytics")
        );
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
                    group_name: Some(group_name("analytics")),
                    leader_endpoint: Some("http://127.0.0.1:18082".to_string()),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        assert_eq!(
            manager.choose_group_for_path("/alpha/file").expect("group"),
            group_name("analytics")
        );
        assert_eq!(
            manager
                .endpoint_for_group(&group_name("analytics"))
                .expect("owner endpoint"),
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
                    group_name: Some(group_name("root")),
                    leader_endpoint: Some("http://127.0.0.1:18081".to_string()),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        assert_eq!(
            manager
                .endpoint_for_group(&group_name("root"))
                .expect("leader endpoint"),
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
            .record_state_watermark(watermark_proto("root", 10))
            .expect("watermark");
        manager
            .record_state_watermark(watermark_proto("root", 8))
            .expect("older watermark");
        manager
            .record_state_watermark(watermark_proto("analytics", 3))
            .expect("other group");

        assert_eq!(
            manager
                .state_watermark_proto(&group_name("root"))
                .and_then(|watermark| watermark.state_id.map(|state_id| state_id.index)),
            Some(10)
        );
        assert_eq!(
            manager
                .state_watermark_proto(&group_name("analytics"))
                .and_then(|watermark| watermark.state_id.map(|state_id| state_id.index)),
            Some(3)
        );
    }

    fn watermark_proto(group_name: &str, index: u64) -> GroupStateWatermarkProto {
        GroupStateWatermarkProto {
            group_name: group_name.to_string(),
            state_id: Some(RaftLogIdProto {
                term: 1,
                leader_node_id: 1,
                index,
            }),
        }
    }

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }
}
