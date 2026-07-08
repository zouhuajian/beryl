// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata target selection and refresh cache updates.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::RwLock;
use types::{GroupName, GroupStateWatermark};

use crate::cache::StateIdCache;
use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult};
use crate::rpc_error::RefreshHint;
use crate::runtime::classify::MetadataRefreshCause;
use crate::runtime::context::{AttemptContext, OperationContext};

const METADATA_TARGET_CACHE_LIMIT: usize = 300;

/// Configured metadata group bootstrap target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MetadataGroupTargets {
    /// Stable metadata group name.
    pub(crate) group_name: GroupName,
    /// Metadata endpoints for this group.
    pub(crate) endpoints: Vec<String>,
}

#[derive(Debug)]
struct MetadataTargetState {
    groups: Vec<MetadataGroupTargets>,
    leader_cache: HashMap<GroupName, String>,
    route_cache: HashMap<String, GroupName>,
    route_cache_order: VecDeque<String>,
    mount_epoch_cache: HashMap<String, u64>,
    mount_epoch_cache_order: VecDeque<String>,
    route_epoch_cache: HashMap<String, u64>,
    route_epoch_cache_order: VecDeque<String>,
}

impl MetadataTargetState {
    fn insert_route(&mut self, path: String, group_name: GroupName) {
        let MetadataTargetState {
            route_cache,
            route_cache_order,
            ..
        } = self;
        insert_bounded(route_cache, route_cache_order, path, group_name);
    }

    fn record_mount_epoch_hint(&mut self, operation_path: Option<&str>, mount_prefix: Option<&str>, epoch: u64) {
        let MetadataTargetState {
            mount_epoch_cache,
            mount_epoch_cache_order,
            ..
        } = self;
        record_epoch_hint(
            mount_epoch_cache,
            mount_epoch_cache_order,
            operation_path,
            mount_prefix,
            epoch,
        );
    }

    fn record_route_epoch_hint(&mut self, operation_path: Option<&str>, mount_prefix: Option<&str>, epoch: u64) {
        let MetadataTargetState {
            route_epoch_cache,
            route_epoch_cache_order,
            ..
        } = self;
        record_epoch_hint(
            route_epoch_cache,
            route_epoch_cache_order,
            operation_path,
            mount_prefix,
            epoch,
        );
    }
}

/// Owns metadata target selection and correctness cache updates after refresh signals.
#[derive(Clone, Debug)]
pub(crate) struct MetadataTargets {
    state: Arc<RwLock<MetadataTargetState>>,
    watermarks: StateIdCache,
}

impl MetadataTargets {
    /// Create metadata targets from configured metadata groups.
    pub(crate) fn new(groups: Vec<MetadataGroupTargets>) -> ClientResult<Self> {
        if groups.is_empty() {
            return Err(ClientError::InvalidArgument(
                "MetadataTargets requires at least one metadata group".to_string(),
            ));
        }
        if let Some(group) = groups.iter().find(|group| group.endpoints.is_empty()) {
            return Err(ClientError::InvalidArgument(format!(
                "MetadataTargets group {} requires at least one endpoint",
                group.group_name
            )));
        }
        Ok(Self {
            state: Arc::new(RwLock::new(MetadataTargetState {
                groups,
                leader_cache: HashMap::new(),
                route_cache: HashMap::new(),
                route_cache_order: VecDeque::new(),
                mount_epoch_cache: HashMap::new(),
                mount_epoch_cache_order: VecDeque::new(),
                route_epoch_cache: HashMap::new(),
                route_epoch_cache_order: VecDeque::new(),
            })),
            watermarks: StateIdCache::new(300),
        })
    }

    /// Build metadata targets from client config.
    pub(crate) fn from_config(config: &ClientConfig) -> ClientResult<Self> {
        let groups = config
            .metadata_groups
            .iter()
            .map(|group| MetadataGroupTargets {
                group_name: group.group_name.clone(),
                endpoints: group.endpoints.clone(),
            })
            .collect();
        Self::new(groups)
    }

    /// Choose the owner group for a path, using owner cache before bootstrap config.
    pub(crate) fn group_for_path(&self, path: &str) -> ClientResult<GroupName> {
        let state = self.state.read();
        if let Some(group_name) = state.route_cache.get(path) {
            return Ok(group_name.clone());
        }
        state
            .groups
            .first()
            .map(|group| group.group_name.clone())
            .ok_or_else(|| ClientError::Config("metadata group configuration is empty".to_string()))
    }

    /// Choose the owner group for an operation.
    pub(crate) fn group_for_operation(&self, operation: &OperationContext) -> ClientResult<GroupName> {
        if let Some(path) = operation.original_target_path() {
            self.group_for_path(path)
        } else {
            self.group_for_path("")
        }
    }

    /// Return cached mount epoch for a path or its best matching mount prefix.
    pub(crate) fn cached_mount_epoch(&self, path: &str) -> Option<u64> {
        cached_epoch_for_path(&self.state.read().mount_epoch_cache, path)
    }

    /// Return cached route epoch for a path or its best matching mount prefix.
    pub(crate) fn cached_route_epoch(&self, path: &str) -> Option<u64> {
        cached_epoch_for_path(&self.state.read().route_epoch_cache, path)
    }

    /// Select endpoint for the next attempt.
    pub(crate) fn endpoint_for_group(&self, group_name: &GroupName, attempt: u32) -> ClientResult<String> {
        let state = self.state.read();
        if let Some(endpoint) = state.leader_cache.get(group_name) {
            return Ok(endpoint.clone());
        }
        state
            .groups
            .iter()
            .find(|group| &group.group_name == group_name)
            .map(|group| {
                let index = attempt as usize % group.endpoints.len();
                group.endpoints[index].clone()
            })
            .ok_or_else(|| ClientError::Config(format!("metadata group {} is not configured", group_name)))
    }

    /// Clear a cached leader when transport failed against that exact endpoint.
    pub(crate) fn record_transport_failure(&self, group_name: &GroupName, endpoint: &str) {
        let mut state = self.state.write();
        if state
            .leader_cache
            .get(group_name)
            .is_some_and(|cached| cached == endpoint)
        {
            state.leader_cache.remove(group_name);
        }
    }

    /// Record a structured refresh decision and update correctness caches.
    pub(crate) fn record_refresh(
        &self,
        operation: &OperationContext,
        reason: MetadataRefreshCause,
        hint: &RefreshHint,
    ) -> ClientResult<()> {
        let mut state = self.state.write();
        match reason {
            MetadataRefreshCause::NotLeader => {
                if let (Some(group_name), Some(endpoint)) = (hint.group_name.as_ref(), hint.leader_endpoint.as_ref()) {
                    state.leader_cache.insert(group_name.clone(), endpoint.clone());
                }
            }
            MetadataRefreshCause::OwnerGroupMismatch => {
                let Some(group_name) = hint.group_name.as_ref() else {
                    return Err(ClientError::Metadata(
                        "owner group mismatch refresh missing group_name hint".to_string(),
                    ));
                };
                if let Some(path) = operation.original_target_path() {
                    state.insert_route(path.to_string(), group_name.clone());
                }
                if let Some(endpoint) = hint.leader_endpoint.as_ref() {
                    state.leader_cache.insert(group_name.clone(), endpoint.clone());
                }
            }
            MetadataRefreshCause::MountEpochMismatch => {
                if let Some(mount_epoch) = hint.mount_epoch {
                    state.record_mount_epoch_hint(
                        operation.original_target_path(),
                        hint.mount_prefix.as_deref(),
                        mount_epoch,
                    );
                }
            }
            MetadataRefreshCause::RouteEpochMismatch => {
                if let Some(route_epoch) = hint.route_epoch {
                    state.record_route_epoch_hint(
                        operation.original_target_path(),
                        hint.mount_prefix.as_deref(),
                        route_epoch,
                    );
                }
            }
            MetadataRefreshCause::StaleState
            | MetadataRefreshCause::WorkerRunMismatch
            | MetadataRefreshCause::BlockStampMismatch
            | MetadataRefreshCause::Unknown => {}
        }
        Ok(())
    }

    /// Add cached freshness hints to an attempt context without inventing defaults.
    pub(crate) fn enrich_attempt_context(
        &self,
        operation: &OperationContext,
        mut ctx: AttemptContext,
    ) -> AttemptContext {
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
    pub(crate) fn record_state_watermark(
        &self,
        watermark: proto::common::GroupStateWatermarkProto,
    ) -> ClientResult<()> {
        let watermark = GroupStateWatermark::try_from(watermark)
            .map_err(|err| ClientError::Metadata(format!("invalid state watermark: {err}")))?;
        self.watermarks.update_if_ahead(watermark);
        Ok(())
    }

    /// Return cached watermark as proto for a group.
    pub(crate) fn state_watermark_proto(
        &self,
        group_name: &GroupName,
    ) -> Option<proto::common::GroupStateWatermarkProto> {
        self.watermarks.get(group_name).map(|watermark| (&watermark).into())
    }
}

fn record_epoch_hint(
    cache: &mut HashMap<String, u64>,
    order: &mut VecDeque<String>,
    operation_path: Option<&str>,
    mount_prefix: Option<&str>,
    epoch: u64,
) {
    if let Some(path) = operation_path {
        insert_bounded(cache, order, path.to_string(), epoch);
    }
    if let Some(prefix) = mount_prefix {
        insert_bounded(cache, order, prefix.to_string(), epoch);
    }
}

fn insert_bounded<K, V>(cache: &mut HashMap<K, V>, order: &mut VecDeque<K>, key: K, value: V)
where
    K: Clone + Eq + Hash,
{
    if let Some(existing) = cache.get_mut(&key) {
        *existing = value;
        return;
    }
    while cache.len() >= METADATA_TARGET_CACHE_LIMIT {
        let Some(evicted) = order.pop_front() else {
            break;
        };
        if cache.remove(&evicted).is_some() {
            break;
        }
    }
    cache.insert(key.clone(), value);
    order.push_back(key);
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

impl Default for MetadataTargets {
    fn default() -> Self {
        Self::new(vec![MetadataGroupTargets {
            group_name: GroupName::parse("root").expect("default group name is valid"),
            endpoints: vec!["127.0.0.1:18080".to_string()],
        }])
        .expect("default metadata group must be valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_error::RefreshHint;
    use crate::runtime::classify::MetadataRefreshCause;
    use crate::runtime::policy::OperationKind;
    use crate::runtime::{OperationContext, OperationIdentity};
    use proto::common::{GroupStateWatermarkProto, RaftLogIdProto};
    use types::{ClientId, GroupName};

    fn manager() -> MetadataTargets {
        MetadataTargets::new(vec![MetadataGroupTargets {
            group_name: group_name("root"),
            endpoints: vec!["http://127.0.0.1:18080".to_string()],
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

        assert_eq!(manager.group_for_path("/alpha").expect("group"), group_name("root"));
    }

    #[test]
    fn owner_group_mismatch_updates_mount_route_cache() {
        let manager = manager();
        let op = path_operation();

        manager
            .record_refresh(
                &op,
                MetadataRefreshCause::OwnerGroupMismatch,
                &RefreshHint {
                    group_name: Some(group_name("analytics")),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        assert_eq!(
            manager.group_for_path("/alpha/file").expect("group"),
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
                MetadataRefreshCause::OwnerGroupMismatch,
                &RefreshHint {
                    group_name: Some(group_name("analytics")),
                    leader_endpoint: Some("http://127.0.0.1:18082".to_string()),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        assert_eq!(
            manager.group_for_path("/alpha/file").expect("group"),
            group_name("analytics")
        );
        assert_eq!(
            manager
                .endpoint_for_group(&group_name("analytics"), 0)
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
                MetadataRefreshCause::NotLeader,
                &RefreshHint {
                    group_name: Some(group_name("root")),
                    leader_endpoint: Some("http://127.0.0.1:18081".to_string()),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        assert_eq!(
            manager
                .endpoint_for_group(&group_name("root"), 0)
                .expect("leader endpoint"),
            "http://127.0.0.1:18081"
        );
    }

    #[test]
    fn metadata_targets_rotate_configured_endpoints_without_cached_leader() {
        let targets = MetadataTargets::new(vec![MetadataGroupTargets {
            group_name: group_name("root"),
            endpoints: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        }])
        .expect("metadata targets");

        assert_eq!(targets.endpoint_for_group(&group_name("root"), 0).unwrap(), "a");
        assert_eq!(targets.endpoint_for_group(&group_name("root"), 1).unwrap(), "b");
        assert_eq!(targets.endpoint_for_group(&group_name("root"), 2).unwrap(), "c");
        assert_eq!(targets.endpoint_for_group(&group_name("root"), 3).unwrap(), "a");
    }

    #[test]
    fn transport_failure_clears_failed_cached_leader() {
        let targets = MetadataTargets::new(vec![MetadataGroupTargets {
            group_name: group_name("root"),
            endpoints: vec!["a".to_string(), "b".to_string()],
        }])
        .expect("metadata targets");
        let op = path_operation();

        targets
            .record_refresh(
                &op,
                MetadataRefreshCause::NotLeader,
                &RefreshHint {
                    group_name: Some(group_name("root")),
                    leader_endpoint: Some("leader".to_string()),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");
        assert_eq!(targets.endpoint_for_group(&group_name("root"), 0).unwrap(), "leader");

        targets.record_transport_failure(&group_name("root"), "leader");

        assert_eq!(targets.endpoint_for_group(&group_name("root"), 1).unwrap(), "b");
    }

    #[test]
    fn mount_epoch_hint_enriches_later_attempts() {
        let manager = manager();
        let op = path_operation();

        manager
            .record_refresh(
                &op,
                MetadataRefreshCause::MountEpochMismatch,
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
                MetadataRefreshCause::RouteEpochMismatch,
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
    fn refresh_hint_caches_are_bounded() {
        let manager = manager();

        for index in 0..(METADATA_TARGET_CACHE_LIMIT + 50) {
            let operation = OperationContext::new(
                ClientId::new(7),
                OperationKind::MetadataRead,
                "OpenFile",
                OperationIdentity::path(format!("/tenant/{index}/file")),
            )
            .expect("operation context");
            manager
                .record_refresh(
                    &operation,
                    MetadataRefreshCause::OwnerGroupMismatch,
                    &RefreshHint {
                        group_name: Some(group_name("analytics")),
                        ..RefreshHint::default()
                    },
                )
                .expect("owner refresh");
            manager
                .record_refresh(
                    &operation,
                    MetadataRefreshCause::MountEpochMismatch,
                    &RefreshHint {
                        mount_epoch: Some(index as u64),
                        mount_prefix: Some(format!("/tenant/{index}")),
                        ..RefreshHint::default()
                    },
                )
                .expect("mount refresh");
            manager
                .record_refresh(
                    &operation,
                    MetadataRefreshCause::RouteEpochMismatch,
                    &RefreshHint {
                        route_epoch: Some(index as u64),
                        mount_prefix: Some(format!("/tenant/{index}")),
                        ..RefreshHint::default()
                    },
                )
                .expect("route refresh");
        }

        let state = manager.state.read();
        assert!(state.route_cache.len() <= METADATA_TARGET_CACHE_LIMIT);
        assert!(state.mount_epoch_cache.len() <= METADATA_TARGET_CACHE_LIMIT);
        assert!(state.route_epoch_cache.len() <= METADATA_TARGET_CACHE_LIMIT);
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
