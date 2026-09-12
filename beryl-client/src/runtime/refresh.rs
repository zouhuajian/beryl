// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata target selection and refresh cache updates.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use beryl_types::{GroupName, GroupStateWatermark};
use parking_lot::RwLock;

use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult, RefreshHint};
use crate::metadata::MetadataAuthorityUpdate;
use crate::runtime::context::{AttemptContext, OperationContext};

const METADATA_TARGET_CACHE_LIMIT: usize = 300;

/// Routing and freshness caches protected by one lock so one response update
/// cannot become partially visible to a concurrent request.
#[derive(Debug)]
struct MetadataTargetState {
    leader_endpoint: Option<String>,
    mount_epoch_cache: HashMap<String, u64>,
    mount_epoch_cache_order: VecDeque<String>,
    route_epoch_cache: HashMap<String, u64>,
    route_epoch_cache_order: VecDeque<String>,
    watermark: Option<GroupStateWatermark>,
}

impl MetadataTargetState {
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

/// Owns metadata target selection and monotonic correctness cache updates from
/// successful response authority and structured refresh signals.
#[derive(Debug)]
pub(crate) struct MetadataTargets {
    group_name: GroupName,
    endpoints: Vec<String>,
    state: RwLock<MetadataTargetState>,
}

impl MetadataTargets {
    /// Builds the single supported root route from sealed client configuration.
    pub(crate) fn from_config(config: &ClientConfig) -> ClientResult<Self> {
        let group_name = GroupName::parse("root")
            .map_err(|error| ClientError::invalid_configuration(format!("invalid built-in root group: {error}")))?;
        Ok(Self {
            group_name,
            endpoints: config.metadata_endpoints().to_vec(),
            state: RwLock::new(MetadataTargetState {
                leader_endpoint: None,
                mount_epoch_cache: HashMap::new(),
                mount_epoch_cache_order: VecDeque::new(),
                route_epoch_cache: HashMap::new(),
                route_epoch_cache_order: VecDeque::new(),
                watermark: None,
            }),
        })
    }

    pub(crate) fn group_name(&self) -> &GroupName {
        &self.group_name
    }

    fn validate_group(&self, group_name: &GroupName) -> ClientResult<()> {
        if group_name != &self.group_name {
            return Err(ClientError::invalid_configuration(format!(
                "metadata group {group_name} is not configured"
            )));
        }
        Ok(())
    }

    /// Return cached mount epoch for a path or its best matching mount prefix.
    pub(crate) fn cached_mount_epoch(&self, path: &str) -> Option<u64> {
        cached_epoch_for_path(&self.state.read().mount_epoch_cache, path)
    }

    /// Return cached route epoch for a path or its best matching mount prefix.
    pub(crate) fn cached_route_epoch(&self, path: &str) -> Option<u64> {
        cached_epoch_for_path(&self.state.read().route_epoch_cache, path)
    }

    /// Selects the cached leader or a configured bootstrap endpoint.
    pub(crate) fn endpoint(&self, attempt: u32) -> String {
        self.state
            .read()
            .leader_endpoint
            .clone()
            .unwrap_or_else(|| self.endpoints[attempt as usize % self.endpoints.len()].clone())
    }

    /// Clears only the leader endpoint whose transport failed.
    pub(crate) fn record_transport_failure(&self, endpoint: &str) {
        let mut state = self.state.write();
        if state.leader_endpoint.as_deref() == Some(endpoint) {
            state.leader_endpoint = None;
        }
    }

    /// Record a structured refresh decision and update correctness caches.
    pub(crate) fn record_refresh(
        &self,
        operation: &OperationContext,
        kind: ErrorKind,
        hint: &RefreshHint,
    ) -> ClientResult<()> {
        let mut state = self.state.write();
        match kind {
            ErrorKind::Metadata(MetadataErrorKind::NotLeader) => {
                if let (Some(group_name), Some(endpoint)) = (hint.group_name.as_ref(), hint.leader_endpoint.as_ref()) {
                    self.validate_group(group_name)?;
                    state.leader_endpoint = Some(endpoint.clone());
                }
            }
            ErrorKind::Metadata(MetadataErrorKind::OwnerGroupMismatch | MetadataErrorKind::GroupMismatch) => {
                let Some(group_name) = hint.group_name.as_ref() else {
                    return Err(ClientError::metadata(
                        "owner group mismatch refresh missing group_name hint".to_string(),
                    ));
                };
                self.validate_group(group_name)?;
                if let Some(endpoint) = hint.leader_endpoint.as_ref() {
                    state.leader_endpoint = Some(endpoint.clone());
                }
            }
            ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch) => {
                if let Some(mount_epoch) = hint.mount_epoch {
                    state.record_mount_epoch_hint(
                        operation.original_target_path(),
                        hint.mount_prefix.as_deref(),
                        mount_epoch,
                    );
                }
            }
            ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch) => {
                if let Some(route_epoch) = hint.route_epoch {
                    state.record_route_epoch_hint(
                        operation.original_target_path(),
                        hint.mount_prefix.as_deref(),
                        route_epoch,
                    );
                }
            }
            ErrorKind::Metadata(MetadataErrorKind::StaleState) | ErrorKind::Worker(WorkerErrorKind::RunMismatch) => {}
            _ => {
                return Err(ClientError::metadata(format!(
                    "unsupported metadata refresh error kind: {kind:?}"
                )))
            }
        }
        Ok(())
    }

    /// Atomically applies one validated successful response without allowing
    /// concurrent or late responses to move any authority value backwards.
    pub(crate) fn apply_authority_update(
        &self,
        operation: &OperationContext,
        update: MetadataAuthorityUpdate,
    ) -> ClientResult<()> {
        self.validate_group(&update.group_name)?;
        if update
            .state
            .iter()
            .any(|watermark| watermark.group_name != update.group_name)
        {
            return Err(ClientError::metadata(
                "metadata authority update contains a watermark for another group".to_string(),
            ));
        }
        let operation_path = operation.original_target_path();
        if operation_path.is_none() && (update.mount_epoch.is_some() || update.route_epoch.is_some()) {
            return Err(ClientError::metadata(
                "metadata authority epochs require an operation path".to_string(),
            ));
        }

        let mut state = self.state.write();
        for watermark in update.state {
            if state
                .watermark
                .as_ref()
                .is_none_or(|current| watermark.state_id > current.state_id)
            {
                state.watermark = Some(watermark);
            }
        }
        if let Some(mount_epoch) = update.mount_epoch {
            state.record_mount_epoch_hint(operation_path, None, mount_epoch);
        }
        if let Some(route_epoch) = update.route_epoch {
            state.record_route_epoch_hint(operation_path, None, route_epoch);
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

    /// Returns the highest observed root-group watermark.
    pub(crate) fn state_watermark_proto(&self) -> Option<beryl_proto::common::GroupStateWatermarkProto> {
        self.state.read().watermark.as_ref().map(Into::into)
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
        insert_bounded_epoch(cache, order, path.to_string(), epoch);
    }
    if let Some(prefix) = mount_prefix {
        insert_bounded_epoch(cache, order, prefix.to_string(), epoch);
    }
}

/// Inserts one path-scoped epoch while preserving the highest observed value.
fn insert_bounded_epoch(cache: &mut HashMap<String, u64>, order: &mut VecDeque<String>, key: String, epoch: u64) {
    if let Some(existing) = cache.get_mut(&key) {
        *existing = (*existing).max(epoch);
        return;
    }
    insert_bounded(cache, order, key, epoch);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{Operation, OperationContext, OperationDeadline};
    use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind};
    use beryl_proto::common::{GroupStateWatermarkProto, RaftLogIdProto};
    use beryl_types::{ClientId, GroupName};

    fn manager() -> MetadataTargets {
        MetadataTargets::from_config(&ClientConfig::builder().build().unwrap()).unwrap()
    }

    fn path_operation() -> OperationContext {
        OperationContext::new_named(
            ClientId::new(7),
            "test-client",
            Operation::OpenFile,
            Some("/alpha/file".to_string()),
            OperationDeadline::new(1_000),
        )
        .expect("operation context")
    }

    fn metadata_attempt(operation: &OperationContext) -> AttemptContext {
        AttemptContext::for_metadata(operation, group_name("root")).expect("metadata attempt")
    }

    #[test]
    fn transport_failure_clears_failed_cached_leader() {
        let targets =
            MetadataTargets::from_config(&ClientConfig::builder().metadata_endpoints(["a", "b"]).build().unwrap())
                .unwrap();
        let op = path_operation();

        targets
            .record_refresh(
                &op,
                ErrorKind::Metadata(MetadataErrorKind::NotLeader),
                &RefreshHint {
                    group_name: Some(group_name("root")),
                    leader_endpoint: Some("leader".to_string()),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");
        assert_eq!(targets.endpoint(0), "leader");

        targets.record_transport_failure("a");
        assert_eq!(targets.endpoint(0), "leader");
        targets.record_transport_failure("leader");

        assert_eq!(targets.endpoint(1), "b");
    }

    #[test]
    fn authority_updates_and_refresh_hints_never_regress() {
        let manager = manager();
        let op = path_operation();

        manager
            .apply_authority_update(
                &op,
                MetadataAuthorityUpdate {
                    group_name: group_name("root"),
                    state: vec![GroupStateWatermark::try_from(watermark_proto("root", 10)).unwrap()],
                    mount_epoch: Some(31),
                    route_epoch: Some(41),
                },
            )
            .expect("new authority update");
        for (kind, hint) in [
            (
                MetadataErrorKind::MountEpochMismatch,
                RefreshHint {
                    mount_epoch: Some(30),
                    mount_prefix: Some("/alpha".to_string()),
                    ..RefreshHint::default()
                },
            ),
            (
                MetadataErrorKind::RouteEpochMismatch,
                RefreshHint {
                    route_epoch: Some(40),
                    mount_prefix: Some("/alpha".to_string()),
                    ..RefreshHint::default()
                },
            ),
        ] {
            manager
                .record_refresh(&op, ErrorKind::Metadata(kind), &hint)
                .expect("older refresh hint");
        }
        manager
            .apply_authority_update(
                &op,
                MetadataAuthorityUpdate {
                    group_name: group_name("root"),
                    state: vec![GroupStateWatermark::try_from(watermark_proto("root", 8)).unwrap()],
                    mount_epoch: Some(29),
                    route_epoch: Some(39),
                },
            )
            .expect("older authority update");

        let header = manager
            .enrich_attempt_context(&op, metadata_attempt(&op))
            .with_state(manager.state_watermark_proto().into_iter().collect())
            .metadata_header()
            .expect("metadata header");
        assert_eq!(header.mount_epoch, Some(31));
        assert_eq!(header.route_epoch, Some(41));
        assert_eq!(header.state[0].state_id.as_ref().map(|state| state.index), Some(10));
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
