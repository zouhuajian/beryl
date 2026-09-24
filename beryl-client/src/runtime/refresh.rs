// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata target selection and refresh cache updates.

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use beryl_types::{GroupName, GroupStateWatermark};
use parking_lot::RwLock;

use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult, RefreshHint};
use crate::metadata::MetadataAuthorityUpdate;

/// Routing and freshness caches protected by one lock so one response update
/// cannot become partially visible to a concurrent request.
#[derive(Debug)]
struct MetadataTargetState {
    leader_endpoint: Option<String>,
    watermark: Option<GroupStateWatermark>,
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
    pub(crate) fn from_config(config: &ClientConfig) -> Self {
        let group_name = GroupName::parse("root").expect("valid built-in root group");
        Self {
            group_name,
            endpoints: config.metadata_endpoints().to_vec(),
            state: RwLock::new(MetadataTargetState {
                leader_endpoint: None,
                watermark: None,
            }),
        }
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
    pub(crate) fn record_refresh(&self, kind: ErrorKind, hint: &RefreshHint) -> ClientResult<()> {
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
    pub(crate) fn apply_authority_update(&self, update: MetadataAuthorityUpdate) {
        let mut state = self.state.write();
        if let Some(watermark) = update.state {
            if state
                .watermark
                .as_ref()
                .is_none_or(|current| watermark.state_id > current.state_id)
            {
                state.watermark = Some(watermark);
            }
        }
    }

    /// Returns the highest observed root-group watermark.
    pub(crate) fn state_watermark_proto(&self) -> Option<beryl_proto::common::GroupStateWatermarkProto> {
        self.state.read().watermark.as_ref().map(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::context::AttemptContext;
    use crate::runtime::{Operation, OperationContext, OperationDeadline};
    use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind};
    use beryl_proto::common::{GroupStateWatermarkProto, RaftLogIdProto};
    use beryl_types::{ClientId, GroupName};

    fn manager() -> MetadataTargets {
        MetadataTargets::from_config(&ClientConfig::builder().build().unwrap())
    }

    fn operation() -> OperationContext {
        OperationContext::new_named(
            ClientId::new(7),
            "test-client",
            Operation::OpenFile,
            OperationDeadline::new(1_000),
        )
    }

    fn metadata_attempt(operation: &OperationContext) -> AttemptContext {
        AttemptContext::for_metadata(operation, group_name("root"), "http://127.0.0.1:18080")
    }

    #[test]
    fn transport_failure_clears_failed_cached_leader() {
        let targets =
            MetadataTargets::from_config(&ClientConfig::builder().metadata_endpoints(["a", "b"]).build().unwrap());

        targets
            .record_refresh(
                ErrorKind::Metadata(MetadataErrorKind::NotLeader),
                &RefreshHint {
                    group_name: Some(group_name("root")),
                    leader_endpoint: Some("leader".to_string()),
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
    fn authority_watermarks_never_regress() {
        let manager = manager();
        let op = operation();

        manager.apply_authority_update(MetadataAuthorityUpdate {
            group_name: group_name("root"),
            state: Some(GroupStateWatermark::try_from(watermark_proto("root", 10)).unwrap()),
        });
        manager.apply_authority_update(MetadataAuthorityUpdate {
            group_name: group_name("root"),
            state: Some(GroupStateWatermark::try_from(watermark_proto("root", 8)).unwrap()),
        });

        let header = metadata_attempt(&op)
            .with_state(manager.state_watermark_proto())
            .metadata_header();

        assert_eq!(header.state.unwrap().state_id.map(|state| state.index), Some(10));
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
