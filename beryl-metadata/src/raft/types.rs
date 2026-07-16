// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Raft type definitions for metadata service.

use crate::raft::storage::SnapshotFile;
use beryl_common::header::HeaderIdentity;
use beryl_types::ids::ClientId;
use beryl_types::CallId;
use openraft::RaftTypeConfig;
use serde::{Deserialize, Serialize};

/// Raft type configuration for metadata service.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub(crate) struct MetadataRaftTypeConfig;

impl RaftTypeConfig for MetadataRaftTypeConfig {
    type D = crate::raft::command::Command;
    type R = crate::raft::response::AppDataResponse;
    type NodeId = u64;
    type Node = MetadataNode;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = SnapshotFile;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}

/// Raft state for metadata service.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AppMetadataRaftState {
    pub last_applied_log_id: Option<openraft::LogId<u64>>,
    pub last_purged_log_id: Option<openraft::LogId<u64>>,
    // Note: HardState and StoredMembership structures may be different in openraft 0.9.21
    // We'll use a simplified structure for now
    pub vote: Option<openraft::Vote<u64>>,
    pub committed: Option<openraft::LogId<u64>>,
    pub membership: openraft::StoredMembership<u64, MetadataNode>,
}

/// Node information for metadata service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct MetadataNode {
    pub node_id: u64,
    pub address: String,
}

/// Deduplication key: (client_id, call_id).
///
/// DedupKey identifies a logical mutation request. It must not include command
/// payload, epochs, paths, inodes, or CommandFingerprint.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct DedupKey {
    pub client_id: ClientId,
    pub call_id: CallId,
}

impl DedupKey {
    pub fn new(client_id: ClientId, call_id: CallId) -> Self {
        Self { client_id, call_id }
    }

    pub(crate) fn from_header_identity(identity: &HeaderIdentity) -> Result<Self, String> {
        if identity.client_id.is_zero() {
            return Err("client_id must be non-zero for dedup".to_string());
        }
        if identity.call_id.is_zero() {
            return Err("call_id must be non-zero for dedup".to_string());
        }
        Ok(Self::new(identity.client_id, identity.call_id))
    }
}

/// Stable fingerprint of a command payload.
///
/// CommandFingerprint validates payload consistency under the same DedupKey. It
/// is deliberately separate from DedupKey so call_id reuse with different
/// payloads is detected instead of treated as a new request.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct CommandFingerprint(pub u64);

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_common::header::HeaderIdentity;
    use beryl_types::GroupName;
    use uuid::Uuid;

    #[test]
    fn dedup_key_from_header_identity_uses_checked_client_call_identity() {
        let identity = HeaderIdentity {
            client_id: ClientId::new(42),
            call_id: CallId::new(),
            group_name: Some(GroupName::parse("root").unwrap()),
        };

        let dedup = DedupKey::from_header_identity(&identity).expect("dedup key");
        assert_eq!(dedup.client_id, identity.client_id);
        assert_eq!(dedup.call_id, identity.call_id);

        let zero_client = HeaderIdentity {
            client_id: ClientId::new(0),
            ..identity.clone()
        };
        assert!(DedupKey::from_header_identity(&zero_client).is_err());

        let zero_call = HeaderIdentity {
            call_id: CallId::from_uuid(Uuid::nil()),
            ..identity
        };
        assert!(DedupKey::from_header_identity(&zero_call).is_err());
    }
}
