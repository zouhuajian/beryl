// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Service-layer error contract coverage.

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RecoveryAction, RefreshHint, RpcErrorDetail};
use beryl_metadata::service::header_from_rpc_error;
use beryl_proto::convert::rpc_error_from_proto;
use beryl_types::GroupName;

mod rpc_header_invariant_tests {
    use super::*;

    #[test]
    fn refresh_metadata_header_carries_kind_recovery_and_hint() {
        let err = RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch),
            RefreshHint {
                group_name: Some("root".to_string()),
                mount_epoch: Some(7),
                ..Default::default()
            },
            "mount epoch mismatch",
        );
        let header = header_from_rpc_error(&None, Some(GroupName::parse("root").unwrap()), Some(7), &err);
        let detail = header.error.expect("refresh failure must carry header.error");
        let rpc_error = rpc_error_from_proto(&detail);

        assert_eq!(
            rpc_error.kind,
            ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch)
        );
        match rpc_error.recovery {
            RecoveryAction::RefreshMetadata { hint } => {
                assert_eq!(hint.group_name.as_deref(), Some("root"));
                assert_eq!(hint.mount_epoch, Some(7));
            }
            other => panic!("expected RefreshMetadata recovery, got {other:?}"),
        }
    }
}
