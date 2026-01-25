// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Rename NOREPLACE behavior tests.

mod common;
use common::FsTestHarness;
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProto;
use proto::metadata::{FsRenameRequestProto, LookupRequestProto};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Request;
use types::fs::{FileAttrs, Inode, InodeId};
use types::ids::{DataHandleId, MountId};

#[tokio::test]
async fn rename_noreplace_preserves_destination() {
    let harness = FsTestHarness::new().await.unwrap();
    let (_mount_id, root_inode_id) = harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            types::ids::ShardGroupId::new(1),
        )
        .await
        .unwrap();

    // Manually seed directory entries for a and b.
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let mut attrs = FileAttrs::new();
    attrs.mode = 0o644;
    attrs.nlink = 1;
    attrs.update_timestamps(now_ms);
    let inode_a = InodeId::new(2001);
    let inode_b = InodeId::new(2002);
    let file_a = Inode::new_file(inode_a, attrs.clone(), MountId::new(1), DataHandleId::new(2001));
    let file_b = Inode::new_file(inode_b, attrs, MountId::new(1), DataHandleId::new(2002));
    harness.storage.put_inode(&file_a).unwrap();
    harness.storage.put_inode(&file_b).unwrap();
    harness.storage.put_dentry(root_inode_id, "a", inode_a).unwrap();
    harness.storage.put_dentry(root_inode_id, "b", inode_b).unwrap();

    let header = FsTestHarness::create_test_request_header();
    let group_id = 1;
    let mut rename_header = header.clone();
    if let Some(h) = rename_header.as_mut() {
        h.group_id = group_id;
    }

    let rename_req = FsRenameRequestProto {
        header: rename_header,
        src_parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        src_name: "a".to_string(),
        dst_parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        dst_name: "b".to_string(),
        flags: 0x1, // RENAME_NOREPLACE
    };

    let result = MetadataFsServiceProto::rename(&harness.fs_service, Request::new(rename_req)).await;
    match result {
        Ok(resp) => {
            let errno = FsTestHarness::extract_error_code(&resp.into_inner().header);
            assert_eq!(errno, Some(proto::common::FsErrnoProto::FsErrnoEexist as u32));
        }
        Err(status) => {
            assert!(
                status.message().contains("Destination exists"),
                "unexpected status: {:?}",
                status
            );
        }
    }

    // Source and destination should remain intact.
    let mut lookup_header = header.clone();
    if let Some(h) = lookup_header.as_mut() {
        h.group_id = group_id;
    }
    let lookup_a = MetadataFsServiceProto::lookup(
        &harness.fs_service,
        Request::new(LookupRequestProto {
            header: lookup_header.clone(),
            parent_inode_id: Some(proto::fs::InodeIdProto {
                value: root_inode_id.as_raw(),
            }),
            name: "a".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        FsTestHarness::extract_lookup_inode_id(&lookup_a).unwrap().as_raw(),
        inode_a.as_raw()
    );

    let lookup_b = MetadataFsServiceProto::lookup(
        &harness.fs_service,
        Request::new(LookupRequestProto {
            header: lookup_header,
            parent_inode_id: Some(proto::fs::InodeIdProto {
                value: root_inode_id.as_raw(),
            }),
            name: "b".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        FsTestHarness::extract_lookup_inode_id(&lookup_b).unwrap().as_raw(),
        inode_b.as_raw()
    );
}
