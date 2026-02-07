// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Path service error semantics tests.

mod common;

use common::FsTestHarness;
use metadata::service::guard::LeadershipChecker;
use metadata::service::{MetadataFileSystemServiceImpl, MetadataFsServiceImpl};
use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProto;
use proto::metadata::*;
use std::sync::Arc;
use tonic::Request;
use types::fs::FileAttrs;
use types::ids::ShardGroupId;

#[derive(Clone)]
struct NotLeader;

impl LeadershipChecker for NotLeader {
    fn is_leader(&self) -> bool {
        false
    }
}

#[derive(Clone)]
struct AlwaysLeader;

impl LeadershipChecker for AlwaysLeader {
    fn is_leader(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn test_path_service_propagates_need_refresh_from_fs() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (_mount_id, _root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let fs_service = MetadataFsServiceImpl::new(
        fs_harness.state_store.clone() as Arc<dyn metadata::state::StateStore>,
        fs_harness.mount_table.clone(),
    )
    .with_storage(fs_harness.storage.clone())
    .with_leadership_checker(Arc::new(NotLeader));
    let fs_core = fs_service.fs_core();

    let path_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_leadership_checker(Arc::new(NotLeader));

    let attrs = FileAttrs::new();
    let req_header = FsTestHarness::create_test_request_header();
    let mkdir_req = MkdirPathRequestProto {
        header: req_header,
        path: "/mnt/test/dir".to_string(),
        attrs: Some(proto::fs::FileAttrsProto {
            mode: attrs.mode,
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            atime_ms: attrs.atime_ms,
            mtime_ms: attrs.mtime_ms,
            ctime_ms: attrs.ctime_ms,
            nlink: attrs.nlink,
        }),
        create_parents: false,
    };

    let resp = FileSystemServiceProto::mkdir(&path_service, Request::new(mkdir_req))
        .await
        .unwrap()
        .into_inner();

    let header = resp.header.expect("missing response header");
    let error = header.error.expect("expected error in response header");
    assert_eq!(
        error.error_class,
        proto::common::ErrorClassProto::ErrorClassNeedRefresh as i32
    );
    assert_eq!(
        error.refresh_reason,
        proto::common::RefreshReasonProto::RefreshReasonNotLeader as i32
    );
}

#[tokio::test]
async fn test_path_service_resolver_not_found_is_enoent() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (_mount_id, _root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let fs_service = MetadataFsServiceImpl::new(
        fs_harness.state_store.clone() as Arc<dyn metadata::state::StateStore>,
        fs_harness.mount_table.clone(),
    )
    .with_storage(fs_harness.storage.clone());
    let fs_core = fs_service.fs_core();

    let path_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_leadership_checker(Arc::new(AlwaysLeader));

    let req_header = FsTestHarness::create_test_request_header();
    let lookup_req = GetFileStatusRequestProto {
        header: req_header,
        path: "/mnt/test/missing.txt".to_string(),
    };

    let resp = FileSystemServiceProto::get_file_status(&path_service, Request::new(lookup_req))
        .await
        .unwrap()
        .into_inner();

    let header = resp.header.expect("missing response header");
    let error = header.error.expect("expected error in response header");
    assert_eq!(
        error.code,
        Some(proto::common::error_detail_proto::Code::FsErrno(
            proto::common::FsErrnoProto::FsErrnoEnoent as i32
        ))
    );
}

#[tokio::test]
async fn test_fs_service_lookup_not_found_is_grpc_ok_with_header_error() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (_mount_id, root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let req_header = FsTestHarness::create_test_request_header();
    let lookup_req = LookupRequestProto {
        header: req_header,
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        name: "missing.txt".to_string(),
    };

    let resp = MetadataFsServiceProto::lookup(&fs_harness.fs_service, Request::new(lookup_req))
        .await
        .expect("business errors must return grpc OK")
        .into_inner();

    let header = resp.header.expect("missing response header");
    let error = header.error.expect("expected error in response header");
    assert_eq!(
        error.code,
        Some(proto::common::error_detail_proto::Code::FsErrno(
            proto::common::FsErrnoProto::FsErrnoEnoent as i32
        ))
    );
}

#[tokio::test]
async fn test_path_service_mount_epoch_mismatch_is_need_refresh_with_reason_and_hint() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (mount_id, _root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let fs_service = MetadataFsServiceImpl::new(
        fs_harness.state_store.clone() as Arc<dyn metadata::state::StateStore>,
        fs_harness.mount_table.clone(),
    )
    .with_storage(fs_harness.storage.clone());
    let fs_core = fs_service.fs_core();
    let path_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_leadership_checker(Arc::new(AlwaysLeader));

    let mut req_header = FsTestHarness::create_test_request_header();
    if let Some(header) = req_header.as_mut() {
        let mount = fs_harness
            .mount_table
            .get_mount(mount_id)
            .unwrap()
            .expect("mount must exist");
        header.mount_epoch = Some(mount.config_version.saturating_sub(1));
    }

    let attrs = FileAttrs::new();
    let mkdir_req = MkdirPathRequestProto {
        header: req_header,
        path: "/mnt/test/dir".to_string(),
        attrs: Some(proto::fs::FileAttrsProto {
            mode: attrs.mode,
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            atime_ms: attrs.atime_ms,
            mtime_ms: attrs.mtime_ms,
            ctime_ms: attrs.ctime_ms,
            nlink: attrs.nlink,
        }),
        create_parents: false,
    };

    let resp = FileSystemServiceProto::mkdir(&path_service, Request::new(mkdir_req))
        .await
        .expect("business errors must return grpc OK")
        .into_inner();

    let header = resp.header.expect("missing response header");
    let error = header.error.expect("expected error in response header");
    assert_eq!(
        error.error_class,
        proto::common::ErrorClassProto::ErrorClassNeedRefresh as i32
    );
    assert_eq!(
        error.refresh_reason,
        proto::common::RefreshReasonProto::RefreshReasonMountEpochMismatch as i32
    );
    assert!(header.mount_epoch.is_some(), "mount_epoch hint must be present");
}
