// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

mod common;

use common::FsTestHarness;
use metadata::service::guard::LeadershipChecker;
use metadata::service::{MetadataFileSystemServiceImpl, MetadataFsServiceImpl};
use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use proto::metadata::{
    GetXattrPathRequestProto, ListXattrPathRequestProto, MkdirPathRequestProto, SetXattrPathRequestProto,
};
use std::sync::Arc;
use tonic::Request;
use types::fs::FileAttrs;
use types::ids::ShardGroupId;

const POSIX_ACL_ACCESS_XATTR: &str = "system.posix_acl_access";

#[derive(Clone)]
struct AlwaysLeader;

impl LeadershipChecker for AlwaysLeader {
    fn is_leader(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn acl_xattr_roundtrip_works_under_default_none_authz() {
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
    .with_raft_node(fs_harness.raft_node.clone())
    .with_leadership_checker(Arc::new(AlwaysLeader));
    let fs_core = fs_service.fs_core();

    let path_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_leadership_checker(Arc::new(AlwaysLeader));

    let attrs = FileAttrs::new();
    let mkdir_resp = FileSystemServiceProto::mkdir(
        &path_service,
        Request::new(MkdirPathRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test/acl-dir".to_string(),
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
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(mkdir_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());

    // Keep this test independent from optional ACL codec symbols in `types`.
    // For stub/no-authz semantics we only need xattr roundtrip behavior.
    let acl_blob = vec![0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC, 0xDD];

    let set_resp = FileSystemServiceProto::set_xattr(
        &path_service,
        Request::new(SetXattrPathRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test/acl-dir".to_string(),
            name: POSIX_ACL_ACCESS_XATTR.to_string(),
            value: acl_blob.clone(),
            create: false,
            replace: false,
        }),
    )
    .await
    .expect("setxattr business errors must remain grpc OK")
    .into_inner();
    assert!(
        set_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none(),
        "acl xattr under NONE authz must not be denied or need_refresh"
    );

    let get_resp = FileSystemServiceProto::get_xattr(
        &path_service,
        Request::new(GetXattrPathRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test/acl-dir".to_string(),
            name: POSIX_ACL_ACCESS_XATTR.to_string(),
        }),
    )
    .await
    .expect("getxattr business errors must remain grpc OK")
    .into_inner();
    assert!(get_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());
    assert_eq!(get_resp.value, acl_blob);

    let list_resp = FileSystemServiceProto::list_xattr(
        &path_service,
        Request::new(ListXattrPathRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test/acl-dir".to_string(),
        }),
    )
    .await
    .expect("listxattr business errors must remain grpc OK")
    .into_inner();
    assert!(list_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());
    assert!(list_resp.names.contains(&POSIX_ACL_ACCESS_XATTR.to_string()));
}
