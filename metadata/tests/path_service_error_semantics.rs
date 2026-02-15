// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Path service error semantics tests.

mod common;

use ::common::error::canonical::CanonicalError;
use ::common::header::RequestHeader;
use async_trait::async_trait;
use common::FsTestHarness;
use metadata::service::domain::RequestContext;
use metadata::service::guard::LeadershipChecker;
use metadata::service::{
    AclInodeAuthz, AuthzOp, AuthzProvider, AuthzScheme, AuthzTarget, DenyAllAuthz, MetadataFileSystemServiceImpl,
    MetadataFsServiceImpl, RocksDbInodePermReader, StaticGroupResolver,
};
use metadata::state::StateStore;
use proto::common::{error_detail_proto::Code as ErrorCodeProto, ErrorClassProto, FsErrnoProto, RpcErrorCodeProto};
use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProto;
use proto::metadata::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::Request;
use types::fs::{FileAttrs, Inode, InodeId};
use types::ids::{DataHandleId, ShardGroupId};
use types::ClientId;

#[derive(Clone)]
struct NotLeader;

impl LeadershipChecker for NotLeader {
    fn is_leader(&self) -> bool {
        false
    }

    fn leader_endpoint(&self) -> Option<String> {
        Some("127.0.0.1:17000".to_string())
    }
}

#[derive(Clone)]
struct AlwaysLeader;

impl LeadershipChecker for AlwaysLeader {
    fn is_leader(&self) -> bool {
        true
    }
}

#[derive(Clone, Default)]
struct AuditAuthz {
    calls: Arc<Mutex<Vec<(AuthzOp, AuthzTarget)>>>,
}

impl AuditAuthz {
    async fn take_calls(&self) -> Vec<(AuthzOp, AuthzTarget)> {
        let mut calls = self.calls.lock().await;
        std::mem::take(&mut *calls)
    }
}

fn header_with_principal(principal: &str) -> Option<proto::common::RequestHeaderProto> {
    let mut header = RequestHeader::new(ClientId::new(1));
    header.principal = Some(principal.to_string());
    Some((&header).into())
}

#[async_trait]
impl AuthzProvider for AuditAuthz {
    fn scheme(&self) -> AuthzScheme {
        AuthzScheme::RangerPath
    }

    async fn authorize(
        &self,
        _req_ctx: &RequestContext,
        target: AuthzTarget,
        op: AuthzOp,
    ) -> Result<(), CanonicalError> {
        self.calls.lock().await.push((op, target));
        Ok(())
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
    assert_eq!(
        error
            .refresh_hint
            .as_ref()
            .and_then(|hint| hint.leader_endpoint.as_deref()),
        Some("127.0.0.1:17000")
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
async fn get_file_status_success_header_includes_route_and_mount_epoch() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (mount_id, _root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();
    let mount = fs_harness.mount_table.get_mount(mount_id).unwrap().unwrap();
    let route_epoch = fs_harness.state_store.get_layout_version().await.unwrap().as_u64();

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

    let resp = FileSystemServiceProto::get_file_status(
        &path_service,
        Request::new(GetFileStatusRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    let header = resp.header.expect("missing response header");
    assert!(header.error.is_none());
    assert_eq!(header.mount_epoch, Some(mount.config_version));
    assert_eq!(header.route_epoch, Some(route_epoch));
}

#[tokio::test]
async fn list_status_success_header_includes_route_and_mount_epoch() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (mount_id, _root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();
    let mount = fs_harness.mount_table.get_mount(mount_id).unwrap().unwrap();
    let route_epoch = fs_harness.state_store.get_layout_version().await.unwrap().as_u64();

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

    let resp = FileSystemServiceProto::list_status(
        &path_service,
        Request::new(ListStatusPathRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test".to_string(),
            recursive: false,
            cursor: vec![],
            limit: 16,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    let header = resp.header.expect("missing response header");
    assert!(header.error.is_none());
    assert_eq!(header.mount_epoch, Some(mount.config_version));
    assert_eq!(header.route_epoch, Some(route_epoch));
}

#[tokio::test]
async fn open_success_header_includes_route_and_mount_epoch() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (mount_id, _root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();
    let mount = fs_harness.mount_table.get_mount(mount_id).unwrap().unwrap();
    let route_epoch = fs_harness.state_store.get_layout_version().await.unwrap().as_u64();

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

    let resp = FileSystemServiceProto::open(
        &path_service,
        Request::new(OpenPathRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test".to_string(),
            flags: 0,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    let header = resp.header.expect("missing response header");
    assert!(header.error.is_none());
    assert_eq!(header.mount_epoch, Some(mount.config_version));
    assert_eq!(header.route_epoch, Some(route_epoch));
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
    .with_storage(fs_harness.storage.clone())
    .with_raft_node(fs_harness.raft_node.clone());
    let fs_core = fs_service.fs_core();
    let path_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_leadership_checker(Arc::new(AlwaysLeader));

    let mount = fs_harness
        .mount_table
        .get_mount(mount_id)
        .unwrap()
        .expect("mount must exist");

    let attrs = FileAttrs::new();
    let mut create_header = FsTestHarness::create_test_request_header();
    if let Some(header) = create_header.as_mut() {
        header.mount_epoch = Some(mount.config_version);
    }
    let create_req = CreatePathRequestProto {
        header: create_header,
        path: "/mnt/test/open_write_mount_epoch.bin".to_string(),
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
        layout: Some(proto::common::FileLayoutProto {
            block_size: 1024,
            chunk_size: 512,
            replication: 1,
        }),
    };
    let create_resp = FileSystemServiceProto::create(&path_service, Request::new(create_req))
        .await
        .expect("create path should succeed")
        .into_inner();
    let create_header = create_resp.header.expect("missing create response header");
    assert!(
        create_header.error.is_none(),
        "create precondition failed: {:?}",
        create_header.error
    );

    let mut req_header = FsTestHarness::create_test_request_header();
    if let Some(header) = req_header.as_mut() {
        header.mount_epoch = Some(mount.config_version.saturating_sub(1));
    }
    let open_req = OpenWriteByPathRequestProto {
        header: req_header,
        path: "/mnt/test/open_write_mount_epoch.bin".to_string(),
        desired_len: Some(1024),
        mode: WriteModeProto::WriteModeWrite as i32,
    };

    let resp = FileSystemServiceProto::open_write_by_path(&path_service, Request::new(open_req))
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
    assert_eq!(
        error.refresh_hint.as_ref().and_then(|hint| hint.mount_epoch),
        Some(mount.config_version)
    );
}

#[tokio::test]
async fn test_path_service_deny_all_blocks_metadata_read_with_header_error() {
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
    .with_authz_provider(Arc::new(DenyAllAuthz))
    .with_leadership_checker(Arc::new(AlwaysLeader));
    let fs_core = fs_service.fs_core();

    let path_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_authz_provider(Arc::new(DenyAllAuthz))
            .with_leadership_checker(Arc::new(AlwaysLeader));

    let req_header = FsTestHarness::create_test_request_header();
    let lookup_req = GetFileStatusRequestProto {
        header: req_header,
        path: "/mnt/test".to_string(),
    };
    let resp = FileSystemServiceProto::get_file_status(&path_service, Request::new(lookup_req))
        .await
        .expect("authz business errors must return gRPC OK")
        .into_inner();

    let header = resp.header.expect("missing response header");
    let error = header.error.expect("expected permission denied in header.error");
    assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
    assert_ne!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
    assert!(
        error.message.contains("op=READ"),
        "expected deny message to include op=READ, got: {}",
        error.message
    );
    match error.code {
        Some(ErrorCodeProto::FsErrno(errno))
            if errno == FsErrnoProto::FsErrnoEacces as i32 || errno == FsErrnoProto::FsErrnoEperm as i32 => {}
        other => panic!("expected EACCES/EPERM fs errno, got {:?}", other),
    }
}

#[tokio::test]
async fn test_path_service_acl_mode_denies_when_principal_missing() {
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

    let acl_provider = Arc::new(AclInodeAuthz::new(
        Arc::new(StaticGroupResolver::new(BTreeMap::new())),
        Arc::new(RocksDbInodePermReader::new(fs_harness.storage.clone(), 2)),
    ));
    let path_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_authz_provider(acl_provider)
            .with_leadership_checker(Arc::new(AlwaysLeader));

    let resp = FileSystemServiceProto::get_file_status(
        &path_service,
        Request::new(GetFileStatusRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test".to_string(),
        }),
    )
    .await
    .expect("authz business errors must remain grpc OK")
    .into_inner();

    let header = resp.header.expect("missing response header");
    let error = header.error.expect("expected acl deny in response header");
    assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
    match error.code {
        Some(ErrorCodeProto::RpcCode(code)) if code == RpcErrorCodeProto::RpcErrCodeUnauthenticated as i32 => {}
        other => panic!("expected unauthenticated rpc code, got {:?}", other),
    }
}

#[tokio::test]
async fn test_fs_service_acl_mode_denies_when_principal_missing_with_grpc_ok() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (_mount_id, root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let perm_reader: Arc<dyn metadata::service::InodePermReader> =
        Arc::new(RocksDbInodePermReader::new(fs_harness.storage.clone(), 300));
    let acl_provider = Arc::new(AclInodeAuthz::new(
        Arc::new(StaticGroupResolver::new(BTreeMap::new())),
        Arc::clone(&perm_reader),
    ));
    let fs_service = MetadataFsServiceImpl::new(
        fs_harness.state_store.clone() as Arc<dyn metadata::state::StateStore>,
        fs_harness.mount_table.clone(),
    )
    .with_storage(fs_harness.storage.clone())
    .with_raft_node(fs_harness.raft_node.clone())
    .with_authz_provider(acl_provider)
    .with_inode_perm_reader(perm_reader)
    .with_leadership_checker(Arc::new(AlwaysLeader));

    let resp = MetadataFsServiceProto::get_attr(
        &fs_service,
        Request::new(GetAttrRequestProto {
            header: FsTestHarness::create_test_request_header(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: root_inode_id.as_raw(),
            }),
        }),
    )
    .await
    .expect("authz business errors must remain grpc OK")
    .into_inner();

    let header = resp.header.expect("missing response header");
    let error = header.error.expect("expected acl deny in response header");
    match error.code {
        Some(ErrorCodeProto::RpcCode(code)) if code == RpcErrorCodeProto::RpcErrCodeUnauthenticated as i32 => {}
        other => panic!("expected unauthenticated rpc code, got {:?}", other),
    }
}

#[tokio::test]
async fn test_acl_cache_invalidation_after_set_attr_is_immediate() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (mount_id, _root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let inode_id = InodeId::new(8080);
    let mut attrs = FileAttrs::new();
    attrs.uid = 1000;
    attrs.gid = 1000;
    attrs.mode = 0o604; // others can read
    let inode = Inode::new_file(inode_id, attrs.clone(), mount_id, DataHandleId::new(8080));
    fs_harness.storage.put_inode(&inode).unwrap();

    let perm_reader: Arc<dyn metadata::service::InodePermReader> =
        Arc::new(RocksDbInodePermReader::new(fs_harness.storage.clone(), 300));
    let acl_provider = Arc::new(AclInodeAuthz::new(
        Arc::new(StaticGroupResolver::new(BTreeMap::new())),
        Arc::clone(&perm_reader),
    ));
    let fs_service = MetadataFsServiceImpl::new(
        fs_harness.state_store.clone() as Arc<dyn metadata::state::StateStore>,
        fs_harness.mount_table.clone(),
    )
    .with_storage(fs_harness.storage.clone())
    .with_raft_node(fs_harness.raft_node.clone())
    .with_authz_provider(acl_provider)
    .with_inode_perm_reader(perm_reader)
    .with_leadership_checker(Arc::new(AlwaysLeader));

    let first_read = MetadataFsServiceProto::get_attr(
        &fs_service,
        Request::new(GetAttrRequestProto {
            header: header_with_principal("2000"),
            inode_id: Some(proto::fs::InodeIdProto {
                value: inode_id.as_raw(),
            }),
        }),
    )
    .await
    .expect("first read must be grpc OK")
    .into_inner();
    assert!(
        first_read.header.as_ref().and_then(|h| h.error.as_ref()).is_none(),
        "first read should be allowed"
    );

    let mut chmod_attrs = attrs;
    chmod_attrs.mode = 0o600; // remove others read
    let chmod = MetadataFsServiceProto::set_attr(
        &fs_service,
        Request::new(SetAttrRequestProto {
            header: header_with_principal("1000"),
            inode_id: Some(proto::fs::InodeIdProto {
                value: inode_id.as_raw(),
            }),
            mask: 2, // mode
            attrs: Some(proto::fs::FileAttrsProto {
                mode: chmod_attrs.mode,
                uid: chmod_attrs.uid,
                gid: chmod_attrs.gid,
                size: chmod_attrs.size,
                atime_ms: chmod_attrs.atime_ms,
                mtime_ms: chmod_attrs.mtime_ms,
                ctime_ms: chmod_attrs.ctime_ms,
                nlink: chmod_attrs.nlink,
            }),
        }),
    )
    .await
    .expect("chmod/setattr must be grpc OK")
    .into_inner();
    assert!(
        chmod.header.as_ref().and_then(|h| h.error.as_ref()).is_none(),
        "set_attr should succeed for owner"
    );

    let second_read = MetadataFsServiceProto::get_attr(
        &fs_service,
        Request::new(GetAttrRequestProto {
            header: header_with_principal("2000"),
            inode_id: Some(proto::fs::InodeIdProto {
                value: inode_id.as_raw(),
            }),
        }),
    )
    .await
    .expect("second read must be grpc OK")
    .into_inner();
    let error = second_read
        .header
        .as_ref()
        .and_then(|h| h.error.as_ref())
        .expect("second read must be denied immediately after set_attr");
    match error.code {
        Some(ErrorCodeProto::FsErrno(errno))
            if errno == FsErrnoProto::FsErrnoEacces as i32 || errno == FsErrnoProto::FsErrnoEperm as i32 => {}
        other => panic!("expected EACCES/EPERM, got {:?}", other),
    }
}

#[tokio::test]
async fn test_path_service_deny_all_blocks_open_write_by_path_with_header_error() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (_mount_id, _root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let allow_fs_service = MetadataFsServiceImpl::new(
        fs_harness.state_store.clone() as Arc<dyn metadata::state::StateStore>,
        fs_harness.mount_table.clone(),
    )
    .with_storage(fs_harness.storage.clone())
    .with_raft_node(fs_harness.raft_node.clone())
    .with_leadership_checker(Arc::new(AlwaysLeader));
    let allow_fs_core = allow_fs_service.fs_core();
    let allow_path_service = MetadataFileSystemServiceImpl::new(
        fs_harness.mount_table.clone(),
        fs_harness.storage.clone(),
        allow_fs_core,
    )
    .with_leadership_checker(Arc::new(AlwaysLeader));

    let attrs = FileAttrs::new();
    let create_req = CreatePathRequestProto {
        header: FsTestHarness::create_test_request_header(),
        path: "/mnt/test/deny.bin".to_string(),
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
        layout: Some(proto::common::FileLayoutProto {
            block_size: 4096,
            chunk_size: 1024,
            replication: 1,
        }),
    };
    let create_resp = FileSystemServiceProto::create(&allow_path_service, Request::new(create_req))
        .await
        .unwrap()
        .into_inner();
    assert!(
        create_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none(),
        "setup create failed: {:?}",
        create_resp.header
    );

    let fs_service = MetadataFsServiceImpl::new(
        fs_harness.state_store.clone() as Arc<dyn metadata::state::StateStore>,
        fs_harness.mount_table.clone(),
    )
    .with_storage(fs_harness.storage.clone())
    .with_raft_node(fs_harness.raft_node.clone())
    .with_authz_provider(Arc::new(DenyAllAuthz))
    .with_leadership_checker(Arc::new(AlwaysLeader));
    let fs_core = fs_service.fs_core();
    let path_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_authz_provider(Arc::new(DenyAllAuthz))
            .with_leadership_checker(Arc::new(AlwaysLeader));

    let open_req = OpenWriteByPathRequestProto {
        header: FsTestHarness::create_test_request_header(),
        path: "/mnt/test/deny.bin".to_string(),
        desired_len: Some(0),
        mode: WriteModeProto::WriteModeWrite as i32,
    };
    let resp = FileSystemServiceProto::open_write_by_path(&path_service, Request::new(open_req))
        .await
        .expect("authz business errors must return gRPC OK")
        .into_inner();

    let header = resp.header.expect("missing response header");
    let error = header.error.expect("expected permission denied in header.error");
    assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
    assert_ne!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
    match error.code {
        Some(ErrorCodeProto::FsErrno(errno))
            if errno == FsErrnoProto::FsErrnoEacces as i32 || errno == FsErrnoProto::FsErrnoEperm as i32 => {}
        other => panic!("expected EACCES/EPERM fs errno, got {:?}", other),
    }
}

#[tokio::test]
async fn test_path_service_path_authz_plumbing_calls_read_write_and_rename_targets() {
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

    let audit = Arc::new(AuditAuthz::default());
    let path_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_authz_provider(audit.clone())
            .with_leadership_checker(Arc::new(AlwaysLeader));

    let read_resp = FileSystemServiceProto::get_file_status(
        &path_service,
        Request::new(GetFileStatusRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(read_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());

    let attrs = FileAttrs::new();
    let mkdir_resp = FileSystemServiceProto::mkdir(
        &path_service,
        Request::new(MkdirPathRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test/authz-dir".to_string(),
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

    let rename_resp = FileSystemServiceProto::rename(
        &path_service,
        Request::new(RenamePathRequestProto {
            header: FsTestHarness::create_test_request_header(),
            src_path: "/mnt/test/authz-dir".to_string(),
            dst_path: "/mnt/test/authz-dir-renamed".to_string(),
            flags: 0,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(rename_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());

    let calls = audit.take_calls().await;
    assert_eq!(
        calls.len(),
        4,
        "path service authz checks must be emitted exactly once per target via GuardChain"
    );
    assert!(calls.contains(&(AuthzOp::Read, AuthzTarget::for_path("/mnt/test".to_string()))));
    assert!(calls.contains(&(AuthzOp::Write, AuthzTarget::for_path_parent("/mnt/test", "authz-dir"))));
    assert!(calls.contains(&(
        AuthzOp::Rename,
        AuthzTarget::for_path("/mnt/test/authz-dir".to_string())
    )));
    assert!(calls.contains(&(
        AuthzOp::Rename,
        AuthzTarget::for_path_parent("/mnt/test", "authz-dir-renamed")
    )));
}

#[tokio::test]
async fn test_path_service_rename_path_authz_checks_src_then_dst_parent() {
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

    let audit = Arc::new(AuditAuthz::default());
    let path_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_authz_provider(audit.clone())
            .with_leadership_checker(Arc::new(AlwaysLeader));

    let attrs = FileAttrs::new();
    let mkdir_resp = FileSystemServiceProto::mkdir(
        &path_service,
        Request::new(MkdirPathRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test/rename-src".to_string(),
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
    let _ = audit.take_calls().await;

    let rename_resp = FileSystemServiceProto::rename(
        &path_service,
        Request::new(RenamePathRequestProto {
            header: FsTestHarness::create_test_request_header(),
            src_path: "/mnt/test/rename-src".to_string(),
            dst_path: "/mnt/test/rename-dst".to_string(),
            flags: 0,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(rename_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());

    let calls = audit.take_calls().await;
    let rename_calls: Vec<AuthzTarget> = calls
        .into_iter()
        .filter_map(|(op, target)| if op == AuthzOp::Rename { Some(target) } else { None })
        .collect();
    let expected_path_checks = [
        AuthzTarget::for_path("/mnt/test/rename-src".to_string()),
        AuthzTarget::for_path_parent("/mnt/test", "rename-dst"),
    ];
    assert!(
        rename_calls.len() >= expected_path_checks.len(),
        "expected at least two rename authz calls for path checks, got {:?}",
        rename_calls
    );
    assert_eq!(&rename_calls[..expected_path_checks.len()], &expected_path_checks);

    let verify_resp = FileSystemServiceProto::get_file_status(
        &path_service,
        Request::new(GetFileStatusRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test/rename-dst".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(verify_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());
}

#[tokio::test]
async fn test_path_service_acl_traverse_denies_without_intermediate_execute() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (mount_id, root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let dir_a = InodeId::new(9001);
    let mut dir_a_attrs = FileAttrs::new();
    dir_a_attrs.uid = 1000;
    dir_a_attrs.gid = 1000;
    dir_a_attrs.mode = 0o755;
    fs_harness
        .storage
        .put_inode(&Inode::new_dir(dir_a, dir_a_attrs, mount_id))
        .unwrap();
    fs_harness.storage.put_dentry(root_inode_id, "a", dir_a).unwrap();

    let dir_b = InodeId::new(9002);
    let mut dir_b_attrs = FileAttrs::new();
    dir_b_attrs.uid = 1000;
    dir_b_attrs.gid = 1000;
    dir_b_attrs.mode = 0o744; // readable, but no execute for non-owner
    fs_harness
        .storage
        .put_inode(&Inode::new_dir(dir_b, dir_b_attrs, mount_id))
        .unwrap();
    fs_harness.storage.put_dentry(dir_a, "b", dir_b).unwrap();

    let file_inode = InodeId::new(9003);
    let mut file_attrs = FileAttrs::new();
    file_attrs.uid = 1000;
    file_attrs.gid = 1000;
    file_attrs.mode = 0o644; // target itself is readable
    fs_harness
        .storage
        .put_inode(&Inode::new_file(
            file_inode,
            file_attrs,
            mount_id,
            DataHandleId::new(9003),
        ))
        .unwrap();
    fs_harness.storage.put_dentry(dir_b, "file", file_inode).unwrap();

    let fs_service = MetadataFsServiceImpl::new(
        fs_harness.state_store.clone() as Arc<dyn StateStore>,
        fs_harness.mount_table.clone(),
    )
    .with_storage(fs_harness.storage.clone())
    .with_raft_node(fs_harness.raft_node.clone())
    .with_leadership_checker(Arc::new(AlwaysLeader));
    let fs_core = fs_service.fs_core();

    let acl_provider = Arc::new(AclInodeAuthz::new(
        Arc::new(StaticGroupResolver::new(BTreeMap::new())),
        Arc::new(RocksDbInodePermReader::new(fs_harness.storage.clone(), 300)),
    ));
    let path_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_authz_provider(acl_provider)
            .with_leadership_checker(Arc::new(AlwaysLeader));

    let resp = FileSystemServiceProto::get_file_status(
        &path_service,
        Request::new(GetFileStatusRequestProto {
            header: header_with_principal("2000"),
            path: "/mnt/test/a/b/file".to_string(),
        }),
    )
    .await
    .expect("business/authz errors must remain grpc OK")
    .into_inner();

    let header = resp.header.expect("missing response header");
    let error = header.error.expect("expected traverse deny");
    match error.code {
        Some(ErrorCodeProto::FsErrno(errno))
            if errno == FsErrnoProto::FsErrnoEacces as i32 || errno == FsErrnoProto::FsErrnoEperm as i32 => {}
        other => panic!("expected EACCES/EPERM for traverse deny, got {:?}", other),
    }
    assert!(
        error.message.contains("op=EXECUTE"),
        "traverse deny should be execute-based, got: {}",
        error.message
    );
}

#[tokio::test]
async fn test_path_service_acl_sticky_bit_denies_unlink_for_non_owner() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (mount_id, root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let sticky_dir = InodeId::new(9101);
    let mut sticky_attrs = FileAttrs::new();
    sticky_attrs.uid = 1001;
    sticky_attrs.gid = 1001;
    sticky_attrs.mode = 0o1777;
    fs_harness
        .storage
        .put_inode(&Inode::new_dir(sticky_dir, sticky_attrs, mount_id))
        .unwrap();
    fs_harness
        .storage
        .put_dentry(root_inode_id, "sticky", sticky_dir)
        .unwrap();

    let victim_inode = InodeId::new(9102);
    let mut victim_attrs = FileAttrs::new();
    victim_attrs.uid = 1002;
    victim_attrs.gid = 1002;
    victim_attrs.mode = 0o644;
    fs_harness
        .storage
        .put_inode(&Inode::new_file(
            victim_inode,
            victim_attrs,
            mount_id,
            DataHandleId::new(9102),
        ))
        .unwrap();
    fs_harness
        .storage
        .put_dentry(sticky_dir, "victim", victim_inode)
        .unwrap();

    let fs_service = MetadataFsServiceImpl::new(
        fs_harness.state_store.clone() as Arc<dyn StateStore>,
        fs_harness.mount_table.clone(),
    )
    .with_storage(fs_harness.storage.clone())
    .with_raft_node(fs_harness.raft_node.clone())
    .with_leadership_checker(Arc::new(AlwaysLeader));
    let fs_core = fs_service.fs_core();

    let acl_provider = Arc::new(AclInodeAuthz::new(
        Arc::new(StaticGroupResolver::new(BTreeMap::new())),
        Arc::new(RocksDbInodePermReader::new(fs_harness.storage.clone(), 300)),
    ));
    let path_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_authz_provider(acl_provider)
            .with_leadership_checker(Arc::new(AlwaysLeader));

    let resp = FileSystemServiceProto::unlink(
        &path_service,
        Request::new(UnlinkPathRequestProto {
            header: header_with_principal("2000"),
            path: "/mnt/test/sticky/victim".to_string(),
        }),
    )
    .await
    .expect("business/authz errors must remain grpc OK")
    .into_inner();

    let header = resp.header.expect("missing response header");
    let error = header.error.expect("expected sticky deny in header");
    match error.code {
        Some(ErrorCodeProto::RpcCode(code)) if code == RpcErrorCodeProto::RpcErrCodePermissionDenied as i32 => {}
        other => panic!("expected PermissionDenied rpc code, got {:?}", other),
    }
    assert!(error.message.contains("STICKY_BIT_DENIED"));
}

#[tokio::test]
async fn test_fs_service_acl_sticky_bit_denies_rename_for_non_owner() {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (mount_id, root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let sticky_dir = InodeId::new(9201);
    let mut sticky_attrs = FileAttrs::new();
    sticky_attrs.uid = 1001;
    sticky_attrs.gid = 1001;
    sticky_attrs.mode = 0o1777;
    fs_harness
        .storage
        .put_inode(&Inode::new_dir(sticky_dir, sticky_attrs, mount_id))
        .unwrap();
    fs_harness
        .storage
        .put_dentry(root_inode_id, "sticky", sticky_dir)
        .unwrap();

    let src_inode = InodeId::new(9202);
    let mut src_attrs = FileAttrs::new();
    src_attrs.uid = 1002;
    src_attrs.gid = 1002;
    src_attrs.mode = 0o644;
    fs_harness
        .storage
        .put_inode(&Inode::new_file(
            src_inode,
            src_attrs,
            mount_id,
            DataHandleId::new(9202),
        ))
        .unwrap();
    fs_harness.storage.put_dentry(sticky_dir, "src", src_inode).unwrap();

    let perm_reader: Arc<dyn metadata::service::InodePermReader> =
        Arc::new(RocksDbInodePermReader::new(fs_harness.storage.clone(), 300));
    let acl_provider = Arc::new(AclInodeAuthz::new(
        Arc::new(StaticGroupResolver::new(BTreeMap::new())),
        Arc::clone(&perm_reader),
    ));
    let fs_service = MetadataFsServiceImpl::new(
        fs_harness.state_store.clone() as Arc<dyn StateStore>,
        fs_harness.mount_table.clone(),
    )
    .with_storage(fs_harness.storage.clone())
    .with_raft_node(fs_harness.raft_node.clone())
    .with_authz_provider(acl_provider)
    .with_inode_perm_reader(perm_reader)
    .with_leadership_checker(Arc::new(AlwaysLeader));

    let resp = MetadataFsServiceProto::rename(
        &fs_service,
        Request::new(FsRenameRequestProto {
            header: header_with_principal("2000"),
            src_parent_inode_id: Some(proto::fs::InodeIdProto {
                value: sticky_dir.as_raw(),
            }),
            src_name: "src".to_string(),
            dst_parent_inode_id: Some(proto::fs::InodeIdProto {
                value: sticky_dir.as_raw(),
            }),
            dst_name: "dst".to_string(),
            flags: 0,
        }),
    )
    .await
    .expect("business/authz errors must remain grpc OK")
    .into_inner();

    let header = resp.header.expect("missing response header");
    let error = header.error.expect("expected sticky deny in header");
    match error.code {
        Some(ErrorCodeProto::RpcCode(code)) if code == RpcErrorCodeProto::RpcErrCodePermissionDenied as i32 => {}
        other => panic!("expected PermissionDenied rpc code, got {:?}", other),
    }
    assert!(error.message.contains("STICKY_BIT_DENIED"));
}
