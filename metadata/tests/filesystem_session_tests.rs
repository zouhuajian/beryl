// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! FileSystemService session/write flow integration tests.

mod common;

use ::common::header::RequestHeader as CommonRequestHeader;
use common::FsTestHarness;
use metadata::service::guard::LeadershipChecker;
use metadata::service::{MetadataFileSystemServiceImpl, MetadataFsServiceImpl};
use metadata::state::StateStore;
use metadata::worker::{HealthStatus, WorkerManager};
use proto::common::{
    error_detail_proto::Code as ErrorCodeProto, ClientInfoProto, ErrorClassProto, RefreshReasonProto,
    RequestHeaderProto, RpcErrorCodeProto,
};
use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use proto::metadata::*;
use proto::worker::CommitWriteResponseProto;
use std::sync::Arc;
use tonic::Request;
use types::fs::FileAttrs;
use types::ids::{ShardGroupId, WorkerId};
use types::layout::FileLayout;
use types::ClientId;

#[derive(Clone)]
struct AlwaysLeader;

impl LeadershipChecker for AlwaysLeader {
    fn is_leader(&self) -> bool {
        true
    }
}

struct SessionHarness {
    pub _fs_harness: FsTestHarness,
    pub path_service: MetadataFileSystemServiceImpl,
    pub mount_epoch: u64,
    pub route_epoch: u64,
}

impl SessionHarness {
    async fn new() -> Self {
        let fs_harness = FsTestHarness::new().await.unwrap();
        let (mount_id, _root_inode_id) = fs_harness
            .create_mount_with_root(
                "/mnt/test".to_string(),
                "file:///tmp/test".to_string(),
                ShardGroupId::new(1),
            )
            .await
            .unwrap();

        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(1);
        worker_manager
            .register_worker(
                worker_id,
                "127.0.0.1".to_string(),
                proto::common::NetTransportKindProto::NetTransportKindGrpc as i32,
                7,
                None,
            )
            .unwrap();
        worker_manager
            .update_runtime(
                worker_id,
                1,
                7,
                1024 * 1024,
                0,
                1024 * 1024,
                0,
                0,
                HealthStatus::Healthy,
            )
            .unwrap();

        let metrics = Arc::new(metadata::metrics::MetadataMetrics::new());
        let fs_service = MetadataFsServiceImpl::new(
            fs_harness.state_store.clone() as Arc<dyn metadata::state::StateStore>,
            fs_harness.mount_table.clone(),
        )
        .with_storage(fs_harness.storage.clone())
        .with_raft_node(fs_harness.raft_node.clone())
        .with_leadership_checker(Arc::new(AlwaysLeader))
        .with_worker_manager(worker_manager)
        .with_metrics(metrics.clone());
        fs_service.set_worker_commit_hook_for_test(Arc::new(|_req| CommitWriteResponseProto::default()));
        let fs_core = fs_service.fs_core();

        let path_service =
            MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
                .with_metrics(metrics)
                .with_leadership_checker(Arc::new(AlwaysLeader));

        let mount_entry = fs_harness.mount_table.get_mount(mount_id).unwrap().unwrap();
        let route_epoch = fs_harness.state_store.get_layout_version().await.unwrap().as_u64();

        Self {
            _fs_harness: fs_harness,
            path_service,
            mount_epoch: mount_entry.config_version,
            route_epoch,
        }
    }

    fn header(&self, route_epoch: Option<u64>) -> Option<RequestHeaderProto> {
        let mut header: RequestHeaderProto = (&CommonRequestHeader::new(ClientId::new(100))).into();
        if let Some(client) = header.client.as_mut() {
            client.client_name = "it".to_string();
        } else {
            header.client = Some(ClientInfoProto {
                call_id: String::new(),
                client_id: 100,
                client_name: "it".to_string(),
            });
        }
        header.group_id = 1;
        header.mount_epoch = Some(self.mount_epoch);
        header.route_epoch = route_epoch;
        Some(header)
    }

    async fn create_file(&self, path: &str) {
        let attrs = FileAttrs::new();
        let layout = FileLayout::new(1024, 512, 1);
        let req = CreatePathRequestProto {
            header: self.header(Some(self.route_epoch)),
            path: path.to_string(),
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
                block_size: layout.block_size,
                chunk_size: layout.chunk_size,
                replication: layout.replication as u32,
            }),
        };
        let resp = FileSystemServiceProto::create(&self.path_service, Request::new(req))
            .await
            .unwrap()
            .into_inner();
        assert!(
            resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none(),
            "create_file failed: {:?}",
            resp.header
        );
    }
}

fn rpc_code(resp: &Option<proto::common::ResponseHeaderProto>) -> Option<i32> {
    match resp.as_ref()?.error.as_ref()?.code.as_ref()? {
        ErrorCodeProto::RpcCode(code) => Some(*code),
        ErrorCodeProto::FsErrno(_) => None,
    }
}

#[tokio::test]
async fn write_session_happy_path() {
    let harness = SessionHarness::new().await;
    let path = "/mnt/test/happy.bin";
    harness.create_file(path).await;

    let open_req = OpenWriteByPathRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        path: path.to_string(),
        desired_len: Some(1024),
        mode: WriteModeProto::WriteModeWrite as i32,
    };
    let open_resp = FileSystemServiceProto::open_write_by_path(&harness.path_service, Request::new(open_req))
        .await
        .unwrap()
        .into_inner();
    assert!(open_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());

    let renew_req = RenewWriteSessionLeaseRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        file_handle: open_resp.file_handle,
        lease_id: open_resp.lease_id,
        lease_epoch: open_resp.lease_epoch,
    };
    let renew_resp = FileSystemServiceProto::renew_write_session_lease(&harness.path_service, Request::new(renew_req))
        .await
        .unwrap()
        .into_inner();
    assert!(renew_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());

    let close_req = CloseWriteSessionRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        file_handle: open_resp.file_handle,
        lease_id: open_resp.lease_id,
        fencing_token: open_resp.fencing_token,
        extents: vec![],
        final_size: 0,
        open_epoch: open_resp.open_epoch,
        lease_epoch: open_resp.lease_epoch,
    };
    let close_resp = FileSystemServiceProto::close_write_session(&harness.path_service, Request::new(close_req))
        .await
        .unwrap()
        .into_inner();
    assert!(close_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());
}

#[tokio::test]
async fn write_session_full_lifecycle_includes_sync_and_release() {
    let harness = SessionHarness::new().await;
    let path = "/mnt/test/full_lifecycle.bin";
    harness.create_file(path).await;

    let open_req = OpenWriteByPathRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        path: path.to_string(),
        desired_len: Some(1024),
        mode: WriteModeProto::WriteModeWrite as i32,
    };
    let open_resp = FileSystemServiceProto::open_write_by_path(&harness.path_service, Request::new(open_req))
        .await
        .unwrap()
        .into_inner();
    assert!(open_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());
    let lease_id = open_resp.lease_id;
    let fencing_token = open_resp.fencing_token;

    let renew_req = RenewWriteSessionLeaseRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        file_handle: open_resp.file_handle,
        lease_id,
        lease_epoch: open_resp.lease_epoch,
    };
    let renew_resp = FileSystemServiceProto::renew_write_session_lease(&harness.path_service, Request::new(renew_req))
        .await
        .unwrap()
        .into_inner();
    assert!(renew_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());

    let fsync_req = FsyncSessionRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        file_handle: open_resp.file_handle,
        flags: 0,
        lease_id,
        lease_epoch: Some(open_resp.lease_epoch),
        fencing_token,
        worker_epoch: None,
        target_size: Some(0),
    };
    let fsync_resp = FileSystemServiceProto::fsync_session(&harness.path_service, Request::new(fsync_req.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(fsync_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());

    let close_req = CloseWriteSessionRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        file_handle: open_resp.file_handle,
        lease_id,
        fencing_token,
        extents: vec![],
        final_size: 0,
        open_epoch: open_resp.open_epoch,
        lease_epoch: open_resp.lease_epoch,
    };
    let close_resp = FileSystemServiceProto::close_write_session(&harness.path_service, Request::new(close_req))
        .await
        .unwrap()
        .into_inner();
    assert!(close_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());

    let release_req = ReleaseSessionRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        file_handle: open_resp.file_handle,
    };
    let release_resp = FileSystemServiceProto::release_session(&harness.path_service, Request::new(release_req))
        .await
        .unwrap()
        .into_inner();
    assert!(release_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());
}

#[tokio::test]
async fn route_epoch_mismatch_closed_loop() {
    let harness = SessionHarness::new().await;
    let path = "/mnt/test/layout.bin";
    harness.create_file(path).await;

    let stale = harness.route_epoch.saturating_add(1);
    let layout_req_stale = GetFileLayoutByPathRequestProto {
        header: harness.header(Some(stale)),
        path: path.to_string(),
        range: None,
    };
    let stale_call =
        FileSystemServiceProto::get_file_layout_by_path(&harness.path_service, Request::new(layout_req_stale)).await;
    assert!(stale_call.is_ok());
    let stale_resp = stale_call.unwrap().into_inner();
    let err = stale_resp.header.as_ref().and_then(|h| h.error.as_ref()).unwrap();
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
    assert_eq!(
        rpc_code(&stale_resp.header),
        Some(RpcErrorCodeProto::RpcErrCodeRouteEpochMismatch as i32)
    );
    assert_eq!(
        err.refresh_reason,
        RefreshReasonProto::RefreshReasonRouteEpochMismatch as i32
    );
    assert!(err.message.contains("refresh route and replay"));
    assert!(
        err.message.contains("server="),
        "expected actionable server route_epoch hint: {}",
        err.message
    );
    assert!(
        err.message.contains("GetFileLayout"),
        "expected actionable replay target in hint: {}",
        err.message
    );

    let layout_req_fresh = GetFileLayoutByPathRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        path: path.to_string(),
        range: None,
    };
    let fresh_resp =
        FileSystemServiceProto::get_file_layout_by_path(&harness.path_service, Request::new(layout_req_fresh))
            .await
            .unwrap()
            .into_inner();
    assert!(fresh_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());
}

#[tokio::test]
async fn fencing_is_refreshable() {
    let harness = SessionHarness::new().await;
    let path = "/mnt/test/fencing.bin";
    harness.create_file(path).await;

    let open_req = OpenWriteByPathRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        path: path.to_string(),
        desired_len: Some(1024),
        mode: WriteModeProto::WriteModeWrite as i32,
    };
    let open_resp = FileSystemServiceProto::open_write_by_path(&harness.path_service, Request::new(open_req))
        .await
        .unwrap()
        .into_inner();

    let mut bad_token = open_resp.fencing_token;
    if let Some(token) = bad_token.as_mut() {
        token.epoch = token.epoch.saturating_add(1);
    }

    let close_req = CloseWriteSessionRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        file_handle: open_resp.file_handle,
        lease_id: open_resp.lease_id,
        fencing_token: bad_token,
        extents: vec![],
        final_size: 0,
        open_epoch: open_resp.open_epoch,
        lease_epoch: open_resp.lease_epoch,
    };
    let close_resp = FileSystemServiceProto::close_write_session(&harness.path_service, Request::new(close_req))
        .await
        .unwrap()
        .into_inner();
    let err = close_resp.header.as_ref().and_then(|h| h.error.as_ref()).unwrap();
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
    assert_eq!(
        rpc_code(&close_resp.header),
        Some(RpcErrorCodeProto::RpcErrCodeFencing as i32)
    );
    assert_eq!(err.refresh_reason, RefreshReasonProto::RefreshReasonFencing as i32);
    assert!(
        err.message.contains("re-open write session"),
        "expected actionable replay hint in fencing response: {}",
        err.message
    );
}

#[tokio::test]
async fn worker_epoch_mismatch_is_refreshable() {
    let harness = SessionHarness::new().await;
    let path = "/mnt/test/worker_epoch.bin";
    harness.create_file(path).await;

    let open_req = OpenWriteByPathRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        path: path.to_string(),
        desired_len: Some(1024),
        mode: WriteModeProto::WriteModeWrite as i32,
    };
    let open_resp = FileSystemServiceProto::open_write_by_path(&harness.path_service, Request::new(open_req))
        .await
        .unwrap()
        .into_inner();
    assert!(open_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_none());

    let fsync_req = FsyncSessionRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        file_handle: open_resp.file_handle,
        flags: 0,
        lease_id: open_resp.lease_id,
        lease_epoch: Some(open_resp.lease_epoch),
        fencing_token: open_resp.fencing_token,
        worker_epoch: Some(999),
        target_size: Some(0),
    };
    let fsync_call = FileSystemServiceProto::fsync_session(&harness.path_service, Request::new(fsync_req)).await;
    assert!(fsync_call.is_ok(), "business mismatch must not use non-OK gRPC status");
    let fsync_resp = fsync_call.unwrap().into_inner();
    let err = fsync_resp.header.as_ref().and_then(|h| h.error.as_ref()).unwrap();
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
    assert_eq!(
        rpc_code(&fsync_resp.header),
        Some(RpcErrorCodeProto::RpcErrCodeWorkerEpochMismatch as i32)
    );
    assert_eq!(
        err.refresh_reason,
        RefreshReasonProto::RefreshReasonWorkerEpochMismatch as i32
    );
    assert!(
        err.message.contains("worker_epoch mismatch"),
        "expected worker epoch actionable hint: {}",
        err.message
    );
}

#[tokio::test]
async fn no_business_status_regression() {
    let harness = SessionHarness::new().await;
    let path = "/mnt/test/status.bin";
    harness.create_file(path).await;

    let open_req = OpenWriteByPathRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        path: path.to_string(),
        desired_len: Some(1024),
        mode: WriteModeProto::WriteModeWrite as i32,
    };
    let open_resp = FileSystemServiceProto::open_write_by_path(&harness.path_service, Request::new(open_req))
        .await
        .unwrap()
        .into_inner();

    let bad_open_epoch = open_resp.open_epoch.saturating_add(1);
    let close_req = CloseWriteSessionRequestProto {
        header: harness.header(Some(harness.route_epoch)),
        file_handle: open_resp.file_handle,
        lease_id: open_resp.lease_id,
        fencing_token: open_resp.fencing_token,
        extents: vec![],
        final_size: 0,
        open_epoch: bad_open_epoch,
        lease_epoch: open_resp.lease_epoch,
    };

    let close_call = FileSystemServiceProto::close_write_session(&harness.path_service, Request::new(close_req)).await;
    assert!(close_call.is_ok(), "business mismatch must not use non-OK gRPC status");
    let close_resp = close_call.unwrap().into_inner();
    let err = close_resp.header.as_ref().and_then(|h| h.error.as_ref()).unwrap();
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
    assert_eq!(
        rpc_code(&close_resp.header),
        Some(RpcErrorCodeProto::RpcErrCodeEpochMismatch as i32)
    );
    assert_eq!(
        err.refresh_reason,
        RefreshReasonProto::RefreshReasonEpochMismatch as i32
    );
}
