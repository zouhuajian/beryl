// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Fsync barrier and worker commit propagation tests.

mod common;
use common::FsTestHarness;
use metadata::inode_lease::WriteMode;
use proto::common::{
    error_detail_proto::Code as ErrorCodeProto, ClientInfoProto, ErrorClassProto, LeaseIdProto, RefreshReasonProto,
    RequestHeaderProto,
};
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProto;
use proto::metadata::{CreateRequestProto, FsyncRequestProto, LookupRequestProto};
use proto::worker::{CommitWriteRequestProto, CommitWriteResponseProto};
use std::sync::{Arc, Mutex};
use tonic::Request;
use types::fs::FileAttrs;
use types::ids::{BlockId, BlockIndex, ClientId, DataHandleId};
use types::layout::FileLayout;
use types::lease::FencingToken;

async fn create_file_and_lookup(harness: &FsTestHarness, mount_root: u64, name: &str) -> u64 {
    let req_header = FsTestHarness::create_test_request_header();
    let mut attrs = FileAttrs::new();
    attrs.mode = 0o644;
    let layout = FileLayout::new(1024, 512, 1);

    let create_req = CreateRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto { value: mount_root }),
        name: name.to_string(),
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
    MetadataFsServiceProto::create(&harness.fs_service, Request::new(create_req))
        .await
        .unwrap();

    let lookup_req = LookupRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto { value: mount_root }),
        name: name.to_string(),
    };
    let lookup_resp = MetadataFsServiceProto::lookup(&harness.fs_service, Request::new(lookup_req))
        .await
        .unwrap()
        .into_inner();
    lookup_resp.inode.unwrap().inode_id.unwrap().value
}

fn make_request_header() -> Option<RequestHeaderProto> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let call_seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    Some(RequestHeaderProto {
        client: Some(ClientInfoProto {
            call_id: format!("call-{}", call_seq),
            client_id: 42,
            client_name: String::new(),
        }),
        group_id: 1,
        mount_epoch: None,
        deadline_ms: 0,
        traceparent: String::new(),
        caller_context: None,
        state_id: None,
        retry_count: 0,
        route_epoch: None,
    })
}

fn make_lease_proto(lease_id: u128) -> Option<LeaseIdProto> {
    Some(LeaseIdProto {
        high: (lease_id >> 64) as u64,
        low: lease_id as u64,
    })
}

#[tokio::test]
async fn fsync_barrier_success_updates_size() {
    let harness = FsTestHarness::new().await.unwrap();
    let (mount_id, root_inode_id) = harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            types::ids::ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let inode_id_raw = create_file_and_lookup(&harness, root_inode_id.as_raw(), "a").await;
    let inode_id = types::fs::InodeId::new(inode_id_raw);

    // Lease + write session setup
    let lease_mgr = harness.fs_service.inode_lease_manager_for_test();
    let client_id = ClientId::new(42);
    let call_id = types::CallId::new();
    let (lease_id, lease_epoch, _) = lease_mgr
        .try_acquire(inode_id, client_id, Some(call_id.clone()), WriteMode::Write, None)
        .unwrap();

    let block_id = BlockId::new(DataHandleId::new(inode_id.as_raw()), BlockIndex::new(0));
    let fencing_token = FencingToken {
        block_id,
        owner: client_id,
        epoch: lease_epoch,
    };

    let last_req: Arc<Mutex<Option<CommitWriteRequestProto>>> = Arc::new(Mutex::new(None));
    {
        let last_req = last_req.clone();
        harness
            .fs_service
            .set_worker_commit_hook_for_test(Arc::new(move |req: CommitWriteRequestProto| {
                last_req.lock().unwrap().replace(req.clone());
                CommitWriteResponseProto {
                    header: Some(proto::worker::DataResponseHeaderProto {
                        client: None,
                        error: None,
                        worker_epoch: None,
                        endpoint_hint: None,
                    }),
                    committed_length: req.committed_length,
                    current_block_stamp: 0,
                }
            }));
    }

    let write_target = proto::metadata::WriteTargetProto {
        block_id: Some(proto::common::BlockIdProto {
            data_handle_id: block_id.data_handle_id.as_raw(),
            block_index: block_id.index.as_raw(),
        }),
        worker_endpoints: vec![proto::common::WorkerEndpointInfoProto {
            worker_id: 1,
            endpoint: "mock-worker".to_string(),
            net_transport_kind: proto::common::NetTransportKindProto::NetTransportKindGrpc as i32,
            worker_epoch: 1,
        }],
        fencing_token: Some(proto::common::FencingTokenProto {
            block_id: Some(proto::common::BlockIdProto {
                data_handle_id: block_id.data_handle_id.as_raw(),
                block_index: block_id.index.as_raw(),
            }),
            owner: client_id.as_raw(),
            epoch: lease_epoch,
        }),
    };

    let session_mgr = harness.fs_service.write_session_manager_for_test();
    let file_handle = session_mgr.create_session(
        inode_id,
        mount_id,
        lease_id,
        lease_epoch,
        fencing_token.clone(),
        1,
        0,
        WriteMode::Write,
        vec![write_target],
        metadata::write_session::WriterIdentity { client_id, call_id },
    );
    session_mgr.set_last_written(file_handle, 8);

    let fsync_req = FsyncRequestProto {
        header: make_request_header(),
        inode_id: Some(proto::fs::InodeIdProto {
            value: inode_id.as_raw(),
        }),
        flags: 0,
        lease_id: make_lease_proto(lease_id.as_raw()),
        lease_epoch: Some(lease_epoch),
        fencing_token: Some(proto::common::FencingTokenProto {
            block_id: Some(proto::common::BlockIdProto {
                data_handle_id: block_id.data_handle_id.as_raw(),
                block_index: block_id.index.as_raw(),
            }),
            owner: client_id.as_raw(),
            epoch: lease_epoch,
        }),
        file_handle: Some(file_handle),
        target_size: Some(1), // smaller than last_written to exercise max rule
        route_epoch: None,
        worker_epoch: None,
    };

    let resp = MetadataFsServiceProto::fsync(&harness.fs_service, Request::new(fsync_req))
        .await
        .unwrap()
        .into_inner();
    if let Some(err) = resp.header.as_ref().and_then(|h| h.error.clone()) {
        panic!("fsync returned error: {:?}", err);
    }

    // Worker saw the effective target size (>= last_written)
    let committed = last_req.lock().unwrap().clone().unwrap().committed_length;
    assert_eq!(committed, 8);

    // Storage size updated
    let inode = harness.storage.get_inode(inode_id).unwrap().unwrap();
    assert!(inode.attrs.size >= 8);

    harness.fs_service.clear_worker_commit_hook_for_test();
}

#[tokio::test]
async fn fsync_barrier_retryable_does_not_persist_size() {
    let harness = FsTestHarness::new().await.unwrap();
    let (mount_id, root_inode_id) = harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            types::ids::ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let inode_id_raw = create_file_and_lookup(&harness, root_inode_id.as_raw(), "b").await;
    let inode_id = types::fs::InodeId::new(inode_id_raw);

    let lease_mgr = harness.fs_service.inode_lease_manager_for_test();
    let client_id = ClientId::new(99);
    let call_id = types::CallId::new();
    let (lease_id, lease_epoch, _) = lease_mgr
        .try_acquire(inode_id, client_id, Some(call_id.clone()), WriteMode::Write, None)
        .unwrap();

    let block_id = BlockId::new(DataHandleId::new(inode_id.as_raw()), BlockIndex::new(0));
    let fencing_token = FencingToken {
        block_id,
        owner: client_id,
        epoch: lease_epoch,
    };

    let last_req: Arc<Mutex<Option<CommitWriteRequestProto>>> = Arc::new(Mutex::new(None));
    {
        let last_req = last_req.clone();
        harness
            .fs_service
            .set_worker_commit_hook_for_test(Arc::new(move |req: CommitWriteRequestProto| {
                last_req.lock().unwrap().replace(req.clone());
                let err = proto::common::ErrorDetailProto {
                    error_class: ErrorClassProto::ErrorClassRetryable as i32,
                    code: Some(ErrorCodeProto::RpcCode(
                        proto::common::RpcErrorCodeProto::RpcErrCodeStaleState as i32,
                    )),
                    refresh_reason: RefreshReasonProto::RefreshReasonUnknown as i32,
                    retry_after_ms: Some(50),
                    message: "timeout".to_string(),
                    refresh_hint: None,
                };
                CommitWriteResponseProto {
                    header: Some(proto::worker::DataResponseHeaderProto {
                        client: None,
                        error: Some(err),
                        worker_epoch: None,
                        endpoint_hint: None,
                    }),
                    committed_length: 0,
                    current_block_stamp: 0,
                }
            }));
    }

    let write_target = proto::metadata::WriteTargetProto {
        block_id: Some(proto::common::BlockIdProto {
            data_handle_id: block_id.data_handle_id.as_raw(),
            block_index: block_id.index.as_raw(),
        }),
        worker_endpoints: vec![proto::common::WorkerEndpointInfoProto {
            worker_id: 1,
            endpoint: "mock-worker".to_string(),
            net_transport_kind: proto::common::NetTransportKindProto::NetTransportKindGrpc as i32,
            worker_epoch: 1,
        }],
        fencing_token: Some(proto::common::FencingTokenProto {
            block_id: Some(proto::common::BlockIdProto {
                data_handle_id: block_id.data_handle_id.as_raw(),
                block_index: block_id.index.as_raw(),
            }),
            owner: client_id.as_raw(),
            epoch: lease_epoch,
        }),
    };

    let session_mgr = harness.fs_service.write_session_manager_for_test();
    let file_handle = session_mgr.create_session(
        inode_id,
        mount_id,
        lease_id,
        lease_epoch,
        fencing_token.clone(),
        1,
        0,
        WriteMode::Write,
        vec![write_target],
        metadata::write_session::WriterIdentity { client_id, call_id },
    );
    session_mgr.set_last_written(file_handle, 4);

    let fsync_req = FsyncRequestProto {
        header: make_request_header(),
        inode_id: Some(proto::fs::InodeIdProto {
            value: inode_id.as_raw(),
        }),
        flags: 0,
        lease_id: make_lease_proto(lease_id.as_raw()),
        lease_epoch: Some(lease_epoch),
        fencing_token: Some(proto::common::FencingTokenProto {
            block_id: Some(proto::common::BlockIdProto {
                data_handle_id: block_id.data_handle_id.as_raw(),
                block_index: block_id.index.as_raw(),
            }),
            owner: client_id.as_raw(),
            epoch: lease_epoch,
        }),
        file_handle: Some(file_handle),
        target_size: Some(2),
        route_epoch: None,
        worker_epoch: None,
    };

    let resp = MetadataFsServiceProto::fsync(&harness.fs_service, Request::new(fsync_req))
        .await
        .unwrap()
        .into_inner();
    let err = resp.header.unwrap().error.unwrap();
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassRetryable as i32);
    assert_eq!(
        err.code,
        Some(ErrorCodeProto::RpcCode(
            proto::common::RpcErrorCodeProto::RpcErrCodeStaleState as i32
        ))
    );

    // Storage size should remain unchanged (still 0)
    let inode = harness.storage.get_inode(inode_id).unwrap().unwrap();
    assert_eq!(inode.attrs.size, 0);

    harness.fs_service.clear_worker_commit_hook_for_test();
}
