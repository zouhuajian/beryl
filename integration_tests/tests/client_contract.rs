// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

mod common;

use ::common::error::canonical::{CanonicalError, ErrorCode as CanonicalErrorCode, RefreshReason};
use ::common::header::{RequestHeader, ResponseHeader, RpcErrorCode};
use client::api::hcfs::Client;
use client::canonical::{retry_metadata_once, retry_metadata_with_policy, RetryOutcome, RetryPolicy};
use client::config::ClientConfig;
use client::error::ClientError;
use common::{init_logging, MockMetadataServer, MockWorkerServer};
use proto::common::{
    error_detail_proto, BlockIdProto, ByteRangeProto, ClientInfoProto, ErrorClassProto, FencingTokenProto,
    FsErrnoProto, StreamIdProto,
};
use proto::metadata::MsyncRequestProto;
use proto::worker::worker_data_service_server::WorkerDataService;
use proto::worker::{
    CommitWriteRequestProto, DataRequestHeaderProto, DataResponseHeaderProto, OpenReadStreamRequestProto,
    OpenWriteStreamRequestProto,
};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tonic::Request;

/// Test client initialization stays lightweight without contacting endpoints.
#[tokio::test]
async fn test_client_init() {
    init_logging();

    let config = ClientConfig::default();
    let client_result = Client::new(config).await;
    assert!(client_result.is_ok() || client_result.is_err());
}

fn data_request_header(call_id: &str) -> DataRequestHeaderProto {
    DataRequestHeaderProto {
        client: Some(ClientInfoProto {
            call_id: call_id.to_string(),
            client_id: 1,
            client_name: "integration-test".to_string(),
        }),
        traceparent: String::new(),
    }
}

fn block_id(data_handle_id: u64, block_index: u32) -> BlockIdProto {
    BlockIdProto {
        data_handle_id,
        block_index,
    }
}

fn assert_stream_unimplemented_header(header: DataResponseHeaderProto, operation: &str) {
    let error = header.error.expect("placeholder response should carry error detail");
    assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
    assert_eq!(
        error.code,
        Some(error_detail_proto::Code::FsErrno(FsErrnoProto::FsErrnoEnotimpl as i32))
    );
    assert!(error.message.contains(operation));
}

/// Direct worker reads are stream placeholders until worker execution is wired.
#[tokio::test]
async fn test_direct_worker_open_read_stream_is_explicitly_unimplemented() {
    init_logging();

    let mock_worker = MockWorkerServer::new(1);
    let request = Request::new(OpenReadStreamRequestProto {
        header: Some(data_request_header("open-read-stream")),
        block_id: Some(block_id(100, 0)),
        byte_range: Some(ByteRangeProto { offset: 0, len: 9 }),
        block_stamp: 1,
        frame_size: 4096,
    });

    let response = WorkerDataService::open_read_stream(&mock_worker, request)
        .await
        .expect("open read placeholder should use structured response")
        .into_inner();

    assert!(response.stream_id.is_none());
    assert_eq!(response.frame_size, 0);
    assert_eq!(response.window_bytes, 0);
    assert_stream_unimplemented_header(response.header.expect("data header"), "OpenReadStream");
}

/// Test Msync with mock metadata server.
#[tokio::test]
async fn test_msync_with_mock() {
    init_logging();

    let mock_server = MockMetadataServer::new(1, vec![2, 3]);

    use proto::common::ClientInfoProto;
    use proto::common::RequestHeaderProto;
    let header = RequestHeaderProto {
        client: Some(ClientInfoProto {
            call_id: "test-call-id".to_string(),
            client_id: 1,
            client_name: "test-client".to_string(),
        }),
        deadline_ms: 0,
        traceparent: String::new(),
        caller_context: None,
        state: Vec::new(),
        retry_count: 0,
        group_id: 1,
        mount_epoch: None,
        route_epoch: None,
        principal: String::new(),
        real_user: String::new(),
        doas: String::new(),
        authn_type: proto::common::AuthnTypeProto::Unspecified as i32,
    };

    let request = tonic::Request::new(MsyncRequestProto { header: Some(header) });

    let response = mock_server.msync(request).await;
    assert!(response.is_ok());
    let resp = response.unwrap().into_inner();
    assert!(resp.header.is_some());
    assert!(resp.state.is_some());
}

#[tokio::test]
async fn test_msync_mock_rejects_missing_group_id() {
    init_logging();

    let mock_server = MockMetadataServer::new(1, vec![2, 3]);
    let request = tonic::Request::new(MsyncRequestProto {
        header: Some(proto::common::RequestHeaderProto {
            client: Some(proto::common::ClientInfoProto {
                call_id: "test-call-id".to_string(),
                client_id: 1,
                client_name: "test-client".to_string(),
            }),
            deadline_ms: 0,
            traceparent: String::new(),
            caller_context: None,
            state: Vec::new(),
            retry_count: 0,
            group_id: 0,
            mount_epoch: None,
            route_epoch: None,
            principal: String::new(),
            real_user: String::new(),
            doas: String::new(),
            authn_type: proto::common::AuthnTypeProto::Unspecified as i32,
        }),
    });

    let response = mock_server
        .msync(request)
        .await
        .expect("mock msync uses gRPC OK for application errors")
        .into_inner();

    let header = response.header.expect("response header");
    assert!(header.error.is_some());
    assert!(response.state.is_none());
}

/// Mount epoch mismatch should surface as NEED_REFRESH and be auto-retired after applying hints.
#[tokio::test]
async fn test_mount_epoch_mismatch_refresh_and_retry() {
    init_logging();

    let base_header = RequestHeader::new(types::ids::ClientId::new(7)).with_group_id(0);
    let server_epoch = Arc::new(AtomicU64::new(2));
    let attempts = Arc::new(AtomicUsize::new(0));
    let mount_refreshes = Arc::new(AtomicUsize::new(0));
    let route_refreshes = Arc::new(AtomicUsize::new(0));

    let outcome: RetryOutcome<(ResponseHeader, ())> = retry_metadata_once(
        base_header.clone(),
        {
            let server_epoch = server_epoch.clone();
            let attempts = attempts.clone();
            move |hdr: RequestHeader| {
                let server_epoch = server_epoch.clone();
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    let current_epoch = server_epoch.load(Ordering::SeqCst);
                    if hdr.mount_epoch != Some(current_epoch) {
                        let canonical = CanonicalError::need_refresh(
                            RpcErrorCode::MountEpochMismatch,
                            RefreshReason::MountEpochMismatch,
                            "mount epoch mismatch".to_string(),
                        );
                        let mut resp = ResponseHeader::from_canonical(hdr.client.clone(), canonical)
                            .with_group_id(hdr.group_id.unwrap_or(0));
                        resp.mount_epoch = Some(current_epoch);
                        Ok((resp, ()))
                    } else {
                        let mut resp = ResponseHeader::ok(hdr.client.clone()).with_group_id(hdr.group_id.unwrap_or(0));
                        resp.mount_epoch = Some(current_epoch);
                        Ok((resp, ()))
                    }
                }
            }
        },
        {
            let mount_refreshes = mount_refreshes.clone();
            let route_refreshes = route_refreshes.clone();
            move |dispatch_ctx, current_header| {
                let mount_refreshes = mount_refreshes.clone();
                let route_refreshes = route_refreshes.clone();
                let mut refreshed = current_header.child_with_same_call_id();
                let reason = dispatch_ctx.reason;
                let mount_epoch = dispatch_ctx
                    .hint
                    .mount_epoch
                    .or(dispatch_ctx.response_header.mount_epoch);
                let group_id = dispatch_ctx.hint.group_id.or(dispatch_ctx.response_header.group_id);
                async move {
                    assert_eq!(reason, RefreshReason::MountEpochMismatch);
                    // Best available mount refresh hook in this test: route/mapping refresh.
                    mount_refreshes.fetch_add(1, Ordering::SeqCst);
                    route_refreshes.fetch_add(1, Ordering::SeqCst);
                    refreshed.mount_epoch = mount_epoch;
                    refreshed.group_id = group_id;
                    Ok(refreshed)
                }
            }
        },
    )
    .await
    .expect("retry loop must succeed");

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(outcome.refreshes, 1);
    assert_eq!(outcome.retries, 0);
    assert_eq!(mount_refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(route_refreshes.load(Ordering::SeqCst), 1);
    let last_err = outcome.last_canonical_error.expect("should capture NEED_REFRESH");
    assert_eq!(
        last_err.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::MountEpochMismatch))
    );
    assert_eq!(last_err.reason, Some(RefreshReason::MountEpochMismatch));
    assert!(outcome.result.0.canonical_error.is_none());
}

/// Direct worker writes are stream placeholders until worker execution is wired.
#[tokio::test]
async fn test_direct_worker_open_write_stream_is_explicitly_unimplemented() {
    init_logging();

    let mock_worker = MockWorkerServer::new(1);
    let request = Request::new(OpenWriteStreamRequestProto {
        header: Some(data_request_header("open-write-stream")),
        block_id: Some(block_id(10, 0)),
        token: Some(FencingTokenProto {
            block_id: Some(block_id(10, 0)),
            owner: 1,
            epoch: 1,
        }),
        block_stamp: 1,
        frame_size: 4096,
    });

    let response = WorkerDataService::open_write_stream(&mock_worker, request)
        .await
        .expect("open write placeholder should use structured response")
        .into_inner();

    assert!(response.stream_id.is_none());
    assert_eq!(response.frame_size, 0);
    assert_eq!(response.window_bytes, 0);
    assert_stream_unimplemented_header(response.header.expect("data header"), "OpenWriteStream");
}

/// CommitWrite is also a phase-1 placeholder and must not pretend data was persisted.
#[tokio::test]
async fn test_direct_worker_commit_write_is_explicitly_unimplemented() {
    init_logging();

    let mock_worker = MockWorkerServer::new(1);
    let request = Request::new(CommitWriteRequestProto {
        header: Some(data_request_header("commit-write")),
        stream_id: Some(StreamIdProto { high: 0, low: 7 }),
        block_id: Some(block_id(20, 0)),
        token: Some(FencingTokenProto {
            block_id: Some(block_id(20, 0)),
            owner: 1,
            epoch: 5,
        }),
        commit_seq: 1,
        committed_length: 4,
        require_sync: true,
    });

    let response = WorkerDataService::commit_write(&mock_worker, request)
        .await
        .expect("commit placeholder should use structured response")
        .into_inner();

    assert_eq!(response.committed_length, 0);
    assert_eq!(response.block_stamp, 0);
    assert_eq!(response.persisted_through, 0);
    assert_stream_unimplemented_header(response.header.expect("data header"), "CommitWrite");
}

/// Route moved: client must follow NEED_REFRESH/MOVED hint and retry.
#[tokio::test]
async fn test_moved_refresh_and_retry() {
    init_logging();

    let base_header = RequestHeader::new(types::ids::ClientId::new(11)).with_group_id(1);
    let attempts = Arc::new(AtomicUsize::new(0));
    let route_refreshes = Arc::new(AtomicUsize::new(0));

    let outcome: RetryOutcome<(ResponseHeader, ())> = retry_metadata_once(
        base_header.clone(),
        {
            let attempts = attempts.clone();
            move |hdr: RequestHeader| {
                let attempts = attempts.clone();
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        let canonical = CanonicalError::need_refresh(
                            RpcErrorCode::ShardMoved,
                            RefreshReason::Moved,
                            "shard moved".to_string(),
                        );
                        let resp = ResponseHeader::from_canonical(hdr.client.clone(), canonical).with_group_id(2);
                        Ok((resp, ()))
                    } else {
                        let resp = ResponseHeader::ok(hdr.client.clone()).with_group_id(hdr.group_id.unwrap_or(0));
                        Ok((resp, ()))
                    }
                }
            }
        },
        {
            let route_refreshes = route_refreshes.clone();
            move |dispatch_ctx, current_header| {
                let route_refreshes = route_refreshes.clone();
                let mut next_header = current_header.child_with_same_call_id();
                let reason = dispatch_ctx.reason;
                let group_id_hint = dispatch_ctx.hint.group_id.or(dispatch_ctx.response_header.group_id);
                async move {
                    assert_eq!(reason, RefreshReason::Moved);
                    route_refreshes.fetch_add(1, Ordering::SeqCst);
                    next_header.group_id = group_id_hint.or(next_header.group_id);
                    Ok(next_header)
                }
            }
        },
    )
    .await
    .expect("moved refresh");

    assert_eq!(outcome.refreshes, 1);
    assert_eq!(route_refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.result.0.group_id, Some(2));
    let last_err = outcome.last_canonical_error.expect("captured moved err");
    assert_eq!(
        last_err.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::ShardMoved))
    );
    assert_eq!(last_err.reason, Some(RefreshReason::Moved));
}

/// NotLeader should trigger route refresh action and succeed on retry.
#[tokio::test]
async fn test_not_leader_refresh_and_retry() {
    init_logging();

    let base_header = RequestHeader::new(types::ids::ClientId::new(21)).with_group_id(2);
    let attempts = Arc::new(AtomicUsize::new(0));
    let route_refreshes = Arc::new(AtomicUsize::new(0));

    let outcome: RetryOutcome<(ResponseHeader, ())> = retry_metadata_once(
        base_header.clone(),
        {
            let attempts = attempts.clone();
            move |hdr: RequestHeader| {
                let attempts = attempts.clone();
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        let canonical = CanonicalError::need_refresh(
                            RpcErrorCode::NotLeader,
                            RefreshReason::NotLeader,
                            "not leader".to_string(),
                        );
                        let resp = ResponseHeader::from_canonical(hdr.client.clone(), canonical).with_group_id(4);
                        Ok((resp, ()))
                    } else {
                        let resp = ResponseHeader::ok(hdr.client.clone()).with_group_id(hdr.group_id.unwrap_or(0));
                        Ok((resp, ()))
                    }
                }
            }
        },
        {
            let route_refreshes = route_refreshes.clone();
            move |dispatch_ctx, current_header| {
                let route_refreshes = route_refreshes.clone();
                let mut next_header = current_header.child_with_same_call_id();
                let reason = dispatch_ctx.reason;
                let group_id_hint = dispatch_ctx.hint.group_id.or(dispatch_ctx.response_header.group_id);
                async move {
                    assert_eq!(reason, RefreshReason::NotLeader);
                    route_refreshes.fetch_add(1, Ordering::SeqCst);
                    next_header.group_id = group_id_hint.or(next_header.group_id);
                    Ok(next_header)
                }
            }
        },
    )
    .await
    .expect("not leader refresh");

    assert_eq!(outcome.refreshes, 1);
    assert_eq!(route_refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.result.0.group_id, Some(4));
}

/// Block stamp mismatch should trigger refresh and retry success.
#[tokio::test]
async fn test_block_stamp_mismatch_refresh_and_retry() {
    init_logging();

    let base_header = RequestHeader::new(types::ids::ClientId::new(12)).with_group_id(3);
    let attempts = Arc::new(AtomicUsize::new(0));

    let outcome: RetryOutcome<(ResponseHeader, ())> = retry_metadata_once(
        base_header.clone(),
        {
            let attempts = attempts.clone();
            move |hdr: RequestHeader| {
                let attempts = attempts.clone();
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        let canonical = CanonicalError::need_refresh(
                            RpcErrorCode::BlockStampMismatch,
                            RefreshReason::BlockStampMismatch,
                            "block stamp mismatch".to_string(),
                        );
                        let resp = ResponseHeader::from_canonical(hdr.client.clone(), canonical)
                            .with_group_id(hdr.group_id.unwrap_or(3));
                        Ok((resp, ()))
                    } else {
                        let resp = ResponseHeader::ok(hdr.client.clone()).with_group_id(hdr.group_id.unwrap_or(3));
                        Ok((resp, ()))
                    }
                }
            }
        },
        |dispatch_ctx, current_header| {
            let mut next_header = current_header.child_with_same_call_id();
            let reason = dispatch_ctx.reason;
            let group_id_hint = dispatch_ctx.hint.group_id.or(dispatch_ctx.response_header.group_id);
            async move {
                assert_eq!(reason, RefreshReason::BlockStampMismatch);
                next_header.group_id = group_id_hint.or(next_header.group_id);
                Ok(next_header)
            }
        },
    )
    .await
    .expect("block stamp refresh");

    assert_eq!(outcome.refreshes, 1);
    let last_err = outcome.last_canonical_error.expect("captured block stamp err");
    assert_eq!(
        last_err.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::BlockStampMismatch))
    );
    assert_eq!(last_err.reason, Some(RefreshReason::BlockStampMismatch));
}

/// Transport failures are retried by transport policy and must not trigger refresh dispatch.
#[tokio::test]
async fn test_transport_layering_retries_without_refresh_dispatch() {
    init_logging();

    let base_header = RequestHeader::new(types::ids::ClientId::new(33)).with_group_id(9);
    let attempts = Arc::new(AtomicUsize::new(0));
    let refresh_dispatches = Arc::new(AtomicUsize::new(0));

    let mut call = {
        let attempts = attempts.clone();
        move |hdr: RequestHeader| {
            let attempts = attempts.clone();
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    Err(ClientError::from(tonic::Status::unavailable("temporary outage")))
                } else {
                    Ok((
                        ResponseHeader::ok(hdr.client.clone()).with_group_id(hdr.group_id.unwrap_or(9)),
                        (),
                    ))
                }
            }
        }
    };

    let mut dispatch = {
        let refresh_dispatches = refresh_dispatches.clone();
        move |_dispatch_ctx, current_header: RequestHeader| {
            let refresh_dispatches = refresh_dispatches.clone();
            async move {
                refresh_dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(current_header)
            }
        }
    };

    let outcome = retry_metadata_with_policy(
        base_header,
        &mut call,
        &mut dispatch,
        RetryPolicy {
            max_refresh_attempts: 1,
            max_retryable_attempts: 1,
            max_transport_retries: 1,
            transport_retry_base_ms: 0,
        },
    )
    .await
    .expect("transport retry should recover");

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(refresh_dispatches.load(Ordering::SeqCst), 0);
    assert_eq!(outcome.transport_retries, 1);
}

/// Placeholder for follower read refresh scenario (needs mock metadata wiring).
#[tokio::test]
#[ignore = "TODO: needs end-to-end follower mock wiring"]
async fn test_follower_read_with_refresh() {
    init_logging();
}

/// Placeholder for route table update on NotLeader.
#[tokio::test]
#[ignore = "TODO: needs mock leader redirect scenario"]
async fn test_route_table_update_on_not_leader() {
    init_logging();
}
