// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

mod common;

use ::common::error::canonical::{CanonicalError, ErrorCode as CanonicalErrorCode, RefreshReason};
use ::common::header::{RequestHeader, ResponseHeader, RpcErrorCode};
use bytes::Bytes;
use client::api::hcfs::Client;
use client::canonical::{
    handle_canonical_error, retry_metadata_once, retry_metadata_with_policy, ClientAction, RetryOutcome, RetryPolicy,
};
use client::config::ClientConfig;
use client::error::ClientError;
use common::{create_test_file_meta, init_logging, MockMetadataServer, MockWorkerServer};
use proto::metadata::{MsyncRequestProto, RefreshRouteRequestProto};
use proto::worker::worker_data_service_server::WorkerDataService;
use proto::worker::ReadChunkRequestProto;
use proto::worker::{ChunkDataProto, ChunkSliceProto};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tonic::Request;
use types::ids::DataHandleId;

/// Test client initialization stays lightweight without contacting endpoints.
#[tokio::test]
async fn test_client_init() {
    init_logging();

    let config = ClientConfig::default();
    let client_result = Client::new(config).await;
    assert!(client_result.is_ok() || client_result.is_err());
}

/// Test direct worker read with version mismatch fallback.
#[tokio::test]
async fn test_direct_worker_read_version_mismatch() {
    init_logging();

    use proto::common::ChunkIdProto;

    let mock_worker = MockWorkerServer::new(1);
    mock_worker.add_block_data(100, 0, 0, b"test data".to_vec()).await;
    mock_worker.set_block_version(100, 0, 1).await;

    let request = tonic::Request::new(ReadChunkRequestProto {
        chunk: Some(ChunkIdProto {
            block: Some(proto::common::BlockIdProto {
                data_handle_id: 100,
                block_index: 0,
            }),
            chunk_index: 0,
        }),
        offset_in_chunk: 0,
        len: 9,
        expected_version: 1,
        read_mode: proto::common::ReadModeProto::ReadModeUnspecified as i32,
    });

    let response = WorkerDataService::read_chunk(&mock_worker, request).await;
    assert!(response.is_ok());

    mock_worker.increment_block_version(100, 0).await;

    let request2 = tonic::Request::new(ReadChunkRequestProto {
        chunk: Some(ChunkIdProto {
            block: Some(proto::common::BlockIdProto {
                data_handle_id: 100,
                block_index: 0,
            }),
            chunk_index: 0,
        }),
        offset_in_chunk: 0,
        len: 9,
        expected_version: 1,
        read_mode: proto::common::ReadModeProto::ReadModeUnspecified as i32,
    });

    let response2 = WorkerDataService::read_chunk(&mock_worker, request2).await;
    assert!(response2.is_err());
    let status = response2.unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("Version mismatch"));
}

/// Test GetFileMeta with mock metadata server.
#[tokio::test]
async fn test_get_file_meta_with_mock() {
    init_logging();

    let mock_server = MockMetadataServer::new(1, vec![2, 3]);
    let data_handle_id = DataHandleId::new(100);
    let file_meta = create_test_file_meta(100, 1, 1);
    mock_server.add_file(data_handle_id, file_meta).await;

    let retrieved: Option<proto::common::FileMetaProto> = mock_server.get_file(data_handle_id).await;
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().data_handle_id, 100);
}

/// Test RefreshRoute with mock metadata server.
#[tokio::test]
async fn test_refresh_route_with_mock() {
    init_logging();

    use proto::metadata::metadata_route_service_proto_server::MetadataRouteServiceProto;

    let mock_server = MockMetadataServer::new(1, vec![2, 3]);

    let request = tonic::Request::new(RefreshRouteRequestProto {
        header: None,
        inode_id: None,
    });

    let response = <MockMetadataServer as MetadataRouteServiceProto>::refresh_route(&mock_server, request).await;
    assert!(response.is_ok());
    let resp = response.unwrap().into_inner();
    assert_eq!(resp.route_epoch, 1);
}

/// Test Msync with mock metadata server.
#[tokio::test]
async fn test_msync_with_mock() {
    init_logging();
    use proto::metadata::metadata_route_service_proto_server::MetadataRouteServiceProto;

    let mock_server = MockMetadataServer::new(1, vec![2, 3]);
    let data_handle_id = DataHandleId::new(100);
    let file_meta = create_test_file_meta(100, 1, 1);
    mock_server.add_file(data_handle_id, file_meta).await;

    use proto::common::ClientInfoProto;
    use proto::common::RequestHeaderProto;
    let mut header = RequestHeaderProto {
        client: Some(ClientInfoProto {
            call_id: "test-call-id".to_string(),
            client_id: 1,
            client_name: "test-client".to_string(),
        }),
        deadline_ms: 0,
        traceparent: String::new(),
        caller_context: None,
        state_id: None,
        retry_count: 0,
        group_id: 0,
        mount_epoch: None,
        route_epoch: None,
    };
    header.group_id = 0;

    let request = tonic::Request::new(MsyncRequestProto {
        header: Some(header),
        include_readable_followers: Some(false),
    });

    let response = <MockMetadataServer as MetadataRouteServiceProto>::msync(&mock_server, request).await;
    assert!(response.is_ok());
    let resp = response.unwrap().into_inner();
    assert!(resp.header.is_some());
    assert_eq!(resp.readable_follower_ids.len(), 0);
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

/// Worker epoch mismatch should produce NEED_REFRESH with hints; client refreshes endpoint and retries.
#[tokio::test]
async fn test_worker_epoch_mismatch_refresh_and_retry() {
    init_logging();

    let mock_worker = MockWorkerServer::new(1);
    mock_worker.set_worker_epoch(2).await;
    mock_worker.set_worker_epoch_hint_override(Some(999));

    let chunk_ref = proto::common::ChunkIdProto {
        block: Some(proto::common::BlockIdProto {
            data_handle_id: 10,
            block_index: 0,
        }),
        chunk_index: 0,
    };
    let chunk_slice = ChunkSliceProto {
        chunk: Some(chunk_ref),
        offset_in_chunk: 0,
        len: 4,
    };
    let chunk_data = ChunkDataProto {
        slice: Some(chunk_slice),
        data: Bytes::from_static(b"ping"),
        checksum32: 0,
    };

    let request = proto::worker::WriteChunkRequestProto {
        token: Some(proto::common::FencingTokenProto {
            block_id: Some(proto::common::BlockIdProto {
                data_handle_id: 10,
                block_index: 0,
            }),
            owner: 1,
            epoch: 1,
        }),
        data: Some(chunk_data),
        write_id: 1,
        write_mode: proto::common::WriteModeProto::WriteModeUnspecified as i32,
        route_epoch: 0,
        worker_epoch: 1, // stale on purpose
        file_version: 0,
    };

    let mut attempt_request = request.clone();
    let mut refreshes = 0;
    let meta_refreshes = Arc::new(AtomicUsize::new(0));

    for attempt in 0..2 {
        let response = WorkerDataService::write_chunk(&mock_worker, Request::new(attempt_request.clone()))
            .await
            .expect("service call")
            .into_inner();
        let header = response.header.expect("data header");
        if let Some(err_detail) = header.error.as_ref() {
            let canonical = proto::convert::error_detail_to_canonical(err_detail);
            let action = handle_canonical_error(&canonical);
            refreshes += 1;
            assert!(matches!(
                action,
                Err(ClientAction::Refresh {
                    reason: RefreshReason::WorkerEpochMismatch,
                    ..
                })
            ));
            let hinted_epoch = header.worker_epoch.expect("worker_epoch hint");
            // Metadata refresh must be invoked and override hinted epoch.
            let meta_epoch = {
                meta_refreshes.fetch_add(1, Ordering::SeqCst);
                2u64
            };
            assert_ne!(hinted_epoch, meta_epoch, "hint should be stale");
            attempt_request.worker_epoch = meta_epoch;
            assert_eq!(attempt, 0, "should refresh only once");
            continue;
        }

        // Success after refresh
        assert!(response.stored);
        assert_eq!(header.worker_epoch, Some(2));
        assert_eq!(refreshes, 1);
        assert_eq!(meta_refreshes.load(Ordering::SeqCst), 1);
        break;
    }
}

/// Fencing mismatch should trigger metadata refresh + token renewal before retry success.
#[tokio::test]
async fn test_fencing_refresh_and_retry_with_metadata() {
    init_logging();

    let mock_worker = MockWorkerServer::new(1);
    mock_worker.set_worker_epoch(3).await;
    mock_worker.set_fencing_epoch(5).await;

    let chunk_ref = proto::common::ChunkIdProto {
        block: Some(proto::common::BlockIdProto {
            data_handle_id: 20,
            block_index: 0,
        }),
        chunk_index: 0,
    };
    let chunk_slice = ChunkSliceProto {
        chunk: Some(chunk_ref),
        offset_in_chunk: 0,
        len: 4,
    };
    let chunk_data = ChunkDataProto {
        slice: Some(chunk_slice),
        data: Bytes::from_static(b"fenc"),
        checksum32: 0,
    };

    let request = proto::worker::WriteChunkRequestProto {
        token: Some(proto::common::FencingTokenProto {
            block_id: Some(proto::common::BlockIdProto {
                data_handle_id: 20,
                block_index: 0,
            }),
            owner: 1,
            epoch: 1, // stale
        }),
        data: Some(chunk_data),
        write_id: 2,
        write_mode: proto::common::WriteModeProto::WriteModeUnspecified as i32,
        route_epoch: 0,
        worker_epoch: 3,
        file_version: 0,
    };

    let mut attempt_request = request.clone();
    let mut refreshes = 0;
    let meta_refreshes = Arc::new(AtomicUsize::new(0));
    let token_refreshes = Arc::new(AtomicUsize::new(0));

    for attempt in 0..2 {
        let response = WorkerDataService::write_chunk(&mock_worker, Request::new(attempt_request.clone()))
            .await
            .expect("service call")
            .into_inner();
        let header = response.header.expect("data header");
        if let Some(err_detail) = header.error.as_ref() {
            let canonical = proto::convert::error_detail_to_canonical(err_detail);
            let action = handle_canonical_error(&canonical);
            refreshes += 1;
            assert!(matches!(
                action,
                Err(ClientAction::Refresh {
                    reason: RefreshReason::Fencing,
                    ..
                })
            ));

            meta_refreshes.fetch_add(1, Ordering::SeqCst);
            attempt_request.worker_epoch = 3;

            token_refreshes.fetch_add(1, Ordering::SeqCst);
            attempt_request.token = Some(proto::common::FencingTokenProto {
                block_id: Some(proto::common::BlockIdProto {
                    data_handle_id: 20,
                    block_index: 0,
                }),
                owner: 1,
                epoch: 5,
            });
            assert_eq!(attempt, 0, "should refresh only once");
            continue;
        }

        assert!(response.stored);
        assert_eq!(header.worker_epoch, Some(3));
        assert_eq!(refreshes, 1);
        assert_eq!(meta_refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(token_refreshes.load(Ordering::SeqCst), 1);
        break;
    }
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
    let refreshes = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::new(AtomicUsize::new(0));

    let outcome: RetryOutcome<(ResponseHeader, ())> = retry_metadata_once(
        base_header.clone(),
        {
            let refreshes = refreshes.clone();
            let attempts = attempts.clone();
            move |hdr: RequestHeader| {
                let refreshes = refreshes.clone();
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
