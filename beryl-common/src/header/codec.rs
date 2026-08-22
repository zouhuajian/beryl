// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Transport markers shared by gRPC producers and consumers.

/// Identifies a request rejected before any gRPC handler executes.
pub const HEADER_PRE_HANDLER_REJECTION: &str = "beryl-pre-handler-rejection";
/// Marker value for pre-handler request-concurrency rejection.
pub const PRE_HANDLER_REJECTION_RPC_CONCURRENCY: &str = "rpc-concurrency";
/// Identifies a Worker data rejection made before any local data side effect.
pub const HEADER_WORKER_DATA_REJECTION: &str = "beryl-worker-data-rejection";
/// Marker value for Worker capacity rejection before staging or read IO begins.
pub const WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT: &str = "capacity-before-side-effect";
/// Identifies structured Worker data errors encoded in gRPC status details.
pub const HEADER_WORKER_DATA_ERROR_DETAIL: &str = "beryl-worker-data-error-detail";
/// Version marker for `DataResponseHeaderProto` encoded in status details.
pub const WORKER_DATA_ERROR_DETAIL_V1: &str = "v1";
