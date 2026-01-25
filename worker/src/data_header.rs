// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Data-plane header types for worker operations.
//!
//! This module provides Rust domain types for data-plane specific headers
//! (DataRequestHeaderProto/DataResponseHeaderProto) used in OpenReadStream/OpenWriteStream operations.
//!
//! NOTE: Error semantics are now unified via common.ErrorDetailProto, matching control-plane ResponseHeaderProto.

use common::error::canonical::{CanonicalError, ErrorClass, RefreshReason};
use common::header::ClientInfo;
use proto::common::ErrorDetailProto;
use proto::worker::{DataRequestHeaderProto, DataResponseHeaderProto};

/// Request header for data-plane operations (OpenReadStream/OpenWriteStream).
#[derive(Clone, Debug)]
pub struct DataRequestHeader {
    /// Client information (call_id, client_id, client_name).
    pub client: ClientInfo,
    /// W3C Trace Context: traceparent header value.
    pub traceparent: Option<String>,
}

/// Response header for data-plane operations (OpenReadStream/OpenWriteStream).
/// Uses the same canonical error model as control-plane ResponseHeaderProto.
#[derive(Clone, Debug)]
pub struct DataResponseHeader {
    /// Client information (call_id, client_id, client_name).
    pub client: ClientInfo,
    /// Canonical error detail (single source of truth for all error semantics).
    /// If None or error_class = OK, the operation succeeded.
    pub error: Option<CanonicalError>,
    /// Worker epoch hint for client refresh.
    pub worker_epoch: Option<u64>,
    /// Optional endpoint hint to allow client to reconnect.
    pub endpoint_hint: Option<proto::common::WorkerEndpointInfoProto>,
}

impl DataRequestHeader {
    /// Create a new DataRequestHeader from ClientInfo.
    pub fn new(client: ClientInfo) -> Self {
        Self {
            client,
            traceparent: None,
        }
    }

    /// Set the traceparent.
    pub fn with_traceparent(mut self, traceparent: String) -> Self {
        self.traceparent = Some(traceparent);
        self
    }
}

impl DataResponseHeader {
    /// Create a successful response header.
    pub fn ok(client: ClientInfo) -> Self {
        Self {
            client,
            error: None,
            worker_epoch: None,
            endpoint_hint: None,
        }
    }

    /// Create a NEED_REFRESH response header with reason and message.
    pub fn need_refresh(
        client: ClientInfo,
        reason: RefreshReason,
        rpc_code: common::header::RpcErrorCode,
        message: String,
    ) -> Self {
        Self {
            client,
            error: Some(CanonicalError::need_refresh(rpc_code, reason, message)),
            worker_epoch: None,
            endpoint_hint: None,
        }
    }

    /// Create a RETRYABLE response header with retry_after_ms and message.
    pub fn retryable(
        client: ClientInfo,
        rpc_code: common::header::RpcErrorCode,
        retry_after_ms: Option<u64>,
        message: String,
    ) -> Self {
        Self {
            client,
            error: Some(CanonicalError::retryable(rpc_code, retry_after_ms, message)),
            worker_epoch: None,
            endpoint_hint: None,
        }
    }

    /// Create a FATAL response header with message.
    pub fn fatal(client: ClientInfo, rpc_code: common::header::RpcErrorCode, message: String) -> Self {
        Self {
            client,
            error: Some(CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(common::error::canonical::ErrorCode::RpcCode(rpc_code)),
                reason: None,
                retry_after_ms: None,
                message,
            }),
            worker_epoch: None,
            endpoint_hint: None,
        }
    }
}

// ============================================================================
// Proto Conversions
// ============================================================================

impl DataRequestHeader {
    /// Convert from proto.
    pub fn from_proto(proto: DataRequestHeaderProto) -> Result<Self, String> {
        let client = proto
            .client
            .clone()
            .ok_or_else(|| "missing client".to_string())?
            .try_into()
            .map_err(|e: String| format!("Failed to convert ClientInfoProto: {}", e))?;
        let traceparent = if proto.traceparent.is_empty() {
            None
        } else {
            Some(proto.traceparent)
        };

        Ok(DataRequestHeader { client, traceparent })
    }

    /// Convert to proto.
    pub fn to_proto(&self) -> DataRequestHeaderProto {
        DataRequestHeaderProto {
            client: Some((&self.client).into()),
            traceparent: self.traceparent.clone().unwrap_or_default(),
        }
    }
}

impl DataResponseHeader {
    /// Convert from proto.
    pub fn from_proto(proto: DataResponseHeaderProto) -> Result<Self, String> {
        let client = proto
            .client
            .clone()
            .ok_or_else(|| "missing client".to_string())?
            .try_into()
            .map_err(|e: String| format!("Failed to convert ClientInfoProto: {}", e))?;

        // Convert ErrorDetailProto to CanonicalError
        let error = proto.error.as_ref().map(|err_detail| {
            // Map ErrorClassProto to ErrorClass
            let error_class = match err_detail.error_class() {
                proto::common::ErrorClassProto::ErrorClassOk => ErrorClass::Ok,
                proto::common::ErrorClassProto::ErrorClassNeedRefresh => ErrorClass::NeedRefresh,
                proto::common::ErrorClassProto::ErrorClassRetryable => ErrorClass::Retryable,
                proto::common::ErrorClassProto::ErrorClassFatal => ErrorClass::Fatal,
            };

            // Map oneof code
            let code = match &err_detail.code {
                Some(proto::common::error_detail_proto::Code::FsErrno(fs_errno)) => {
                    // Map FsErrnoProto to FsErrorCode
                    let fs_code = match *fs_errno {
                        x if x == proto::common::FsErrnoProto::FsErrnoOk as i32 => types::fs::FsErrorCode::Ok,
                        x if x == proto::common::FsErrnoProto::FsErrnoEnoent as i32 => types::fs::FsErrorCode::ENoEnt,
                        x if x == proto::common::FsErrnoProto::FsErrnoEexist as i32 => types::fs::FsErrorCode::EExist,
                        x if x == proto::common::FsErrnoProto::FsErrnoEnotempty as i32 => {
                            types::fs::FsErrorCode::ENotEmpty
                        }
                        x if x == proto::common::FsErrnoProto::FsErrnoEnotdir as i32 => types::fs::FsErrorCode::ENotDir,
                        x if x == proto::common::FsErrnoProto::FsErrnoEisdir as i32 => types::fs::FsErrorCode::EIsDir,
                        x if x == proto::common::FsErrnoProto::FsErrnoExdev as i32 => types::fs::FsErrorCode::EXDev,
                        x if x == proto::common::FsErrnoProto::FsErrnoEperm as i32 => types::fs::FsErrorCode::EPerm,
                        x if x == proto::common::FsErrnoProto::FsErrnoEacces as i32 => types::fs::FsErrorCode::EAcces,
                        x if x == proto::common::FsErrnoProto::FsErrnoEinval as i32 => types::fs::FsErrorCode::EInval,
                        x if x == proto::common::FsErrnoProto::FsErrnoEnotsup as i32 => types::fs::FsErrorCode::ENotsup,
                        x if x == proto::common::FsErrnoProto::FsErrnoEnotimpl as i32 => {
                            types::fs::FsErrorCode::ENotImpl
                        }
                        x if x == proto::common::FsErrnoProto::FsErrnoEagain as i32 => types::fs::FsErrorCode::EAgain,
                        x if x == proto::common::FsErrnoProto::FsErrnoEbusy as i32 => types::fs::FsErrorCode::EBusy,
                        _ => types::fs::FsErrorCode::EInval,
                    };
                    Some(common::error::canonical::ErrorCode::FsErrno(fs_code))
                }
                Some(proto::common::error_detail_proto::Code::RpcCode(rpc_code)) => {
                    // Map RpcErrorCodeProto to RpcErrorCode
                    let rpc_code_enum = match *rpc_code {
                        x if x == proto::common::RpcErrorCodeProto::RpcErrCodeUnspecified as i32 => {
                            common::header::RpcErrorCode::Unspecified
                        }
                        x if x == proto::common::RpcErrorCodeProto::RpcErrCodeNotLeader as i32 => {
                            common::header::RpcErrorCode::NotLeader
                        }
                        x if x == proto::common::RpcErrorCodeProto::RpcErrCodeStaleState as i32 => {
                            common::header::RpcErrorCode::StaleState
                        }
                        x if x == proto::common::RpcErrorCodeProto::RpcErrCodeMountEpochMismatch as i32 => {
                            common::header::RpcErrorCode::MountEpochMismatch
                        }
                        x if x == proto::common::RpcErrorCodeProto::RpcErrCodeRouteEpochMismatch as i32 => {
                            common::header::RpcErrorCode::RouteEpochMismatch
                        }
                        x if x == proto::common::RpcErrorCodeProto::RpcErrCodeWorkerEpochMismatch as i32 => {
                            common::header::RpcErrorCode::WorkerEpochMismatch
                        }
                        x if x == proto::common::RpcErrorCodeProto::RpcErrCodeBlockStampMismatch as i32 => {
                            common::header::RpcErrorCode::BlockStampMismatch
                        }
                        x if x == proto::common::RpcErrorCodeProto::RpcErrCodeEpochMismatch as i32 => {
                            common::header::RpcErrorCode::EpochMismatch
                        }
                        x if x == proto::common::RpcErrorCodeProto::RpcErrCodeShardMoved as i32 => {
                            common::header::RpcErrorCode::ShardMoved
                        }
                        x if x == proto::common::RpcErrorCodeProto::RpcErrCodeFencing as i32 => {
                            common::header::RpcErrorCode::Fencing
                        }
                        x if x == proto::common::RpcErrorCodeProto::RpcErrCodeNodeUnavailable as i32 => {
                            common::header::RpcErrorCode::NodeUnavailable
                        }
                        x if x == proto::common::RpcErrorCodeProto::RpcErrCodeApplication as i32 => {
                            common::header::RpcErrorCode::Application
                        }
                        _ => common::header::RpcErrorCode::Application,
                    };
                    Some(common::error::canonical::ErrorCode::RpcCode(rpc_code_enum))
                }
                None => None,
            };

            // Map RefreshReasonProto to RefreshReason
            let reason = match err_detail.refresh_reason() {
                proto::common::RefreshReasonProto::RefreshReasonUnknown => Some(RefreshReason::Unknown),
                proto::common::RefreshReasonProto::RefreshReasonNotLeader => Some(RefreshReason::NotLeader),
                proto::common::RefreshReasonProto::RefreshReasonMoved => Some(RefreshReason::Moved),
                proto::common::RefreshReasonProto::RefreshReasonStaleState => Some(RefreshReason::StaleState),
                proto::common::RefreshReasonProto::RefreshReasonMountEpochMismatch => {
                    Some(RefreshReason::MountEpochMismatch)
                }
                proto::common::RefreshReasonProto::RefreshReasonRouteEpochMismatch => {
                    Some(RefreshReason::RouteEpochMismatch)
                }
                proto::common::RefreshReasonProto::RefreshReasonWorkerEpochMismatch => {
                    Some(RefreshReason::WorkerEpochMismatch)
                }
                proto::common::RefreshReasonProto::RefreshReasonBlockStampMismatch => {
                    Some(RefreshReason::BlockStampMismatch)
                }
                proto::common::RefreshReasonProto::RefreshReasonFencing => Some(RefreshReason::Fencing),
                proto::common::RefreshReasonProto::RefreshReasonEpochMismatch => Some(RefreshReason::EpochMismatch),
            };

            CanonicalError {
                class: error_class,
                code,
                reason,
                retry_after_ms: err_detail.retry_after_ms,
                message: err_detail.message.clone(),
            }
        });

        let worker_epoch = proto.worker_epoch;
        let endpoint_hint = proto.endpoint_hint;

        Ok(DataResponseHeader {
            client,
            error,
            worker_epoch,
            endpoint_hint,
        })
    }

    /// Convert to proto.
    pub fn to_proto(&self) -> DataResponseHeaderProto {
        // Convert CanonicalError to ErrorDetailProto
        let error_detail = self.error.as_ref().map(|err| {
            // Map ErrorClass to ErrorClassProto
            let error_class = match err.class {
                ErrorClass::Ok => proto::common::ErrorClassProto::ErrorClassOk,
                ErrorClass::NeedRefresh => proto::common::ErrorClassProto::ErrorClassNeedRefresh,
                ErrorClass::Retryable => proto::common::ErrorClassProto::ErrorClassRetryable,
                ErrorClass::Fatal => proto::common::ErrorClassProto::ErrorClassFatal,
            };

            // Map ErrorCode to oneof
            let code = match &err.code {
                Some(common::error::canonical::ErrorCode::FsErrno(fs_errno)) => {
                    let fs_errno_proto = match fs_errno {
                        types::fs::FsErrorCode::Ok => proto::common::FsErrnoProto::FsErrnoOk,
                        types::fs::FsErrorCode::ENoEnt => proto::common::FsErrnoProto::FsErrnoEnoent,
                        types::fs::FsErrorCode::EExist => proto::common::FsErrnoProto::FsErrnoEexist,
                        types::fs::FsErrorCode::ENotEmpty => proto::common::FsErrnoProto::FsErrnoEnotempty,
                        types::fs::FsErrorCode::ENotDir => proto::common::FsErrnoProto::FsErrnoEnotdir,
                        types::fs::FsErrorCode::EIsDir => proto::common::FsErrnoProto::FsErrnoEisdir,
                        types::fs::FsErrorCode::EXDev => proto::common::FsErrnoProto::FsErrnoExdev,
                        types::fs::FsErrorCode::EPerm => proto::common::FsErrnoProto::FsErrnoEperm,
                        types::fs::FsErrorCode::EAcces => proto::common::FsErrnoProto::FsErrnoEacces,
                        types::fs::FsErrorCode::EInval => proto::common::FsErrnoProto::FsErrnoEinval,
                        types::fs::FsErrorCode::ENotsup => proto::common::FsErrnoProto::FsErrnoEnotsup,
                        types::fs::FsErrorCode::ENotImpl => proto::common::FsErrnoProto::FsErrnoEnotimpl,
                        types::fs::FsErrorCode::EAgain => proto::common::FsErrnoProto::FsErrnoEagain,
                        types::fs::FsErrorCode::EBusy => proto::common::FsErrnoProto::FsErrnoEbusy,
                    };
                    Some(proto::common::error_detail_proto::Code::FsErrno(fs_errno_proto as i32))
                }
                Some(common::error::canonical::ErrorCode::RpcCode(rpc_code)) => {
                    let rpc_code_proto = match rpc_code {
                        common::header::RpcErrorCode::Unspecified => {
                            proto::common::RpcErrorCodeProto::RpcErrCodeUnspecified
                        }
                        common::header::RpcErrorCode::NotLeader => {
                            proto::common::RpcErrorCodeProto::RpcErrCodeNotLeader
                        }
                        common::header::RpcErrorCode::StaleState => {
                            proto::common::RpcErrorCodeProto::RpcErrCodeStaleState
                        }
                        common::header::RpcErrorCode::MountEpochMismatch => {
                            proto::common::RpcErrorCodeProto::RpcErrCodeMountEpochMismatch
                        }
                        common::header::RpcErrorCode::RouteEpochMismatch => {
                            proto::common::RpcErrorCodeProto::RpcErrCodeRouteEpochMismatch
                        }
                        common::header::RpcErrorCode::WorkerEpochMismatch => {
                            proto::common::RpcErrorCodeProto::RpcErrCodeWorkerEpochMismatch
                        }
                        common::header::RpcErrorCode::BlockStampMismatch => {
                            proto::common::RpcErrorCodeProto::RpcErrCodeBlockStampMismatch
                        }
                        common::header::RpcErrorCode::EpochMismatch => {
                            proto::common::RpcErrorCodeProto::RpcErrCodeEpochMismatch
                        }
                        common::header::RpcErrorCode::ShardMoved => {
                            proto::common::RpcErrorCodeProto::RpcErrCodeShardMoved
                        }
                        common::header::RpcErrorCode::Fencing => proto::common::RpcErrorCodeProto::RpcErrCodeFencing,
                        common::header::RpcErrorCode::NodeUnavailable => {
                            proto::common::RpcErrorCodeProto::RpcErrCodeNodeUnavailable
                        }
                        common::header::RpcErrorCode::Application => {
                            proto::common::RpcErrorCodeProto::RpcErrCodeApplication
                        }
                        _ => proto::common::RpcErrorCodeProto::RpcErrCodeApplication,
                    };
                    Some(proto::common::error_detail_proto::Code::RpcCode(rpc_code_proto as i32))
                }
                None => None,
            };

            // Map RefreshReason to RefreshReasonProto
            let refresh_reason = err.reason.unwrap_or(RefreshReason::Unknown);
            let refresh_reason_proto = match refresh_reason {
                RefreshReason::Unknown => proto::common::RefreshReasonProto::RefreshReasonUnknown,
                RefreshReason::NotLeader => proto::common::RefreshReasonProto::RefreshReasonNotLeader,
                RefreshReason::Moved => proto::common::RefreshReasonProto::RefreshReasonMoved,
                RefreshReason::StaleState => proto::common::RefreshReasonProto::RefreshReasonStaleState,
                RefreshReason::MountEpochMismatch => proto::common::RefreshReasonProto::RefreshReasonMountEpochMismatch,
                RefreshReason::RouteEpochMismatch => proto::common::RefreshReasonProto::RefreshReasonRouteEpochMismatch,
                RefreshReason::WorkerEpochMismatch => {
                    proto::common::RefreshReasonProto::RefreshReasonWorkerEpochMismatch
                }
                RefreshReason::BlockStampMismatch => proto::common::RefreshReasonProto::RefreshReasonBlockStampMismatch,
                RefreshReason::Fencing => proto::common::RefreshReasonProto::RefreshReasonFencing,
                RefreshReason::EpochMismatch => proto::common::RefreshReasonProto::RefreshReasonEpochMismatch,
            };

            ErrorDetailProto {
                error_class: error_class as i32,
                code,
                refresh_reason: refresh_reason_proto as i32,
                retry_after_ms: err.retry_after_ms,
                message: err.message.clone(),
            }
        });

        DataResponseHeaderProto {
            client: Some((&self.client).into()),
            error: error_detail,
            worker_epoch: self.worker_epoch,
            endpoint_hint: self.endpoint_hint.clone(),
        }
    }
}
