// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! FileSystemServiceProto implementation.
//!
//! Handlers convert wire values, invoke one path-first `MetadataFileSystem`
//! operation, and map its result back to the wire response.

use super::filesystem::{
    AbortFileWriteArgs, AllocateBlockArgs, BlockLocationsTarget, CommitFileArgs, CreateDirectoryArgs, CreateFileArgs,
    DeleteArgs, FileRange, Freshness, GetBlockLocationsArgs, GetStatusArgs, ListStatusArgs, OpenFileArgs,
    OpenWriteArgs, RenameArgs, RenewLeaseArgs, SyncWriteArgs,
};
use super::wire::{
    file_attrs_from_proto, file_attrs_to_proto, header_from_fs_failure, header_from_rpc_error, located_block_to_proto,
    location_to_proto, ok_header_from_fs_success, request_context_from_proto,
};
use super::{MetadataFileSystem, MsyncHandler};
use crate::config::{NamespaceListConfig, MAX_LIST_STATUS_PAGE_SIZE};
use crate::error::{to_rpc_error, MetadataError};
use crate::raft::PublishMode;
use beryl_proto::common::{RequestHeaderProto, ResponseHeaderProto};
use beryl_proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use beryl_proto::metadata::{
    get_block_locations_request_proto, AbortFileWriteRequestProto, AbortFileWriteResponseProto,
    AllocateBlockRequestProto, AllocateBlockResponseProto, CommitFileRequestProto, CommitFileResponseProto,
    CommittedBlockProto, CreateDirectoryRequestProto, CreateDirectoryResponseProto, CreateFileRequestProto,
    CreateFileResponseProto, DeleteRequestProto, DeleteResponseProto, DirEntryProto, FileTypeProto,
    GetBlockLocationsRequestProto, GetBlockLocationsResponseProto, GetStatusRequestProto, GetStatusResponseProto,
    ListStatusRequestProto, ListStatusResponseProto, MsyncRequestProto, MsyncResponseProto, OpenFileRequestProto,
    OpenFileResponseProto, OpenWriteRequestProto, OpenWriteResponseProto, RenameRequestProto, RenameResponseProto,
    RenewLeaseRequestProto, RenewLeaseResponseProto, SyncWriteRequestProto, SyncWriteResponseProto, WriteHandleProto,
};
use beryl_types::ids::InodeId;
use beryl_types::{CommittedBlock, ContentGeneration, WriteHandle, WriteMode, MAX_FILE_EXTENTS};
use get_block_locations_request_proto::Target;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::instrument;

/// Attaches the common response envelope without duplicating field access in handlers.
trait HeaderResponse {
    fn with_header(self, header: ResponseHeaderProto) -> Self;
}

macro_rules! impl_header_response {
    ($($resp_ty:ty),+ $(,)?) => {
        $(
            impl HeaderResponse for $resp_ty {
                fn with_header(mut self, header: ResponseHeaderProto) -> Self {
                    self.header = Some(header);
                    self
                }
            }
        )+
    };
}

impl_header_response!(
    GetStatusResponseProto,
    ListStatusResponseProto,
    CreateDirectoryResponseProto,
    DeleteResponseProto,
    RenameResponseProto,
    OpenFileResponseProto,
    GetBlockLocationsResponseProto,
    CreateFileResponseProto,
    OpenWriteResponseProto,
    AllocateBlockResponseProto,
    CommitFileResponseProto,
    AbortFileWriteResponseProto,
    RenewLeaseResponseProto,
    SyncWriteResponseProto,
    MsyncResponseProto,
);

/// Unary gRPC adapter that validates wire requests before entering metadata authority.
pub struct MetadataFileSystemServiceImpl {
    filesystem: Arc<MetadataFileSystem>,
    msync: Option<MsyncHandler>,
    list_status: NamespaceListConfig,
}

macro_rules! response_with_header {
    ($resp:expr, $header:expr) => {{
        Ok(Response::new(HeaderResponse::with_header($resp, $header)))
    }};
}

macro_rules! error_response {
    ($resp_ty:ty, $header:expr) => {{
        response_with_header!(<$resp_ty>::default(), $header)
    }};
}

macro_rules! request_context_or_error {
    ($req:expr, $resp_ty:ty) => {{
        match request_context_from_proto(&$req.header) {
            Ok(ctx) => ctx,
            Err(err) => {
                return error_response!($resp_ty, header_from_rpc_error(&$req.header, None, None, &err));
            }
        }
    }};
}

impl MetadataFileSystemServiceImpl {
    /// Builds the wire adapter with immutable request-boundary policy.
    pub(crate) fn new(
        filesystem: Arc<MetadataFileSystem>,
        msync: Option<MsyncHandler>,
        list_status: NamespaceListConfig,
    ) -> Self {
        Self {
            filesystem,
            msync,
            list_status,
        }
    }

    /// Resolves the proto3 zero sentinel and rejects explicit oversized pages.
    ///
    /// Every successful result is positive and no larger than the configured
    /// server maximum, so lower layers never need an unbounded representation.
    fn list_status_page_size(&self, requested: u32) -> Result<usize, MetadataError> {
        let page_size = if requested == 0 {
            self.list_status.default_page_size()
        } else if requested > MAX_LIST_STATUS_PAGE_SIZE {
            return Err(MetadataError::ResourceExhausted(format!(
                "requested ListStatus limit {requested} exceeds compiled maximum {MAX_LIST_STATUS_PAGE_SIZE}"
            )));
        } else if requested <= self.list_status.max_page_size() {
            requested
        } else {
            return Err(MetadataError::ResourceExhausted(format!(
                "requested ListStatus limit {} exceeds server maximum {}",
                requested,
                self.list_status.max_page_size()
            )));
        };
        Ok(page_size as usize)
    }

    fn header_from_conversion_error(
        req_header: &Option<RequestHeaderProto>,
        err: MetadataError,
    ) -> ResponseHeaderProto {
        let rpc_error = to_rpc_error(err);
        header_from_rpc_error(req_header, None, None, &rpc_error)
    }

    fn freshness_from_header(header: &Option<RequestHeaderProto>) -> Freshness {
        Freshness {
            mount_epoch: header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: header.as_ref().and_then(|h| h.route_epoch),
        }
    }

    /// Validate the wire identity before passing a typed handle to filesystem authority.
    fn write_handle_or_error(
        header: &Option<RequestHeaderProto>,
        handle: Option<WriteHandleProto>,
    ) -> Result<WriteHandle, Box<ResponseHeaderProto>> {
        let invalid = |message: &str| {
            Box::new(header_from_rpc_error(
                header,
                None,
                None,
                &to_rpc_error(MetadataError::InvalidArgument(message.to_string())),
            ))
        };
        let handle = handle.ok_or_else(|| invalid("missing write_handle"))?;
        WriteHandle::try_from(handle).map_err(|message| invalid(&message))
    }

    fn committed_block_from_proto(block: CommittedBlockProto) -> Result<CommittedBlock, MetadataError> {
        CommittedBlock::try_from(block).map_err(MetadataError::InvalidArgument)
    }
}

#[tonic::async_trait]
impl FileSystemServiceProto for MetadataFileSystemServiceImpl {
    #[instrument(skip_all)]
    async fn get_status(
        &self,
        request: Request<GetStatusRequestProto>,
    ) -> Result<Response<GetStatusResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, GetStatusResponseProto);
        match self
            .filesystem
            .get_status(
                &req_ctx,
                GetStatusArgs {
                    path: req.path,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => response_with_header!(
                GetStatusResponseProto {
                    attrs: Some(file_attrs_to_proto(&success.payload.attrs)),
                    kind: FileTypeProto::from(success.payload.kind) as i32,
                    ..Default::default()
                },
                ok_header_from_fs_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(GetStatusResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn list_status(
        &self,
        request: Request<ListStatusRequestProto>,
    ) -> Result<Response<ListStatusResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, ListStatusResponseProto);
        let max_entries = match self.list_status_page_size(req.limit) {
            Ok(max_entries) => max_entries,
            Err(err) => {
                return error_response!(
                    ListStatusResponseProto,
                    Self::header_from_conversion_error(&req.header, err)
                );
            }
        };
        match self
            .filesystem
            .list_status(
                &req_ctx,
                ListStatusArgs {
                    path: req.path,
                    cursor_key: (!req.cursor.is_empty()).then_some(req.cursor),
                    max_entries,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => {
                let header = ok_header_from_fs_success(&req_ctx, &success);
                let payload = success.payload;
                let entries = payload
                    .entries
                    .into_iter()
                    .map(|entry| DirEntryProto {
                        name: entry.name,
                        kind: FileTypeProto::from(entry.kind) as i32,
                        attrs: Some(file_attrs_to_proto(&entry.attrs)),
                    })
                    .collect();
                response_with_header!(
                    ListStatusResponseProto {
                        entries,
                        next_cursor: payload.next_cursor_key,
                        eof: payload.eof,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(ListStatusResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn create_directory(
        &self,
        request: Request<CreateDirectoryRequestProto>,
    ) -> Result<Response<CreateDirectoryResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, CreateDirectoryResponseProto);

        match self
            .filesystem
            .create_directory(
                &req_ctx,
                CreateDirectoryArgs {
                    path: req.path,
                    parsed_attrs: file_attrs_from_proto(req.attrs),
                    recursive: req.recursive,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => {
                let header = ok_header_from_fs_success(&req_ctx, &success);
                let payload = success.payload;
                response_with_header!(
                    CreateDirectoryResponseProto {
                        attrs: Some(file_attrs_to_proto(&payload.attrs)),
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(CreateDirectoryResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    /// Decodes the required breaking delete options and preserves async cleanup semantics.
    ///
    /// Missing options fail closed instead of falling back to the removed
    /// legacy top-level `recursive` field.
    #[instrument(skip_all)]
    async fn delete(&self, request: Request<DeleteRequestProto>) -> Result<Response<DeleteResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, DeleteResponseProto);
        let Some(options) = req.options else {
            return error_response!(
                DeleteResponseProto,
                Self::header_from_conversion_error(
                    &req.header,
                    MetadataError::InvalidArgument("delete options are required".to_string()),
                )
            );
        };
        match self
            .filesystem
            .delete(
                &req_ctx,
                DeleteArgs {
                    path: req.path,
                    recursive: options.recursive,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => response_with_header!(
                DeleteResponseProto::default(),
                ok_header_from_fs_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(DeleteResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn rename(&self, request: Request<RenameRequestProto>) -> Result<Response<RenameResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, RenameResponseProto);
        match self
            .filesystem
            .rename(
                &req_ctx,
                RenameArgs {
                    src_path: req.src_path,
                    dst_path: req.dst_path,
                    flags: req.flags,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => response_with_header!(
                RenameResponseProto::default(),
                ok_header_from_fs_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(RenameResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn open_file(
        &self,
        request: Request<OpenFileRequestProto>,
    ) -> Result<Response<OpenFileResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, OpenFileResponseProto);
        match self
            .filesystem
            .open_file(
                &req_ctx,
                OpenFileArgs {
                    path: req.path,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => {
                let header = ok_header_from_fs_success(&req_ctx, &success);
                let payload = success.payload;
                response_with_header!(
                    OpenFileResponseProto {
                        inode_id: payload.inode_id.as_raw(),
                        file_size: payload.file_size,
                        generation: payload.generation.map(ContentGeneration::as_raw),
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(OpenFileResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn get_block_locations(
        &self,
        request: Request<GetBlockLocationsRequestProto>,
    ) -> Result<Response<GetBlockLocationsResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, GetBlockLocationsResponseProto);
        let target = match req.target {
            Some(Target::Path(path)) => BlockLocationsTarget::Path(path),
            Some(Target::InodeId(inode_id)) => {
                if inode_id == 0 {
                    return error_response!(
                        GetBlockLocationsResponseProto,
                        Self::header_from_conversion_error(
                            &req.header,
                            MetadataError::InvalidArgument("inode_id must be non-zero".to_string()),
                        )
                    );
                }
                BlockLocationsTarget::InodeId(InodeId::new(inode_id))
            }
            None => {
                return error_response!(
                    GetBlockLocationsResponseProto,
                    Self::header_from_conversion_error(
                        &req.header,
                        MetadataError::InvalidArgument("missing block location target".to_string()),
                    )
                )
            }
        };
        let range = req.range.map(|r| FileRange {
            offset: r.offset,
            len: r.len as u64,
        });
        match self
            .filesystem
            .get_block_locations(
                &req_ctx,
                GetBlockLocationsArgs {
                    target,
                    range,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => {
                let header = ok_header_from_fs_success(&req_ctx, &success);
                let payload = success.payload;
                response_with_header!(
                    GetBlockLocationsResponseProto {
                        inode_id: payload.inode_id.as_raw(),
                        file_size: payload.file_size,
                        generation: payload.generation.map(ContentGeneration::as_raw),
                        locations: payload.locations.iter().map(location_to_proto).collect(),
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(
                GetBlockLocationsResponseProto,
                header_from_fs_failure(&req_ctx, &failure)
            ),
        }
    }

    #[instrument(skip_all)]
    async fn create_file(
        &self,
        request: Request<CreateFileRequestProto>,
    ) -> Result<Response<CreateFileResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, CreateFileResponseProto);

        match self
            .filesystem
            .create_file(
                &req_ctx,
                CreateFileArgs {
                    path: req.path,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => {
                let header = ok_header_from_fs_success(&req_ctx, &success);
                let payload = success.payload;
                response_with_header!(
                    CreateFileResponseProto {
                        layout: Some((&payload.layout).into()),
                        write_handle: Some(
                            WriteHandle {
                                inode_id: payload.inode_id,
                                lease_epoch: payload.lease_epoch,
                            }
                            .into()
                        ),
                        expires_at_ms: payload.expires_at_ms,
                        generation: payload.generation.as_raw(),
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(CreateFileResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn open_write(
        &self,
        request: Request<OpenWriteRequestProto>,
    ) -> Result<Response<OpenWriteResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, OpenWriteResponseProto);
        let mode = match beryl_proto::convert::parse_write_mode(req.mode) {
            Ok(mode) => mode,
            _ => {
                return error_response!(
                    OpenWriteResponseProto,
                    Self::header_from_conversion_error(
                        &req.header,
                        MetadataError::InvalidArgument("OpenWrite mode is required".to_string()),
                    )
                )
            }
        };
        match self
            .filesystem
            .open_write(
                &req_ctx,
                OpenWriteArgs {
                    path: req.path,
                    mode,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => {
                let header = ok_header_from_fs_success(&req_ctx, &success);
                let payload = success.payload;
                response_with_header!(
                    OpenWriteResponseProto {
                        write_handle: Some(
                            WriteHandle {
                                inode_id: payload.inode_id,
                                lease_epoch: payload.lease_epoch
                            }
                            .into()
                        ),
                        base_size: payload.base_size,
                        expires_at_ms: payload.expires_at_ms,
                        layout: Some((&payload.layout).into()),
                        generation: payload.generation.as_raw(),
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(OpenWriteResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    /// Decode the allocation predecessor and return a block only after session
    /// ownership, freshness, and allocation completion have been validated.
    #[instrument(skip_all)]
    async fn allocate_block(
        &self,
        request: Request<AllocateBlockRequestProto>,
    ) -> Result<Response<AllocateBlockResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, AllocateBlockResponseProto);
        let handle = match Self::write_handle_or_error(&req.header, req.write_handle) {
            Ok(handle) => handle,
            Err(header) => return response_with_header!(AllocateBlockResponseProto::default(), *header),
        };
        let previous_block_id = match req.previous_block_id.map(TryInto::try_into).transpose() {
            Ok(previous_block_id) => previous_block_id,
            Err(err) => {
                return error_response!(
                    AllocateBlockResponseProto,
                    Self::header_from_conversion_error(
                        &req.header,
                        MetadataError::InvalidArgument(format!("invalid previous_block_id: {err:?}")),
                    )
                )
            }
        };
        match self
            .filesystem
            .allocate_block(
                &req_ctx,
                AllocateBlockArgs {
                    handle,
                    previous_block_id,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => response_with_header!(
                AllocateBlockResponseProto {
                    block: Some(located_block_to_proto(&success.payload.block)),
                    ..Default::default()
                },
                ok_header_from_fs_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(AllocateBlockResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn commit_file(
        &self,
        request: Request<CommitFileRequestProto>,
    ) -> Result<Response<CommitFileResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, CommitFileResponseProto);
        let handle = match Self::write_handle_or_error(&req.header, req.write_handle) {
            Ok(handle) => handle,
            Err(header) => return response_with_header!(CommitFileResponseProto::default(), *header),
        };
        let publish_mode = match beryl_proto::convert::parse_write_mode(req.write_mode) {
            Ok(WriteMode::Overwrite) => PublishMode::ReplaceIfUnchanged,
            Ok(WriteMode::Append) => PublishMode::AppendIfUnchanged,
            _ => {
                return error_response!(
                    CommitFileResponseProto,
                    Self::header_from_conversion_error(
                        &req.header,
                        MetadataError::InvalidArgument("CommitFile write_mode is required".to_string()),
                    )
                )
            }
        };
        if req.committed_blocks.len() > MAX_FILE_EXTENTS {
            return error_response!(
                CommitFileResponseProto,
                Self::header_from_conversion_error(
                    &req.header,
                    MetadataError::ResourceExhausted(format!(
                        "CommitFile committed block count {} exceeds maximum {}",
                        req.committed_blocks.len(),
                        MAX_FILE_EXTENTS
                    )),
                )
            );
        }
        let mut committed_blocks = Vec::with_capacity(req.committed_blocks.len());
        for block in req.committed_blocks {
            match Self::committed_block_from_proto(block) {
                Ok(committed_block) => committed_blocks.push(committed_block),
                Err(err) => {
                    return error_response!(
                        CommitFileResponseProto,
                        Self::header_from_conversion_error(&req.header, err)
                    )
                }
            }
        }
        match self
            .filesystem
            .commit_file(
                &req_ctx,
                CommitFileArgs {
                    handle,
                    committed_blocks,
                    final_size: req.final_size,
                    freshness: Self::freshness_from_header(&req.header),
                    expected_generation: ContentGeneration::new(req.expected_generation),
                    expected_file_size: req.expected_file_size,
                    publish_mode,
                },
            )
            .await
        {
            Ok(success) => response_with_header!(
                CommitFileResponseProto {
                    committed_size: success.payload.committed_size,
                    ..Default::default()
                },
                ok_header_from_fs_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(CommitFileResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn abort_file_write(
        &self,
        request: Request<AbortFileWriteRequestProto>,
    ) -> Result<Response<AbortFileWriteResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, AbortFileWriteResponseProto);
        let handle = match Self::write_handle_or_error(&req.header, req.write_handle) {
            Ok(handle) => handle,
            Err(header) => return response_with_header!(AbortFileWriteResponseProto::default(), *header),
        };
        match self
            .filesystem
            .abort_file_write(
                &req_ctx,
                AbortFileWriteArgs {
                    handle,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => response_with_header!(
                AbortFileWriteResponseProto::default(),
                ok_header_from_fs_success(&req_ctx, &success)
            ),
            Err(failure) => response_with_header!(
                AbortFileWriteResponseProto::default(),
                header_from_fs_failure(&req_ctx, &failure)
            ),
        }
    }

    #[instrument(skip_all)]
    async fn renew_lease(
        &self,
        request: Request<RenewLeaseRequestProto>,
    ) -> Result<Response<RenewLeaseResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, RenewLeaseResponseProto);
        let handle = match Self::write_handle_or_error(&req.header, req.write_handle) {
            Ok(handle) => handle,
            Err(header) => return response_with_header!(RenewLeaseResponseProto::default(), *header),
        };
        match self
            .filesystem
            .renew_lease(
                &req_ctx,
                RenewLeaseArgs {
                    handle,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => response_with_header!(
                RenewLeaseResponseProto {
                    expires_at_ms: success.payload.expires_at_ms,
                    ..Default::default()
                },
                ok_header_from_fs_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(RenewLeaseResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn sync_write(
        &self,
        request: Request<SyncWriteRequestProto>,
    ) -> Result<Response<SyncWriteResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, SyncWriteResponseProto);
        let handle = match Self::write_handle_or_error(&req.header, req.write_handle) {
            Ok(handle) => handle,
            Err(header) => return response_with_header!(SyncWriteResponseProto::default(), *header),
        };
        let publish_mode = match beryl_proto::convert::parse_write_mode(req.write_mode) {
            Ok(WriteMode::Overwrite) => PublishMode::ReplaceIfUnchanged,
            Ok(WriteMode::Append) => PublishMode::AppendIfUnchanged,
            _ => {
                return error_response!(
                    SyncWriteResponseProto,
                    Self::header_from_conversion_error(
                        &req.header,
                        MetadataError::InvalidArgument("SyncWrite write_mode is required".to_string()),
                    )
                )
            }
        };
        if req.committed_blocks.len() > MAX_FILE_EXTENTS {
            return error_response!(
                SyncWriteResponseProto,
                Self::header_from_conversion_error(
                    &req.header,
                    MetadataError::ResourceExhausted(format!(
                        "SyncWrite committed block count {} exceeds maximum {}",
                        req.committed_blocks.len(),
                        MAX_FILE_EXTENTS
                    )),
                )
            );
        }
        let mut committed_blocks = Vec::with_capacity(req.committed_blocks.len());
        for block in req.committed_blocks {
            match Self::committed_block_from_proto(block) {
                Ok(committed_block) => committed_blocks.push(committed_block),
                Err(err) => {
                    return error_response!(
                        SyncWriteResponseProto,
                        Self::header_from_conversion_error(&req.header, err)
                    )
                }
            }
        }
        match self
            .filesystem
            .sync_write(
                &req_ctx,
                SyncWriteArgs {
                    handle,
                    committed_blocks,
                    target_size: req.target_size,
                    freshness: Self::freshness_from_header(&req.header),
                    expected_generation: ContentGeneration::new(req.expected_generation),
                    expected_file_size: req.expected_file_size,
                    publish_mode,
                },
            )
            .await
        {
            Ok(success) => response_with_header!(
                SyncWriteResponseProto {
                    synced_size: success.payload.synced_size,
                    generation: success.payload.generation.map(ContentGeneration::as_raw),
                    ..Default::default()
                },
                ok_header_from_fs_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(SyncWriteResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    async fn msync(&self, request: Request<MsyncRequestProto>) -> Result<Response<MsyncResponseProto>, Status> {
        let req = request.into_inner();
        let response = match self.msync.as_ref() {
            Some(msync) => msync.handle(req),
            None => MsyncHandler::unavailable(req),
        };
        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{NamespaceListConfig, RaftConfig};
    use crate::mount::{DataIoPolicy, MountEntry, MountKind, MountTable};
    use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
    use crate::service::{MetadataFileSystem, MetadataFileSystemDeps, MetadataFileSystemServiceImpl, MsyncHandler};
    use crate::session_registry::SessionRegistry;
    use crate::state::{RouteEpoch, StateStore};
    use crate::worker::{BlockReportBlock, BlockReportBlockState, WorkerManager};
    use crate::MetadataResult;
    use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction, RpcErrorDetail};
    use beryl_common::header::RequestHeader;
    use beryl_proto::common::{ByteRangeProto, ErrorDetailProto, RequestHeaderProto, ResponseHeaderProto};
    use beryl_proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
    use beryl_proto::metadata::get_block_locations_request_proto::Target;
    use beryl_proto::metadata::{
        AllocateBlockRequestProto, CommitFileRequestProto, CommittedBlockProto, CreateFileRequestProto,
        GetBlockLocationsRequestProto, LocatedBlockProto, OpenWriteModeProto, SyncWriteRequestProto, WriteHandleProto,
    };
    use beryl_types::fs::{FileAttrs, Inode, InodeData};
    use beryl_types::ids::{BlockId, BlockIndex, InodeId, MountId, WorkerId};
    use beryl_types::{ClientId, ContentGeneration, FileLayout, GroupName, LeaseEpoch, Tier, TierFree, WorkerRunId};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tonic::Request;

    struct PathTestEnv {
        _temp_dir: TempDir,
        storage: Arc<RocksDBStorage>,
        service: MetadataFileSystemServiceImpl,
        session_registry: Arc<SessionRegistry>,
        worker_manager: Option<Arc<WorkerManager>>,
    }

    struct TestStateStore {
        route_epoch: RouteEpoch,
    }

    impl TestStateStore {
        fn new() -> Self {
            Self {
                route_epoch: RouteEpoch::new(1),
            }
        }
    }

    #[async_trait::async_trait]
    impl StateStore for TestStateStore {
        async fn get_route_epoch(&self) -> MetadataResult<RouteEpoch> {
            Ok(self.route_epoch)
        }
    }

    fn header(client_id: u128) -> Option<RequestHeaderProto> {
        Some((&RequestHeader::new(ClientId::new(client_id))).into())
    }

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    fn publish_mount(table: &MountTable, entry: MountEntry) -> MountEntry {
        table.upsert(entry.clone()).expect("publish mount");
        entry
    }

    fn set_test_inode_allocator_after_current_max(storage: &RocksDBStorage) {
        let max_inode_id = storage
            .max_inode_id()
            .expect("read maximum inode ID")
            .expect("test namespace must contain an inode");
        let next_inode_id = max_inode_id
            .as_raw()
            .checked_add(1)
            .map(InodeId::new)
            .expect("test inode ID must have a successor");
        storage
            .set_next_inode_id(next_inode_id)
            .expect("initialize test inode allocator");
    }

    fn header_error(response_header: Option<ResponseHeaderProto>) -> ErrorDetailProto {
        response_header
            .expect("response header must exist")
            .error
            .expect("header.error must exist")
    }

    fn assert_success_header(response_header: Option<ResponseHeaderProto>) {
        assert!(
            response_header.expect("response header must exist").error.is_none(),
            "response header must not contain a business error"
        );
    }

    fn rpc_error(err: &ErrorDetailProto) -> RpcErrorDetail {
        beryl_proto::convert::rpc_error_from_proto(err)
    }

    fn assert_fail_kind(err: &ErrorDetailProto, expected: ErrorKind) -> RpcErrorDetail {
        let rpc_error = rpc_error(err);
        assert_eq!(rpc_error.kind, expected, "{rpc_error:?}");
        assert!(matches!(rpc_error.recovery, RecoveryAction::Fail), "{rpc_error:?}");
        rpc_error
    }

    async fn write_env() -> PathTestEnv {
        let worker_manager = Some(worker_manager_for_write_targets());
        let root_inode_id = InodeId::new(1000);
        let temp_dir = TempDir::new().expect("create temp dir");
        let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path()).expect("open rocksdb"));
        let mount_table = Arc::new(MountTable::new());

        let mount_entry = publish_mount(
            &mount_table,
            MountEntry {
                mount_id: MountId::new(1),
                mount_prefix: "/mnt/test".to_string(),
                mount_kind: MountKind::External,
                ufs_uri: Some("file:///tmp_mnt_test".to_string()),
                data_io_policy: DataIoPolicy::Allow,
                mount_epoch: 1,
                namespace_owner_group_name: group_name("root"),
                root_inode_id,
            },
        );

        let mut root_attrs = FileAttrs::new();
        root_attrs.uid = 1000;
        root_attrs.gid = 1000;
        root_attrs.mode = 0o755;
        storage
            .put_inode(&Inode::new_dir(root_inode_id, root_attrs, mount_entry.mount_id))
            .expect("put root inode");
        set_test_inode_allocator_after_current_max(&storage);
        storage.put_mount(&mount_entry).expect("put authoritative mount");

        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_config = RaftConfig::default();
        let raft_node = Arc::new(
            AppRaftNode::new(
                1,
                Arc::clone(&storage),
                state_machine,
                Arc::clone(&mount_table),
                &raft_config,
            )
            .await
            .expect("create raft node"),
        );
        raft_node
            .initialize_single_node("127.0.0.1:0".to_string())
            .await
            .expect("initialize single-node raft");
        for _ in 0..50 {
            if raft_node.is_leader() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(raft_node.is_leader(), "single-node raft must become leader");

        let state_store: Arc<dyn StateStore> = Arc::new(TestStateStore::new());
        let session_registry = Arc::new(SessionRegistry::default());
        let owner_group_name = group_name("root");
        let filesystem = Arc::new(MetadataFileSystem::new(MetadataFileSystemDeps {
            state_store,
            mount_table: Arc::clone(&mount_table),
            storage: Arc::clone(&storage),
            raft_node: Some(Arc::clone(&raft_node)),
            session_registry: Arc::clone(&session_registry),
            worker_manager: worker_manager.clone(),
            metrics: None,
            readiness_gate: None,
            file_create_layout: FileLayout::new(128),
        }));
        let msync = Some(MsyncHandler::new(Arc::clone(&raft_node), owner_group_name));
        let service = MetadataFileSystemServiceImpl::new(filesystem, msync, NamespaceListConfig::default());

        PathTestEnv {
            _temp_dir: temp_dir,
            storage,
            service,
            session_registry,
            worker_manager,
        }
    }

    fn worker_manager_for_write_targets() -> Arc<WorkerManager> {
        let manager = Arc::new(WorkerManager::new(60_000));
        for raw in 1..=3 {
            let worker_id = WorkerId::new(raw);
            let endpoint = format!("127.0.0.1:{}", 9000 + raw);
            let worker_run_id: WorkerRunId = format!("550e8400-e29b-41d4-a716-{raw:012x}")
                .parse()
                .expect("valid test worker run id");
            manager
                .register_worker_run(&group_name("root"), worker_id, endpoint.clone(), 1, worker_run_id, None)
                .expect("register worker run");
            manager
                .record_heartbeat_with_tier_free(
                    &group_name("root"),
                    worker_id,
                    worker_run_id,
                    1,
                    &endpoint,
                    1,
                    vec![TierFree {
                        tier: Tier::Hdd,
                        free_bytes: 1024 * 1024,
                    }],
                )
                .expect("record worker heartbeat");
        }
        manager
    }

    fn publish_reported_location(
        env: &PathTestEnv,
        worker_id: WorkerId,
        block_id: BlockId,
        block_stamp: u64,
        effective_len: u64,
    ) {
        publish_reported_locations(env, worker_id, vec![(block_id, block_stamp, effective_len)]);
    }

    fn publish_reported_locations(env: &PathTestEnv, worker_id: WorkerId, blocks: Vec<(BlockId, u64, u64)>) {
        let worker_manager = env.worker_manager.as_ref().expect("worker manager");
        let worker_run_id = worker_manager
            .get_registration(&group_name("root"), worker_id)
            .expect("worker registration")
            .worker_run_id;
        worker_manager
            .receive_full_block_report(
                &group_name("root"),
                worker_id,
                worker_run_id,
                1,
                0,
                true,
                blocks
                    .into_iter()
                    .map(|(block_id, block_stamp, effective_len)| BlockReportBlock {
                        block_id,
                        block_stamp,
                        block_state: BlockReportBlockState::Ready,
                        effective_len,
                    })
                    .collect(),
            )
            .expect("full block report should publish location");
    }

    fn publish_target_reports(env: &PathTestEnv, targets: &[&LocatedBlockProto]) {
        let mut by_worker = HashMap::<WorkerId, Vec<(BlockId, u64, u64)>>::new();
        for target in targets {
            let worker_id = WorkerId::new(
                target
                    .worker_endpoints
                    .first()
                    .expect("target worker endpoint")
                    .worker_id,
            );
            let block_id = target.block_id.as_ref().expect("target block id");
            by_worker.entry(worker_id).or_default().push((
                BlockId::new(InodeId::new(block_id.inode_id), BlockIndex::new(block_id.block_index)),
                target.block_stamp,
                target.block_size,
            ));
        }
        for (worker_id, blocks) in by_worker {
            publish_reported_locations(env, worker_id, blocks);
        }
    }

    async fn open_write_session_with_committed_block(
        env: &PathTestEnv,
        path: &str,
        client_id: u128,
    ) -> (WriteHandleProto, CommittedBlockProto, u64, i32) {
        let create = FileSystemServiceProto::create_file(
            &env.service,
            Request::new(CreateFileRequestProto {
                header: header(client_id),
                path: path.to_string(),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(create.header);

        let expected_generation = create.generation;
        let write_mode = OpenWriteModeProto::OpenWriteModeWrite as i32;
        let write_handle = create.write_handle.expect("write handle");
        let target = FileSystemServiceProto::allocate_block(
            &env.service,
            Request::new(AllocateBlockRequestProto {
                header: header(client_id),
                write_handle: Some(write_handle),
                previous_block_id: None,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner()
        .block
        .expect("write target");
        let reported_block_id = target.block_id.as_ref().expect("target block id");
        let reported_worker_id = WorkerId::new(
            target
                .worker_endpoints
                .first()
                .expect("target worker endpoint")
                .worker_id,
        );
        publish_reported_location(
            env,
            reported_worker_id,
            BlockId::new(
                InodeId::new(reported_block_id.inode_id),
                BlockIndex::new(reported_block_id.block_index),
            ),
            target.block_stamp,
            128,
        );
        let committed = CommittedBlockProto {
            block_id: target.block_id,
            file_offset: target.file_offset,
            len: 128,
        };

        (write_handle, committed, expected_generation, write_mode)
    }

    #[tokio::test]
    async fn sync_write_resolves_equivalent_published_state_after_session_cleanup() {
        let env = write_env().await;
        let (write_handle, committed, expected_generation, write_mode) =
            open_write_session_with_committed_block(&env, "/mnt/test/sync-completed", 51).await;
        let request = SyncWriteRequestProto {
            header: header(51),
            write_handle: Some(write_handle),
            committed_blocks: vec![committed],
            target_size: 128,
            expected_generation,
            write_mode,
            expected_file_size: 0,
        };

        let first = FileSystemServiceProto::sync_write(&env.service, Request::new(request.clone()))
            .await
            .expect("transport status must remain OK")
            .into_inner();
        assert_success_header(first.header);
        let first_generation = first.generation.expect("content generation");
        let inode_id = InodeId::new(write_handle.inode_id);
        env.session_registry
            .remove_session_if_epoch(inode_id, LeaseEpoch::new(write_handle.write_lease_epoch))
            .expect("remove session to model cleanup or restart");

        let replay = FileSystemServiceProto::sync_write(&env.service, Request::new(request.clone()))
            .await
            .expect("transport status must remain OK")
            .into_inner();
        assert_success_header(replay.header);
        assert_eq!(replay.synced_size, first.synced_size);
        assert_eq!(replay.generation, Some(first_generation));

        let mut changed_payload = request;
        changed_payload.committed_blocks.clear();
        let changed = FileSystemServiceProto::sync_write(&env.service, Request::new(changed_payload))
            .await
            .expect("transport status must remain OK")
            .into_inner();
        let err = header_error(changed.header);
        assert_fail_kind(&err, ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument));
        assert!(err.message.contains("completed publish payload"));
    }

    #[tokio::test]
    async fn completed_publish_recovery_cannot_bypass_active_session_owner() {
        let env = write_env().await;
        let owner_client_id = 53;
        let foreign_client_id = 54;
        let (write_handle, committed, expected_generation, write_mode) =
            open_write_session_with_committed_block(&env, "/mnt/test/owned-completed-publish", owner_client_id).await;
        let owner_sync = SyncWriteRequestProto {
            header: header(owner_client_id),
            write_handle: Some(write_handle),
            committed_blocks: vec![committed],
            target_size: 128,
            expected_generation,
            write_mode,
            expected_file_size: 0,
        };
        let first = FileSystemServiceProto::sync_write(&env.service, Request::new(owner_sync.clone()))
            .await
            .expect("transport status must remain OK")
            .into_inner();
        assert_success_header(first.header);
        let inode_id = InodeId::new(write_handle.inode_id);

        let mut foreign_sync = owner_sync;
        foreign_sync.header = header(foreign_client_id);
        let rejected_sync = FileSystemServiceProto::sync_write(&env.service, Request::new(foreign_sync))
            .await
            .expect("transport status must remain OK")
            .into_inner();
        assert_eq!(
            rpc_error(&header_error(rejected_sync.header)).kind,
            ErrorKind::Metadata(MetadataErrorKind::SessionInvalid)
        );
        assert!(env.session_registry.get_session(inode_id).is_some());

        let foreign_commit = CommitFileRequestProto {
            header: header(foreign_client_id),
            write_handle: Some(write_handle),
            committed_blocks: vec![committed],
            final_size: 128,
            expected_generation,
            write_mode,
            expected_file_size: 0,
        };
        let rejected_commit = FileSystemServiceProto::commit_file(&env.service, Request::new(foreign_commit))
            .await
            .expect("transport status must remain OK")
            .into_inner();
        assert_eq!(
            rpc_error(&header_error(rejected_commit.header)).kind,
            ErrorKind::Metadata(MetadataErrorKind::SessionInvalid)
        );
        assert!(env.session_registry.get_session(inode_id).is_some());

        let owner_commit = FileSystemServiceProto::commit_file(
            &env.service,
            Request::new(CommitFileRequestProto {
                header: header(owner_client_id),
                write_handle: Some(write_handle),
                committed_blocks: vec![committed],
                final_size: 128,
                expected_generation,
                write_mode,
                expected_file_size: 0,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(owner_commit.header);
        assert!(env.session_registry.get_session(inode_id).is_none());
    }

    #[tokio::test]
    async fn completed_publish_recovery_uses_state_equivalence_not_request_history() {
        let env = write_env().await;
        let client_id = 52;
        let path = "/mnt/test/state-equivalent-publish";
        let create = FileSystemServiceProto::create_file(
            &env.service,
            Request::new(CreateFileRequestProto {
                header: header(client_id),
                path: path.to_string(),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(create.header);

        let write_handle = create.write_handle.expect("write handle");

        let first_target = FileSystemServiceProto::allocate_block(
            &env.service,
            Request::new(AllocateBlockRequestProto {
                header: header(client_id),
                write_handle: Some(write_handle),
                previous_block_id: None,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner()
        .block
        .expect("first target");
        let second_target = FileSystemServiceProto::allocate_block(
            &env.service,
            Request::new(AllocateBlockRequestProto {
                header: header(client_id),
                write_handle: Some(write_handle),
                previous_block_id: first_target.block_id,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner()
        .block
        .expect("second target");
        let first = CommittedBlockProto {
            block_id: first_target.block_id,
            file_offset: first_target.file_offset,
            len: first_target.block_size,
        };
        let second = CommittedBlockProto {
            block_id: second_target.block_id,
            file_offset: second_target.file_offset,
            len: second_target.block_size,
        };
        publish_target_reports(&env, &[&first_target, &second_target]);

        let published = FileSystemServiceProto::commit_file(
            &env.service,
            Request::new(CommitFileRequestProto {
                header: header(client_id),
                write_handle: Some(write_handle),
                committed_blocks: vec![first, second],
                final_size: 256,
                expected_generation: create.generation,
                write_mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
                expected_file_size: 0,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(published.header);

        let equivalent = FileSystemServiceProto::commit_file(
            &env.service,
            Request::new(CommitFileRequestProto {
                header: header(client_id),
                write_handle: Some(write_handle),
                committed_blocks: vec![second],
                final_size: 256,
                expected_generation: create.generation,
                write_mode: OpenWriteModeProto::OpenWriteModeAppend as i32,
                expected_file_size: 128,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(equivalent.header);
        assert_eq!(equivalent.committed_size, published.committed_size);

        let non_equivalent = FileSystemServiceProto::commit_file(
            &env.service,
            Request::new(CommitFileRequestProto {
                header: header(client_id),
                write_handle: Some(write_handle),
                committed_blocks: vec![second],
                final_size: 256,
                expected_generation: create.generation,
                write_mode: OpenWriteModeProto::OpenWriteModeAppend as i32,
                expected_file_size: 64,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        let err = header_error(non_equivalent.header);
        assert_fail_kind(&err, ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument));
        assert!(err.message.contains("contiguous"));
    }

    #[tokio::test]
    async fn commit_file_resolves_completed_publish_without_a_session() {
        let env = write_env().await;

        let (write_handle, committed, expected_generation, write_mode) =
            open_write_session_with_committed_block(&env, "/mnt/test/replay-file", 30).await;
        let inode_id = write_handle.inode_id;
        assert_ne!(inode_id, 0);
        let file_inode_id = InodeId::new(inode_id);
        let session_inode_id = file_inode_id;
        assert!(env.session_registry.get_session(session_inode_id).is_some());
        let typed_block_id = BlockId::try_from(committed.block_id.unwrap()).unwrap();
        let request = CommitFileRequestProto {
            header: header(30),
            write_handle: Some(write_handle),
            committed_blocks: vec![committed],
            final_size: 128,
            expected_generation,
            write_mode,
            expected_file_size: 0,
        };

        let first = FileSystemServiceProto::commit_file(&env.service, Request::new(request.clone()))
            .await
            .expect("transport status must remain OK")
            .into_inner();
        assert_success_header(first.header);
        assert_eq!(first.committed_size, 128);
        let first_generation = match env
            .storage
            .get_inode(file_inode_id)
            .unwrap()
            .expect("committed inode")
            .data
        {
            InodeData::File {
                generation: Some(generation),
                ..
            } => generation.as_raw(),
            other => panic!("expected committed file inode data, got {:?}", other),
        };
        assert_ne!(first_generation, 0);
        assert!(env.session_registry.get_session(session_inode_id).is_none());
        assert_eq!(first_generation, expected_generation + 1);

        let locations = FileSystemServiceProto::get_block_locations(
            &env.service,
            Request::new(GetBlockLocationsRequestProto {
                header: header(33),
                target: Some(Target::InodeId(inode_id)),
                range: Some(ByteRangeProto { offset: 0, len: 128 }),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(locations.header);
        assert_eq!(locations.generation, Some(first_generation));
        assert_eq!(locations.locations.len(), 1);
        assert_eq!(locations.locations[0].block_stamp, Some(first_generation));

        env.worker_manager
            .as_ref()
            .expect("worker manager")
            .reset_worker_soft_state();
        let second = FileSystemServiceProto::commit_file(&env.service, Request::new(request.clone()))
            .await
            .expect("transport status must remain OK")
            .into_inner();
        assert_success_header(second.header);
        assert_eq!(second.committed_size, first.committed_size);

        let inode = env.storage.get_inode(file_inode_id).unwrap().expect("committed inode");
        assert_eq!(inode.attrs.size, 128);
        match &inode.data {
            InodeData::File {
                extents, generation, ..
            } => {
                assert_eq!(*generation, Some(ContentGeneration::new(first_generation)));
                assert_eq!(extents.len(), 1);
                assert_eq!(extents[0].block_id, typed_block_id);
                assert_eq!(extents[0].len, 128);
                assert_eq!(extents[0].block_stamp, Some(first_generation));
            }
            other => panic!("expected file inode data, got {:?}", other),
        }

        let mismatch = FileSystemServiceProto::commit_file(
            &env.service,
            Request::new(CommitFileRequestProto {
                final_size: 129,
                ..request
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        let err = header_error(mismatch.header);
        assert_fail_kind(&err, ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument));
        assert!(err.message.contains("completed publish payload ends"));
        let after_mismatch = env
            .storage
            .get_inode(file_inode_id)
            .unwrap()
            .expect("inode after mismatch");
        assert_eq!(after_mismatch, inode);
    }
}
