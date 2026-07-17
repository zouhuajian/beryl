// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! FileSystemServiceProto implementation.
//!
//! Handlers convert wire values, invoke one path-first `MetadataFileSystem`
//! operation, and map its result back to the wire response.

use super::filesystem::{
    AbortFileWriteArgs, AddBlockArgs, BlockLocationsTarget, CommitFileArgs, CreateDirectoryArgs, CreateFileArgs,
    CreateFileMode, DeleteArgs, FileRange, Freshness, GetBlockLocationsArgs, GetStatusArgs, ListStatusArgs,
    OpenFileArgs, OpenWriteArgs, PresentedWriteHandle, RenameArgs, RenewLeaseArgs, SessionKey, SyncWriteArgs,
    SyncWriteMode,
};
use super::wire::{
    fencing_to_proto, file_attrs_from_proto, file_attrs_to_proto, file_layout_from_proto, header_from_fs_failure,
    header_from_rpc_error, lease_id_from_proto, lease_id_to_proto, location_to_proto, ok_header_from_fs_success,
    presented_fencing_from_proto, request_context_from_proto, write_target_to_proto,
};
use super::MetadataFileSystem;
use super::MsyncHandler;
use crate::error::{to_fs_error_detail, MetadataError};
use beryl_proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use beryl_proto::metadata::*;
use beryl_types::ids::DataHandleId;
use beryl_types::CommittedBlock;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::instrument;

trait HeaderResponse {
    fn with_header(self, header: beryl_proto::common::ResponseHeaderProto) -> Self;
}

macro_rules! impl_header_response {
    ($($resp_ty:ty),+ $(,)?) => {
        $(
            impl HeaderResponse for $resp_ty {
                fn with_header(mut self, header: beryl_proto::common::ResponseHeaderProto) -> Self {
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
    AddBlockResponseProto,
    CommitFileResponseProto,
    AbortFileWriteResponseProto,
    RenewLeaseResponseProto,
    SyncWriteResponseProto,
    MsyncResponseProto,
);

/// FileSystemServiceProto implementation.
pub struct MetadataFileSystemServiceImpl {
    filesystem: Arc<MetadataFileSystem>,
    msync: Option<MsyncHandler>,
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
    pub(crate) fn new(filesystem: Arc<MetadataFileSystem>, msync: Option<MsyncHandler>) -> Self {
        Self { filesystem, msync }
    }

    fn header_from_conversion_error(
        req_header: &Option<beryl_proto::common::RequestHeaderProto>,
        err: MetadataError,
    ) -> beryl_proto::common::ResponseHeaderProto {
        let rpc_error = to_fs_error_detail(err);
        header_from_rpc_error(req_header, None, None, &rpc_error)
    }

    fn freshness_from_header(header: &Option<beryl_proto::common::RequestHeaderProto>) -> Freshness {
        Freshness {
            mount_epoch: header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: header.as_ref().and_then(|h| h.route_epoch),
        }
    }

    fn write_handle_from_key(key: &SessionKey) -> WriteHandleProto {
        WriteHandleProto {
            handle_id: key.file_handle,
            lease_id: Some(lease_id_to_proto(key.lease_id)),
            lease_epoch: key.lease_epoch,
            open_epoch: key.open_epoch,
            fencing_token: Some(fencing_to_proto(key.fencing_token)),
        }
    }

    fn write_handle_or_error(
        header: &Option<beryl_proto::common::RequestHeaderProto>,
        handle: Option<WriteHandleProto>,
    ) -> Result<WriteHandleProto, Box<beryl_proto::common::ResponseHeaderProto>> {
        handle.ok_or_else(|| {
            Box::new(header_from_rpc_error(
                header,
                None,
                None,
                &to_fs_error_detail(MetadataError::InvalidArgument("missing write_handle".to_string())),
            ))
        })
    }

    fn presented_write_handle(handle: WriteHandleProto) -> PresentedWriteHandle {
        PresentedWriteHandle {
            file_handle: handle.handle_id,
            lease_id: lease_id_from_proto(handle.lease_id),
            lease_epoch: handle.lease_epoch,
            open_epoch: handle.open_epoch,
            fencing_token: presented_fencing_from_proto(handle.fencing_token),
        }
    }

    fn committed_block_from_proto(block: CommittedBlockProto) -> Result<CommittedBlock, MetadataError> {
        CommittedBlock::try_from(block).map_err(MetadataError::InvalidArgument)
    }

    fn data_handle_proto(data_handle_id: DataHandleId) -> beryl_proto::common::DataHandleIdProto {
        data_handle_id.into()
    }
}

#[tonic::async_trait]
impl FileSystemServiceProto for MetadataFileSystemServiceImpl {
    async fn msync(&self, request: Request<MsyncRequestProto>) -> Result<Response<MsyncResponseProto>, Status> {
        let req = request.into_inner();
        let response = match self.msync.as_ref() {
            Some(msync) => msync.handle(req),
            None => MsyncHandler::unavailable(req),
        };
        Ok(Response::new(response))
    }

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
                    ..Default::default()
                },
                ok_header_from_fs_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(GetStatusResponseProto, header_from_fs_failure(&req_ctx, &failure)),
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

    #[instrument(skip_all)]
    async fn delete(&self, request: Request<DeleteRequestProto>) -> Result<Response<DeleteResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, DeleteResponseProto);
        match self
            .filesystem
            .delete(
                &req_ctx,
                DeleteArgs {
                    path: req.path,
                    recursive: req.recursive,
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
    async fn list_status(
        &self,
        request: Request<ListStatusRequestProto>,
    ) -> Result<Response<ListStatusResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, ListStatusResponseProto);
        match self
            .filesystem
            .list_status(
                &req_ctx,
                ListStatusArgs {
                    path: req.path,
                    recursive: req.recursive,
                    cursor_key: (!req.cursor.is_empty()).then_some(req.cursor),
                    max_entries: (req.limit != 0).then_some(req.limit as usize),
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
                    .map(|entry| beryl_proto::fs::DirEntryProto {
                        name: entry.name,
                        kind: match entry.kind {
                            Some(beryl_types::fs::InodeKind::File) => {
                                beryl_proto::fs::InodeKindProto::InodeKindFile as i32
                            }
                            Some(beryl_types::fs::InodeKind::Dir) => {
                                beryl_proto::fs::InodeKindProto::InodeKindDir as i32
                            }
                            Some(beryl_types::fs::InodeKind::Symlink) => {
                                beryl_proto::fs::InodeKindProto::InodeKindSymlink as i32
                            }
                            None => beryl_proto::fs::InodeKindProto::InodeKindUnspecified as i32,
                        },
                        attrs: entry.attrs.as_ref().map(file_attrs_to_proto),
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
                        data_handle_id: Some(Self::data_handle_proto(payload.data_handle_id)),
                        file_size: payload.file_size,
                        file_version: payload.file_version,
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
            Some(get_block_locations_request_proto::Target::Path(path)) => BlockLocationsTarget::Path(path),
            Some(get_block_locations_request_proto::Target::DataHandleId(data_handle)) => {
                BlockLocationsTarget::DataHandle(
                    DataHandleId::try_from(data_handle)
                        .unwrap_or_else(|()| unreachable!("DataHandleIdProto conversion is infallible")),
                )
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
                        data_handle_id: Some(Self::data_handle_proto(payload.data_handle_id)),
                        file_size: payload.file_size,
                        file_version: payload.file_version,
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
        let mode = match CreateModeProto::try_from(req.create_mode) {
            Ok(CreateModeProto::CreateNew) => Ok(CreateFileMode::CreateNew),
            Ok(CreateModeProto::CreateOrOverwrite) => Ok(CreateFileMode::CreateOrOverwrite),
            _ => Err(MetadataError::InvalidArgument("create mode is required".to_string())),
        };

        match self
            .filesystem
            .create_file(
                &req_ctx,
                CreateFileArgs {
                    path: req.path,
                    parsed_attrs: file_attrs_from_proto(req.attrs),
                    parsed_layout: file_layout_from_proto(req.layout),
                    parsed_mode: mode,
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
                        data_handle_id: Some(Self::data_handle_proto(payload.data_handle_id)),
                        inode_id: Some(beryl_proto::fs::InodeIdProto {
                            value: payload.inode_id.as_raw()
                        }),
                        file_size: payload.file_size,
                        layout: Some((&payload.layout).into()),
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
        let mode = match OpenWriteModeProto::try_from(req.mode) {
            Ok(OpenWriteModeProto::OpenWriteModeWrite) => crate::inode_lease::WriteMode::Write,
            Ok(OpenWriteModeProto::OpenWriteModeAppend) => crate::inode_lease::WriteMode::Append,
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
                    desired_len: req.desired_len,
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
                        write_handle: Some(Self::write_handle_from_key(&payload.session_key)),
                        data_handle_id: Some(Self::data_handle_proto(payload.data_handle_id)),
                        base_size: payload.base_size,
                        expires_at_ms: payload.expires_at_ms,
                        layout: Some((&payload.layout).into()),
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(OpenWriteResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn add_block(
        &self,
        request: Request<AddBlockRequestProto>,
    ) -> Result<Response<AddBlockResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, AddBlockResponseProto);
        let handle = match Self::write_handle_or_error(&req.header, req.write_handle) {
            Ok(handle) => Self::presented_write_handle(handle),
            Err(header) => return response_with_header!(AddBlockResponseProto::default(), *header),
        };
        let previous_block_id = match req.previous_block_id.map(TryInto::try_into).transpose() {
            Ok(previous_block_id) => previous_block_id,
            Err(err) => {
                return error_response!(
                    AddBlockResponseProto,
                    Self::header_from_conversion_error(
                        &req.header,
                        MetadataError::InvalidArgument(format!("invalid previous_block_id: {err:?}")),
                    )
                )
            }
        };
        match self
            .filesystem
            .add_block(
                &req_ctx,
                AddBlockArgs {
                    handle,
                    desired_len: req.desired_len,
                    previous_block_id,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => response_with_header!(
                AddBlockResponseProto {
                    target: Some(write_target_to_proto(&success.payload.target)),
                    ..Default::default()
                },
                ok_header_from_fs_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(AddBlockResponseProto, header_from_fs_failure(&req_ctx, &failure)),
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
            Ok(handle) => Self::presented_write_handle(handle),
            Err(header) => return response_with_header!(CommitFileResponseProto::default(), *header),
        };
        let data_handle_id = match req.data_handle_id {
            Some(data_handle_id) => DataHandleId::new(data_handle_id.value),
            None => {
                return error_response!(
                    CommitFileResponseProto,
                    Self::header_from_conversion_error(
                        &req.header,
                        MetadataError::InvalidArgument("missing data_handle_id".to_string()),
                    )
                )
            }
        };
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
                    data_handle_id,
                    committed_blocks,
                    final_size: req.final_size,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => response_with_header!(
                CommitFileResponseProto {
                    committed_size: success.payload.committed_size,
                    file_version: success.payload.file_version,
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
            Ok(handle) => Self::presented_write_handle(handle),
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
            Ok(handle) => Self::presented_write_handle(handle),
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
            Ok(handle) => Self::presented_write_handle(handle),
            Err(header) => return response_with_header!(SyncWriteResponseProto::default(), *header),
        };
        let data_handle_id = match req.data_handle_id {
            Some(data_handle_id) => DataHandleId::new(data_handle_id.value),
            None => {
                return error_response!(
                    SyncWriteResponseProto,
                    Self::header_from_conversion_error(
                        &req.header,
                        MetadataError::InvalidArgument("missing data_handle_id".to_string()),
                    )
                )
            }
        };
        let mode = match WriteSyncModeProto::try_from(req.mode) {
            Ok(WriteSyncModeProto::WriteSyncModeVisibility) => SyncWriteMode::Visibility,
            Ok(WriteSyncModeProto::WriteSyncModeDurability) => SyncWriteMode::Durability,
            Ok(WriteSyncModeProto::WriteSyncModeUnspecified) | Err(_) => {
                return error_response!(
                    SyncWriteResponseProto,
                    Self::header_from_conversion_error(
                        &req.header,
                        MetadataError::InvalidArgument("SyncWrite mode must be visibility or durability".to_string()),
                    )
                )
            }
        };
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
                    data_handle_id,
                    committed_blocks,
                    target_size: req.target_size,
                    flags: req.flags,
                    mode,
                    freshness: Self::freshness_from_header(&req.header),
                },
            )
            .await
        {
            Ok(success) => response_with_header!(
                SyncWriteResponseProto {
                    synced_size: success.payload.synced_size,
                    file_version: success.payload.file_version,
                    ..Default::default()
                },
                ok_header_from_fs_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(SyncWriteResponseProto, header_from_fs_failure(&req_ctx, &failure)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::RaftConfig;
    use crate::mount::{DataIoPolicy, MountKind, MountTable, ROOT_INODE_ID};
    use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
    use crate::readiness::RootReadinessGate;
    use crate::service::{MetadataFileSystem, MetadataFileSystemDeps, MetadataFileSystemServiceImpl, MsyncHandler};
    use crate::state::RouteEpoch;
    use crate::worker::{BlockReportBlock, BlockReportBlockState, HealthStatus, WorkerManager};
    use beryl_common::error::rpc::{
        ErrorKind, InternalErrorKind, MetadataErrorKind, RecoveryAction, RefreshHint, RpcErrorDetail,
    };
    use beryl_common::header::RequestHeader;
    use beryl_proto::common::{
        DataHandleIdProto, FsErrnoProto, GroupStateWatermarkProto, RaftLogIdProto, RequestHeaderProto,
        ResponseHeaderProto,
    };
    use beryl_proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
    use beryl_proto::metadata::{
        get_block_locations_request_proto, AddBlockRequestProto, CommitFileRequestProto, CommittedBlockProto,
        CreateDirectoryRequestProto, CreateFileRequestProto, CreateModeProto, DeleteRequestProto,
        GetBlockLocationsRequestProto, GetStatusRequestProto, ListStatusRequestProto, OpenWriteModeProto,
        OpenWriteRequestProto, RenameRequestProto, SyncWriteRequestProto, WriteHandleProto, WriteSyncModeProto,
    };
    use beryl_types::fs::{Extent, FileAttrs, FsErrorCode, Inode, InodeId};
    use beryl_types::ids::{BlockId, BlockIndex, DataHandleId, WorkerId};
    use beryl_types::layout::FileLayout;
    use beryl_types::{ClientId, GroupName, RaftLogId, WorkerRunId};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tonic::Request;

    const TEST_GROUP_NAME: &str = "root";

    struct PathTestEnv {
        _temp_dir: TempDir,
        storage: Arc<RocksDBStorage>,
        mount_table: Arc<MountTable>,
        raft_node: Option<Arc<AppRaftNode>>,
        service: MetadataFileSystemServiceImpl,
        session_registry: Arc<crate::session_registry::SessionRegistry>,
        worker_manager: Option<Arc<WorkerManager>>,
        mount_id: beryl_types::ids::MountId,
        root_inode_id: InodeId,
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
    impl crate::state::StateStore for TestStateStore {
        async fn get_route_epoch(&self) -> crate::MetadataResult<RouteEpoch> {
            Ok(self.route_epoch)
        }
    }

    fn header(client_id: u128) -> Option<RequestHeaderProto> {
        Some((&RequestHeader::new(ClientId::new(client_id))).into())
    }

    fn header_with_freshness(
        client_id: u128,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        state: Vec<GroupStateWatermarkProto>,
    ) -> Option<RequestHeaderProto> {
        let mut request_header = header(client_id).expect("request header");
        request_header.group_name = TEST_GROUP_NAME.to_string();
        request_header.mount_epoch = mount_epoch;
        request_header.route_epoch = route_epoch;
        request_header.state = state;
        Some(request_header)
    }

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    fn watermark_proto(group_name: &str, state_id: RaftLogId) -> GroupStateWatermarkProto {
        GroupStateWatermarkProto {
            group_name: group_name.to_string(),
            state_id: Some(RaftLogIdProto {
                term: state_id.term,
                leader_node_id: state_id.leader_node_id,
                index: state_id.index,
            }),
        }
    }

    fn header_error(response_header: Option<ResponseHeaderProto>) -> beryl_proto::common::ErrorDetailProto {
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

    fn assert_state_id(actual: &RaftLogIdProto, expected: RaftLogId) {
        assert_eq!(actual.term, expected.term);
        assert_eq!(actual.leader_node_id, expected.leader_node_id);
        assert_eq!(actual.index, expected.index);
    }

    fn rpc_error(err: &beryl_proto::common::ErrorDetailProto) -> RpcErrorDetail {
        beryl_proto::convert::rpc_error_from_proto(err)
    }

    fn fs_errno_to_error_kind(errno: FsErrnoProto) -> ErrorKind {
        let errno = match errno {
            FsErrnoProto::FsErrnoEnoent => FsErrorCode::ENoEnt,
            FsErrnoProto::FsErrnoEexist => FsErrorCode::EExist,
            FsErrnoProto::FsErrnoEnotempty => FsErrorCode::ENotEmpty,
            FsErrnoProto::FsErrnoEnotdir => FsErrorCode::ENotDir,
            FsErrnoProto::FsErrnoEisdir => FsErrorCode::EIsDir,
            FsErrnoProto::FsErrnoExdev => FsErrorCode::EXDev,
            FsErrnoProto::FsErrnoEperm => FsErrorCode::EPerm,
            FsErrnoProto::FsErrnoEacces => FsErrorCode::EAcces,
            FsErrnoProto::FsErrnoEinval => FsErrorCode::EInval,
            FsErrnoProto::FsErrnoEnotsup => FsErrorCode::ENotsup,
            FsErrnoProto::FsErrnoEnotimpl => FsErrorCode::ENotImpl,
            FsErrnoProto::FsErrnoEagain => FsErrorCode::EAgain,
            FsErrnoProto::FsErrnoEbusy => FsErrorCode::EBusy,
            FsErrnoProto::FsErrnoOk => FsErrorCode::Ok,
        };
        ErrorKind::Fs(errno)
    }

    fn assert_fail_kind(err: &beryl_proto::common::ErrorDetailProto, expected: ErrorKind) -> RpcErrorDetail {
        let rpc_error = rpc_error(err);
        assert_eq!(rpc_error.kind, expected, "{rpc_error:?}");
        assert!(matches!(rpc_error.recovery, RecoveryAction::Fail), "{rpc_error:?}");
        rpc_error
    }

    fn assert_fs_errno(err: &beryl_proto::common::ErrorDetailProto, expected: FsErrnoProto) {
        assert_fail_kind(err, fs_errno_to_error_kind(expected));
    }

    fn assert_not_leader(err: &beryl_proto::common::ErrorDetailProto) {
        assert_refresh_metadata(err, ErrorKind::Metadata(MetadataErrorKind::NotLeader));
    }

    fn assert_refresh_metadata(err: &beryl_proto::common::ErrorDetailProto, expected: ErrorKind) -> RpcErrorDetail {
        let rpc_error = rpc_error(err);
        assert_eq!(rpc_error.kind, expected, "{rpc_error:?}");
        assert!(
            matches!(rpc_error.recovery, RecoveryAction::RefreshMetadata { .. }),
            "{rpc_error:?}"
        );
        rpc_error
    }

    fn assert_retry(err: &beryl_proto::common::ErrorDetailProto, expected: ErrorKind) {
        let rpc_error = rpc_error(err);
        assert_eq!(rpc_error.kind, expected, "{rpc_error:?}");
        assert!(
            matches!(rpc_error.recovery, RecoveryAction::Retry { .. }),
            "{rpc_error:?}"
        );
    }

    fn refresh_hint(rpc_error: &RpcErrorDetail) -> &RefreshHint {
        match &rpc_error.recovery {
            RecoveryAction::RefreshMetadata { hint } | RecoveryAction::ReopenWriteSession { hint } => hint,
            other => panic!("expected refresh-like recovery, got {other:?}"),
        }
    }

    fn build_env(
        mount_prefix: &str,
        data_io_policy: DataIoPolicy,
        readiness_gate: Option<Arc<RootReadinessGate>>,
    ) -> PathTestEnv {
        let temp_dir = TempDir::new().expect("create temp dir");
        let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path()).expect("open rocksdb"));
        let mount_table = Arc::new(MountTable::new());

        let (mount_kind, ufs_uri, root_inode_id) = if mount_prefix == "/" {
            (MountKind::Internal, None, ROOT_INODE_ID)
        } else {
            (
                MountKind::External,
                Some(format!("file:///tmp{}", mount_prefix.replace('/', "_"))),
                InodeId::new(1000),
            )
        };
        let mount_entry = mount_table
            .create_mount(
                mount_prefix.to_string(),
                mount_kind,
                ufs_uri,
                data_io_policy,
                group_name("root"),
                root_inode_id,
            )
            .expect("create mount");

        let mut root_attrs = FileAttrs::new();
        root_attrs.uid = 1000;
        root_attrs.gid = 1000;
        root_attrs.mode = 0o755;
        storage
            .put_inode(&Inode::new_dir(root_inode_id, root_attrs, mount_entry.mount_id))
            .expect("put root inode");

        let state_store: Arc<dyn crate::state::StateStore> = Arc::new(TestStateStore::new());
        let session_registry = Arc::new(crate::session_registry::SessionRegistry::default());
        let lease_manager = Arc::new(crate::inode_lease::LeaseManager::default());
        let filesystem = Arc::new(MetadataFileSystem::new(MetadataFileSystemDeps {
            state_store,
            mount_table: Arc::clone(&mount_table),
            storage: Arc::clone(&storage),
            raft_node: None,
            session_registry: Arc::clone(&session_registry),
            lease_manager,
            worker_manager: None,
            metrics: None,
            readiness_gate,
        }));
        let service = MetadataFileSystemServiceImpl::new(filesystem, None);

        PathTestEnv {
            _temp_dir: temp_dir,
            storage,
            mount_table,
            raft_node: None,
            service,
            session_registry,
            worker_manager: None,
            mount_id: mount_entry.mount_id,
            root_inode_id,
        }
    }

    async fn build_env_with_raft(mount_prefix: &str, data_io_policy: DataIoPolicy) -> PathTestEnv {
        build_env_with_raft_and_workers(mount_prefix, data_io_policy, None).await
    }

    async fn build_env_with_raft_and_workers(
        mount_prefix: &str,
        data_io_policy: DataIoPolicy,
        worker_manager: Option<Arc<WorkerManager>>,
    ) -> PathTestEnv {
        build_env_with_optional_raft(mount_prefix, data_io_policy, worker_manager, true).await
    }

    async fn build_env_with_nonleader_raft(mount_prefix: &str, data_io_policy: DataIoPolicy) -> PathTestEnv {
        build_env_with_optional_raft(mount_prefix, data_io_policy, None, false).await
    }

    async fn build_env_with_optional_raft(
        mount_prefix: &str,
        data_io_policy: DataIoPolicy,
        worker_manager: Option<Arc<WorkerManager>>,
        initialize_single_node: bool,
    ) -> PathTestEnv {
        let temp_dir = TempDir::new().expect("create temp dir");
        let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path()).expect("open rocksdb"));
        let mount_table = Arc::new(MountTable::new());

        let (mount_kind, ufs_uri, root_inode_id) = if mount_prefix == "/" {
            (MountKind::Internal, None, ROOT_INODE_ID)
        } else {
            (
                MountKind::External,
                Some(format!("file:///tmp{}", mount_prefix.replace('/', "_"))),
                InodeId::new(1000),
            )
        };
        let mount_entry = mount_table
            .create_mount(
                mount_prefix.to_string(),
                mount_kind,
                ufs_uri,
                data_io_policy,
                group_name("root"),
                root_inode_id,
            )
            .expect("create mount");

        let mut root_attrs = FileAttrs::new();
        root_attrs.uid = 1000;
        root_attrs.gid = 1000;
        root_attrs.mode = 0o755;
        storage
            .put_inode(&Inode::new_dir(root_inode_id, root_attrs, mount_entry.mount_id))
            .expect("put root inode");

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
        if initialize_single_node {
            raft_node
                .initialize_single_node("127.0.0.1:0".to_string())
                .await
                .expect("initialize single-node raft");
            for _ in 0..50 {
                if raft_node.is_leader() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(raft_node.is_leader(), "single-node raft must become leader");
        } else {
            assert!(!raft_node.is_leader(), "uninitialized raft node must not be leader");
        }

        let state_store: Arc<dyn crate::state::StateStore> = Arc::new(TestStateStore::new());
        let session_registry = Arc::new(crate::session_registry::SessionRegistry::default());
        let lease_manager = Arc::new(crate::inode_lease::LeaseManager::default());
        let owner_group_name = group_name("root");
        let filesystem = Arc::new(MetadataFileSystem::new(MetadataFileSystemDeps {
            state_store,
            mount_table: Arc::clone(&mount_table),
            storage: Arc::clone(&storage),
            raft_node: Some(Arc::clone(&raft_node)),
            session_registry: Arc::clone(&session_registry),
            lease_manager,
            worker_manager: worker_manager.clone(),
            metrics: None,
            readiness_gate: None,
        }));
        let msync = Some(MsyncHandler::new(Arc::clone(&raft_node), owner_group_name));
        let service = MetadataFileSystemServiceImpl::new(filesystem, msync);

        PathTestEnv {
            _temp_dir: temp_dir,
            storage,
            mount_table,
            raft_node: Some(raft_node),
            service,
            session_registry,
            worker_manager,
            mount_id: mount_entry.mount_id,
            root_inode_id,
        }
    }

    fn worker_manager_for_write_targets() -> Arc<WorkerManager> {
        let manager = Arc::new(WorkerManager::new(60));
        for raw in 1..=3 {
            let worker_id = WorkerId::new(raw);
            let endpoint = format!("127.0.0.1:{}", 9000 + raw);
            let worker_run_id: WorkerRunId = format!("550e8400-e29b-41d4-a716-{raw:012x}")
                .parse()
                .expect("valid test worker run id");
            manager
                .register_worker(&group_name("root"), worker_id, endpoint.clone(), 1, None)
                .expect("register worker descriptor");
            manager
                .register_worker_run(&group_name("root"), worker_id, endpoint.clone(), 1, worker_run_id, None)
                .expect("register worker run");
            manager
                .register_worker(&group_name("root"), worker_id, endpoint.clone(), 1, None)
                .expect("restore worker descriptor");
            manager
                .record_heartbeat(
                    &group_name("root"),
                    worker_id,
                    worker_run_id,
                    1,
                    &endpoint,
                    1,
                    1024 * 1024,
                    0,
                    1024 * 1024,
                    0,
                    0,
                    HealthStatus::Healthy,
                )
                .expect("record worker heartbeat");
        }
        manager
    }

    fn publish_reported_location(env: &PathTestEnv, block_id: BlockId, block_stamp: u64, effective_len: u64) {
        let worker_manager = env.worker_manager.as_ref().expect("worker manager");
        let worker_id = WorkerId::new(1);
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
                vec![BlockReportBlock {
                    block_id,
                    data_handle_id: block_id.data_handle_id.as_raw(),
                    block_index: block_id.index.as_raw(),
                    block_stamp,
                    effective_len,
                    committed_length: effective_len,
                    block_state: BlockReportBlockState::Ready,
                }],
            )
            .expect("full block report should publish location");
    }

    async fn open_write_session_with_committed_block(
        env: &PathTestEnv,
        path: &str,
        client_id: u128,
    ) -> (WriteHandleProto, u64, CommittedBlockProto) {
        let create = FileSystemServiceProto::create_file(
            &env.service,
            Request::new(CreateFileRequestProto {
                header: header(client_id),
                path: path.to_string(),
                attrs: Some(beryl_proto::fs::FileAttrsProto {
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    ..Default::default()
                }),
                layout: Some(beryl_proto::common::FileLayoutProto {
                    block_size: 4096,
                    chunk_size: 4096,
                    replication: 1,
                    block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
                }),
                create_mode: CreateModeProto::CreateNew as i32,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(create.header);

        let data_handle_id = create.data_handle_id.expect("data handle").value;
        let open = FileSystemServiceProto::open_write(
            &env.service,
            Request::new(OpenWriteRequestProto {
                header: header(client_id),
                path: path.to_string(),
                mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
                desired_len: Some(128),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(open.header);
        let write_handle = open.write_handle.expect("write handle");
        let target = FileSystemServiceProto::add_block(
            &env.service,
            Request::new(AddBlockRequestProto {
                header: header(client_id),
                write_handle: Some(write_handle),
                desired_len: Some(128),
                previous_block_id: None,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner()
        .target
        .expect("write target");
        let committed = CommittedBlockProto {
            block_id: target.block_id,
            file_offset: target.file_offset,
            len: target.effective_len,
            checksum: None,
        };

        (write_handle, data_handle_id, committed)
    }

    #[tokio::test]
    async fn list_status_recursive_request_returns_not_supported() {
        let env = build_env("/mnt/test", DataIoPolicy::Allow, None);

        let response = FileSystemServiceProto::list_status(
            &env.service,
            Request::new(ListStatusRequestProto {
                header: header(708),
                path: "/mnt/test".to_string(),
                recursive: true,
                cursor: Vec::new(),
                limit: 0,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        let err = header_error(response.header);

        assert_fs_errno(&err, FsErrnoProto::FsErrnoEnotsup);
        assert!(err.message.contains("Recursive listing not yet implemented"));
        assert!(response.entries.is_empty());
    }

    #[tokio::test]
    async fn recursive_create_directory_creates_missing_parent_directories() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let request = CreateDirectoryRequestProto {
            header: header(707),
            path: "/mnt/test/a/b/c".to_string(),
            attrs: Some(beryl_proto::fs::FileAttrsProto {
                mode: 0o755,
                uid: 1000,
                gid: 1000,
                ..Default::default()
            }),
            recursive: true,
        };

        let response = FileSystemServiceProto::create_directory(&env.service, Request::new(request.clone()))
            .await
            .expect("transport status must remain OK")
            .into_inner();

        assert_success_header(response.header);
        let next_inode_after_first = env.storage.get_next_inode_id().expect("next inode after mkdir");
        let a = env
            .storage
            .get_dentry(env.root_inode_id, "a")
            .expect("lookup /a")
            .expect("/a created");
        let moved = FileSystemServiceProto::rename(
            &env.service,
            Request::new(RenameRequestProto {
                header: header(707),
                src_path: "/mnt/test/a".to_string(),
                dst_path: "/mnt/test/moved-a".to_string(),
                flags: 0,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(moved.header);

        let replay = FileSystemServiceProto::create_directory(&env.service, Request::new(request))
            .await
            .expect("transport status must remain OK")
            .into_inner();
        assert_success_header(replay.header);
        assert_eq!(env.storage.get_next_inode_id().unwrap(), next_inode_after_first);
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "a").unwrap(), None);
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "moved-a").unwrap(), Some(a));
        let b = env
            .storage
            .get_dentry(a, "b")
            .expect("lookup /a/b")
            .expect("/a/b created");
        let c = env
            .storage
            .get_dentry(b, "c")
            .expect("lookup /a/b/c")
            .expect("/a/b/c created");
        assert!(env
            .storage
            .get_inode(a)
            .expect("load /a")
            .expect("/a inode")
            .kind
            .is_dir());
        assert!(env
            .storage
            .get_inode(b)
            .expect("load /a/b")
            .expect("/a/b inode")
            .kind
            .is_dir());
        assert!(env
            .storage
            .get_inode(c)
            .expect("load /a/b/c")
            .expect("/a/b/c inode")
            .kind
            .is_dir());
    }

    #[tokio::test]
    async fn recursive_create_directory_fails_when_parent_component_is_file() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let file_inode_id = InodeId::new(5001);
        env.storage
            .put_inode(&Inode::new_file(
                file_inode_id,
                FileAttrs::new(),
                env.mount_id,
                DataHandleId::new(5001),
            ))
            .expect("put file inode");
        env.storage
            .put_dentry(env.root_inode_id, "file", file_inode_id)
            .expect("put file dentry");

        let response = FileSystemServiceProto::create_directory(
            &env.service,
            Request::new(CreateDirectoryRequestProto {
                header: header(708),
                path: "/mnt/test/file/child".to_string(),
                attrs: Some(beryl_proto::fs::FileAttrsProto {
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                    ..Default::default()
                }),
                recursive: true,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let err = header_error(response.header);
        assert_fs_errno(&err, FsErrnoProto::FsErrnoEnotdir);
        assert_eq!(env.storage.get_dentry(file_inode_id, "child").unwrap(), None);
    }

    fn put_dir(env: &PathTestEnv, parent_inode_id: InodeId, name: &str, inode_id: InodeId) {
        env.storage
            .put_inode(&Inode::new_dir(inode_id, FileAttrs::new(), env.mount_id))
            .expect("put directory inode");
        env.storage
            .put_dentry(parent_inode_id, name, inode_id)
            .expect("put directory dentry");
    }

    fn put_empty_file(
        env: &PathTestEnv,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        data_handle_id: DataHandleId,
    ) {
        env.storage
            .put_inode(&Inode::new_file(
                inode_id,
                FileAttrs::new(),
                env.mount_id,
                data_handle_id,
            ))
            .expect("put file inode");
        env.storage
            .put_dentry(parent_inode_id, name, inode_id)
            .expect("put file dentry");
        env.storage
            .put_layout(inode_id, FileLayout::new(4096, 4096, 1))
            .expect("put file layout");
        env.storage
            .put_data_handle_owner(data_handle_id, inode_id)
            .expect("put data handle owner");
    }

    #[tokio::test]
    async fn rename_public_path_moves_entry_and_returns_success_header() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let parent_inode_id = InodeId::new(3900);
        let inode_id = InodeId::new(3901);
        let data_handle_id = DataHandleId::new(3901);
        put_dir(&env, env.root_inode_id, "parent", parent_inode_id);
        put_empty_file(&env, parent_inode_id, "source", inode_id, data_handle_id);

        let request = RenameRequestProto {
            header: header(3901),
            src_path: "/mnt/test/parent/source".to_string(),
            dst_path: "/mnt/test/parent/destination".to_string(),
            flags: 1,
        };
        let response = FileSystemServiceProto::rename(&env.service, Request::new(request.clone()))
            .await
            .expect("transport status must remain OK")
            .into_inner();

        assert_success_header(response.header);
        let moved_parent = FileSystemServiceProto::rename(
            &env.service,
            Request::new(RenameRequestProto {
                header: header(3901),
                src_path: "/mnt/test/parent".to_string(),
                dst_path: "/mnt/test/moved-parent".to_string(),
                flags: 0,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(moved_parent.header);
        let replay = FileSystemServiceProto::rename(&env.service, Request::new(request))
            .await
            .expect("transport status must remain OK")
            .into_inner();
        assert_success_header(replay.header);
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "parent").unwrap(), None);
        assert_eq!(
            env.storage.get_dentry(env.root_inode_id, "moved-parent").unwrap(),
            Some(parent_inode_id)
        );
        assert_eq!(env.storage.get_dentry(parent_inode_id, "source").unwrap(), None);
        assert_eq!(
            env.storage.get_dentry(parent_inode_id, "destination").unwrap(),
            Some(inode_id)
        );
        assert_eq!(
            env.storage.get_inode_by_data_handle(data_handle_id).unwrap(),
            Some(inode_id)
        );
    }

    fn put_extent_file(
        env: &PathTestEnv,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        block_id: BlockId,
        len: u64,
    ) {
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), env.mount_id, data_handle_id);
        inode.attrs.size = len;
        if let beryl_types::fs::InodeData::File {
            extents,
            file_version,
            lease_epoch,
        } = &mut inode.data
        {
            *extents = vec![Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len,
                file_version: Some(1),
                block_stamp: Some(1),
            }];
            *file_version = Some(1);
            *lease_epoch = Some(1);
        }
        env.storage.put_inode(&inode).expect("put extent file inode");
        env.storage
            .put_dentry(parent_inode_id, name, inode_id)
            .expect("put extent file dentry");
        env.storage
            .put_layout(inode_id, FileLayout::new(4096, 4096, 1))
            .expect("put extent file layout");
        env.storage
            .put_data_handle_owner(data_handle_id, inode_id)
            .expect("put extent file owner");
    }

    #[tokio::test]
    async fn stale_mount_epoch_returns_refresh_metadata_header_with_consumable_mount_hint() {
        let env = build_env("/mnt/test", DataIoPolicy::Allow, None);

        let response = FileSystemServiceProto::get_status(
            &env.service,
            Request::new(GetStatusRequestProto {
                header: header_with_freshness(101, Some(0), None, Vec::new()),
                path: "/mnt/test".to_string(),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let response_header = response.header.expect("response header must exist");
        let err = response_header.error.expect("header.error must exist");
        let rpc_error = assert_refresh_metadata(&err, ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch));
        assert_eq!(response_header.group_name, TEST_GROUP_NAME);
        assert_eq!(response_header.mount_epoch, Some(1));
        let hint = refresh_hint(&rpc_error);
        assert_eq!(hint.group_name.as_deref(), Some(TEST_GROUP_NAME));
        assert_eq!(hint.mount_epoch, Some(1));
        assert_eq!(hint.route_epoch, None);
    }

    #[tokio::test]
    async fn stale_route_epoch_returns_refresh_metadata_header_with_consumable_route_hint() {
        let env = build_env("/mnt/test", DataIoPolicy::Allow, None);

        let response = FileSystemServiceProto::get_status(
            &env.service,
            Request::new(GetStatusRequestProto {
                header: header_with_freshness(102, Some(1), Some(0), Vec::new()),
                path: "/mnt/test".to_string(),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let response_header = response.header.expect("response header must exist");
        let err = response_header.error.expect("header.error must exist");
        let rpc_error = assert_refresh_metadata(&err, ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch));
        assert_eq!(response_header.group_name, TEST_GROUP_NAME);
        assert_eq!(response_header.mount_epoch, Some(1));
        assert_eq!(response_header.route_epoch, Some(1));
        let hint = refresh_hint(&rpc_error);
        assert_eq!(hint.group_name.as_deref(), Some(TEST_GROUP_NAME));
        assert_eq!(hint.mount_epoch, Some(1));
        assert_eq!(hint.route_epoch, Some(1));
    }

    #[tokio::test]
    async fn stale_state_id_returns_stale_state_without_epoch_domain_mixup() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let local_state_id = env
            .raft_node
            .as_ref()
            .expect("raft node")
            .get_last_applied_state_id()
            .expect("initialized raft applied state");
        let required_state_id = RaftLogId::new(
            local_state_id.term,
            local_state_id.leader_node_id,
            local_state_id.index + 1,
        );

        let response = FileSystemServiceProto::get_status(
            &env.service,
            Request::new(GetStatusRequestProto {
                header: header_with_freshness(
                    103,
                    Some(1),
                    Some(1),
                    vec![watermark_proto(TEST_GROUP_NAME, required_state_id)],
                ),
                path: "/mnt/test".to_string(),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let response_header = response.header.expect("response header must exist");
        let err = response_header.error.expect("header.error must exist");
        let rpc_error = assert_refresh_metadata(&err, ErrorKind::Metadata(MetadataErrorKind::StaleState));
        assert_eq!(response_header.group_name, TEST_GROUP_NAME);
        assert_eq!(response_header.mount_epoch, Some(1));
        assert_ne!(response_header.mount_epoch, Some(required_state_id.index));
        assert_ne!(response_header.route_epoch, Some(required_state_id.index));
        assert!(response_header.state.is_empty());
        assert_eq!(refresh_hint(&rpc_error), &RefreshHint::default());
    }

    #[tokio::test]
    async fn leader_success_header_includes_group_state_watermark_when_last_applied_is_known() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let last_applied = env
            .raft_node
            .as_ref()
            .expect("raft node")
            .get_last_applied_state_id()
            .expect("initialized raft applied state");

        let response = FileSystemServiceProto::get_status(
            &env.service,
            Request::new(GetStatusRequestProto {
                header: header_with_freshness(104, Some(1), Some(1), Vec::new()),
                path: "/mnt/test".to_string(),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let response_header = response.header.expect("response header must exist");
        assert!(response_header.error.is_none());
        assert_eq!(response_header.group_name, TEST_GROUP_NAME);
        assert_eq!(response_header.mount_epoch, Some(1));
        assert_eq!(response_header.route_epoch, Some(1));
        assert_eq!(response_header.state.len(), 1);
        let state = &response_header.state[0];
        assert_eq!(state.group_name, TEST_GROUP_NAME);
        assert_state_id(state.state_id.as_ref().expect("state id"), last_applied);
    }

    #[tokio::test]
    async fn non_leader_success_header_leaves_state_empty() {
        let env = build_env("/mnt/test", DataIoPolicy::Allow, None);

        let response = FileSystemServiceProto::get_status(
            &env.service,
            Request::new(GetStatusRequestProto {
                header: header_with_freshness(105, Some(1), Some(1), Vec::new()),
                path: "/mnt/test".to_string(),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let response_header = response.header.expect("response header must exist");
        assert!(response_header.error.is_none());
        assert_eq!(response_header.group_name, TEST_GROUP_NAME);
        assert_eq!(response_header.mount_epoch, Some(1));
        assert_eq!(response_header.route_epoch, Some(1));
        assert!(response_header.state.is_empty());
    }

    #[tokio::test]
    async fn readiness_precedence_blocks_before_path_resolution() {
        let readiness_gate = Arc::new(RootReadinessGate::new(None));
        let env = build_env("/mnt/test", DataIoPolicy::Allow, Some(readiness_gate));

        let response = FileSystemServiceProto::get_status(
            &env.service,
            Request::new(GetStatusRequestProto {
                header: header(1),
                path: "".to_string(),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let err = header_error(response.header);
        assert_retry(&err, ErrorKind::Internal(InternalErrorKind::NodeUnavailable));
    }

    #[tokio::test]
    async fn leadership_precedence_write_returns_not_leader_before_not_found() {
        let env = build_env_with_nonleader_raft("/mnt/test", DataIoPolicy::Allow).await;

        let response = FileSystemServiceProto::create_directory(
            &env.service,
            Request::new(CreateDirectoryRequestProto {
                header: header(2),
                path: "/mnt/test/missing/child".to_string(),
                attrs: None,
                recursive: false,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let err = header_error(response.header);
        assert_not_leader(&err);
    }

    #[tokio::test]
    async fn leadership_precedence_data_io_returns_not_leader_before_mount_policy_error() {
        let env = build_env_with_nonleader_raft("/mnt/test", DataIoPolicy::Forbid).await;
        let file_inode_id = InodeId::new(2001);
        env.storage
            .put_inode(&Inode::new_file(
                file_inode_id,
                FileAttrs::new(),
                env.mount_id,
                DataHandleId::new(2001),
            ))
            .expect("put test file inode");
        env.storage
            .put_dentry(env.root_inode_id, "file", file_inode_id)
            .expect("put test file dentry");

        let response = FileSystemServiceProto::open_write(
            &env.service,
            Request::new(OpenWriteRequestProto {
                header: header(3),
                path: "/mnt/test/file".to_string(),
                mode: OpenWriteModeProto::OpenWriteModeAppend as i32,
                desired_len: Some(0),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let err = header_error(response.header);
        assert_not_leader(&err);
    }

    #[tokio::test]
    async fn sync_write_rejects_structural_validation_errors() {
        let env = build_env_with_raft_and_workers(
            "/mnt/test",
            DataIoPolicy::Allow,
            Some(worker_manager_for_write_targets()),
        )
        .await;
        let (write_handle, data_handle_id, committed) =
            open_write_session_with_committed_block(&env, "/mnt/test/sync-validation", 40).await;

        let unspecified = FileSystemServiceProto::sync_write(
            &env.service,
            Request::new(SyncWriteRequestProto {
                header: header(40),
                write_handle: Some(write_handle),
                data_handle_id: Some(DataHandleIdProto { value: data_handle_id }),
                committed_blocks: vec![committed.clone()],
                target_size: 128,
                mode: WriteSyncModeProto::WriteSyncModeUnspecified as i32,
                flags: 0,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        let err = header_error(unspecified.header);
        assert_fs_errno(&err, FsErrnoProto::FsErrnoEinval);
        assert!(err.message.contains("SyncWrite mode"));

        let missing_data_handle = FileSystemServiceProto::sync_write(
            &env.service,
            Request::new(SyncWriteRequestProto {
                header: header(40),
                write_handle: Some(write_handle),
                data_handle_id: None,
                committed_blocks: vec![committed.clone()],
                target_size: 128,
                mode: WriteSyncModeProto::WriteSyncModeVisibility as i32,
                flags: 0,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        let err = header_error(missing_data_handle.header);
        assert_fs_errno(&err, FsErrnoProto::FsErrnoEinval);
        assert!(err.message.contains("missing data_handle_id"));

        let mut mismatched = committed;
        mismatched.block_id.as_mut().expect("block id").data_handle_id += 1;
        let mismatch = FileSystemServiceProto::sync_write(
            &env.service,
            Request::new(SyncWriteRequestProto {
                header: header(40),
                write_handle: Some(write_handle),
                data_handle_id: Some(DataHandleIdProto { value: data_handle_id }),
                committed_blocks: vec![mismatched],
                target_size: 128,
                mode: WriteSyncModeProto::WriteSyncModeVisibility as i32,
                flags: 0,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        let err = header_error(mismatch.header);
        assert_fs_errno(&err, FsErrnoProto::FsErrnoEinval);
        assert!(err.message.contains("committed block data_handle_id"));
    }

    #[tokio::test]
    async fn sync_write_valid_request_publishes_prefix_and_keeps_session_open() {
        let env = build_env_with_raft_and_workers(
            "/mnt/test",
            DataIoPolicy::Allow,
            Some(worker_manager_for_write_targets()),
        )
        .await;
        let (write_handle, data_handle_id, committed) =
            open_write_session_with_committed_block(&env, "/mnt/test/sync-publish", 50).await;

        for mode in [
            WriteSyncModeProto::WriteSyncModeVisibility,
            WriteSyncModeProto::WriteSyncModeDurability,
        ] {
            let response = FileSystemServiceProto::sync_write(
                &env.service,
                Request::new(SyncWriteRequestProto {
                    header: header(50),
                    write_handle: Some(write_handle),
                    data_handle_id: Some(DataHandleIdProto { value: data_handle_id }),
                    committed_blocks: vec![committed.clone()],
                    target_size: 128,
                    mode: mode as i32,
                    flags: 0,
                }),
            )
            .await
            .expect("transport status must remain OK")
            .into_inner();
            assert_success_header(response.header);
            assert_eq!(response.synced_size, 128);
            assert!(response.file_version.is_some());
            assert!(env.session_registry.get_session(write_handle.handle_id).is_some());
        }
    }

    #[tokio::test]
    async fn get_locations_rejects_stale_handle() {
        let env = build_env("/", DataIoPolicy::Allow, None);
        let file_inode_id = InodeId::new(9101);
        let current_handle = DataHandleId::new(99101);
        let stale_handle = DataHandleId::new(99100);
        let mut attrs = FileAttrs::new();
        attrs.size = 128;
        let mut inode = Inode::new_file(file_inode_id, attrs, env.mount_id, current_handle);
        inode.data = beryl_types::fs::InodeData::File {
            extents: vec![Extent {
                file_offset: 0,
                block_id: BlockId::new(current_handle, BlockIndex::new(0)),
                block_offset: 0,
                len: 128,
                file_version: Some(4),
                block_stamp: Some(4),
            }],
            file_version: Some(4),
            lease_epoch: Some(4),
        };
        env.storage.put_inode(&inode).expect("put file inode");
        env.storage
            .put_dentry(env.root_inode_id, "file", file_inode_id)
            .expect("put file dentry");
        env.storage
            .put_layout(file_inode_id, FileLayout::new(4096, 4096, 1))
            .expect("put layout");
        env.storage
            .put_data_handle_owner(current_handle, file_inode_id)
            .expect("put current owner");
        env.storage
            .put_data_handle_owner(stale_handle, file_inode_id)
            .expect("put stale owner");

        let response = FileSystemServiceProto::get_block_locations(
            &env.service,
            Request::new(GetBlockLocationsRequestProto {
                header: header(21),
                target: Some(get_block_locations_request_proto::Target::DataHandleId(
                    DataHandleIdProto {
                        value: stale_handle.as_raw(),
                    },
                )),
                range: None,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let err = header_error(response.header);
        assert_refresh_metadata(&err, ErrorKind::Metadata(MetadataErrorKind::StaleState));
        assert!(err.message.contains("not current data_handle_id"));
    }

    #[tokio::test]
    async fn get_locations_by_path_uses_current_handle() {
        let env = build_env("/", DataIoPolicy::Allow, None);
        let file_inode_id = InodeId::new(9103);
        let current_handle = DataHandleId::new(99103);
        let stale_handle = DataHandleId::new(99104);
        let mut attrs = FileAttrs::new();
        attrs.size = 128;
        let mut inode = Inode::new_file(file_inode_id, attrs, env.mount_id, current_handle);
        inode.data = beryl_types::fs::InodeData::File {
            extents: vec![Extent {
                file_offset: 0,
                block_id: BlockId::new(current_handle, BlockIndex::new(0)),
                block_offset: 0,
                len: 128,
                file_version: Some(8),
                block_stamp: Some(8),
            }],
            file_version: Some(8),
            lease_epoch: Some(8),
        };
        env.storage.put_inode(&inode).expect("put file inode");
        env.storage
            .put_dentry(env.root_inode_id, "file", file_inode_id)
            .expect("put file dentry");
        env.storage
            .put_layout(file_inode_id, FileLayout::new(4096, 4096, 1))
            .expect("put layout");
        env.storage
            .put_data_handle_owner(current_handle, file_inode_id)
            .expect("put current owner");
        env.storage
            .put_data_handle_owner(stale_handle, file_inode_id)
            .expect("put stale owner");

        let response = FileSystemServiceProto::get_block_locations(
            &env.service,
            Request::new(GetBlockLocationsRequestProto {
                header: header(23),
                target: Some(get_block_locations_request_proto::Target::Path("/file".to_string())),
                range: None,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        assert_success_header(response.header);
        assert_eq!(
            response.data_handle_id.expect("data handle").value,
            current_handle.as_raw()
        );
        assert_eq!(response.file_version, Some(8));
        assert_eq!(
            response.locations[0]
                .block_id
                .as_ref()
                .expect("block id")
                .data_handle_id,
            current_handle.as_raw()
        );
    }

    #[tokio::test]
    async fn create_file_durable_step_does_not_require_write_runtime() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;

        let response = FileSystemServiceProto::create_file(
            &env.service,
            Request::new(CreateFileRequestProto {
                header: header(20),
                path: "/mnt/test/new-file".to_string(),
                attrs: Some(beryl_proto::fs::FileAttrsProto {
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    ..Default::default()
                }),
                layout: Some(beryl_proto::common::FileLayoutProto {
                    block_size: 4096,
                    chunk_size: 4096,
                    replication: 1,
                    block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
                }),
                create_mode: CreateModeProto::CreateNew as i32,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        assert_success_header(response.header);
        assert!(env.storage.get_dentry(env.root_inode_id, "new-file").unwrap().is_some());
    }

    #[tokio::test]
    async fn open_write_replay_precedes_current_path_resolution() {
        let env = build_env_with_raft_and_workers(
            "/mnt/test",
            DataIoPolicy::Allow,
            Some(worker_manager_for_write_targets()),
        )
        .await;
        let client_id = 25;
        let create = FileSystemServiceProto::create_file(
            &env.service,
            Request::new(CreateFileRequestProto {
                header: header(client_id),
                path: "/mnt/test/open-original".to_string(),
                attrs: Some(beryl_proto::fs::FileAttrsProto {
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    ..Default::default()
                }),
                layout: Some(beryl_proto::common::FileLayoutProto {
                    block_size: 4096,
                    chunk_size: 4096,
                    replication: 1,
                    block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
                }),
                create_mode: CreateModeProto::CreateNew as i32,
            }),
        )
        .await
        .expect("CreateFile transport")
        .into_inner();
        assert_success_header(create.header);

        let open_header = header(client_id);
        let request = OpenWriteRequestProto {
            header: open_header.clone(),
            path: "/mnt/test/open-original".to_string(),
            mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
            desired_len: Some(128),
        };
        let first = FileSystemServiceProto::open_write(&env.service, Request::new(request.clone()))
            .await
            .expect("OpenWrite transport")
            .into_inner();
        assert_success_header(first.header.clone());

        let renamed = FileSystemServiceProto::rename(
            &env.service,
            Request::new(RenameRequestProto {
                header: header(client_id),
                src_path: "/mnt/test/open-original".to_string(),
                dst_path: "/mnt/test/open-moved".to_string(),
                flags: 0,
            }),
        )
        .await
        .expect("Rename transport")
        .into_inner();
        assert_success_header(renamed.header);

        let replay = FileSystemServiceProto::open_write(&env.service, Request::new(request))
            .await
            .expect("OpenWrite replay transport")
            .into_inner();
        assert_success_header(replay.header.clone());
        assert_eq!(replay.write_handle, first.write_handle);
        assert_eq!(replay.data_handle_id, first.data_handle_id);
        assert_eq!(replay.expires_at_ms, first.expires_at_ms);
    }

    #[tokio::test]
    async fn create_file_replay_returns_frozen_result_after_parent_rename() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let client_id = 26;
        let parent_inode_id = InodeId::new(2600);
        put_dir(&env, env.root_inode_id, "parent", parent_inode_id);
        let request = CreateFileRequestProto {
            header: header(client_id),
            path: "/mnt/test/parent/frozen-create".to_string(),
            attrs: Some(beryl_proto::fs::FileAttrsProto {
                mode: 0o640,
                uid: 1000,
                gid: 1000,
                ..Default::default()
            }),
            layout: Some(beryl_proto::common::FileLayoutProto {
                block_size: 4096,
                chunk_size: 4096,
                replication: 1,
                block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
            }),
            create_mode: CreateModeProto::CreateNew as i32,
        };
        let first = FileSystemServiceProto::create_file(&env.service, Request::new(request.clone()))
            .await
            .expect("CreateFile transport")
            .into_inner();
        assert_success_header(first.header.clone());

        let moved_parent = FileSystemServiceProto::rename(
            &env.service,
            Request::new(RenameRequestProto {
                header: header(client_id),
                src_path: "/mnt/test/parent".to_string(),
                dst_path: "/mnt/test/moved-parent".to_string(),
                flags: 0,
            }),
        )
        .await
        .expect("Rename parent transport")
        .into_inner();
        assert_success_header(moved_parent.header);

        let replay = FileSystemServiceProto::create_file(&env.service, Request::new(request))
            .await
            .expect("CreateFile replay transport")
            .into_inner();
        assert_success_header(replay.header.clone());
        assert_eq!(replay.inode_id, first.inode_id);
        assert_eq!(replay.data_handle_id, first.data_handle_id);
        assert_eq!(replay.layout, first.layout);
        assert_eq!(replay.file_size, first.file_size);
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "parent").unwrap(), None);
        assert_eq!(
            env.storage.get_dentry(env.root_inode_id, "moved-parent").unwrap(),
            Some(parent_inode_id)
        );
        assert!(env
            .storage
            .get_dentry(parent_inode_id, "frozen-create")
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn call_id_cannot_cross_durable_and_session_rpc_methods() {
        let env = build_env_with_raft_and_workers(
            "/mnt/test",
            DataIoPolicy::Allow,
            Some(worker_manager_for_write_targets()),
        )
        .await;
        let client_id = 27;
        let create_header = header(client_id);
        let create = FileSystemServiceProto::create_file(
            &env.service,
            Request::new(CreateFileRequestProto {
                header: create_header.clone(),
                path: "/mnt/test/cross-method".to_string(),
                attrs: Some(beryl_proto::fs::FileAttrsProto {
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    ..Default::default()
                }),
                layout: Some(beryl_proto::common::FileLayoutProto {
                    block_size: 4096,
                    chunk_size: 4096,
                    replication: 1,
                    block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
                }),
                create_mode: CreateModeProto::CreateNew as i32,
            }),
        )
        .await
        .expect("CreateFile transport")
        .into_inner();
        assert_success_header(create.header);

        let reused_create_call = FileSystemServiceProto::open_write(
            &env.service,
            Request::new(OpenWriteRequestProto {
                header: create_header,
                path: "/mnt/test/cross-method".to_string(),
                mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
                desired_len: Some(128),
            }),
        )
        .await
        .expect("OpenWrite transport")
        .into_inner();
        assert_fs_errno(&header_error(reused_create_call.header), FsErrnoProto::FsErrnoEinval);

        let open_header = header(client_id);
        let open = FileSystemServiceProto::open_write(
            &env.service,
            Request::new(OpenWriteRequestProto {
                header: open_header.clone(),
                path: "/mnt/test/cross-method".to_string(),
                mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
                desired_len: Some(128),
            }),
        )
        .await
        .expect("OpenWrite transport")
        .into_inner();
        assert_success_header(open.header);
        let write_handle = open.write_handle.expect("write handle");

        let reused_open_call = FileSystemServiceProto::add_block(
            &env.service,
            Request::new(AddBlockRequestProto {
                header: open_header.clone(),
                write_handle: Some(write_handle),
                desired_len: Some(128),
                previous_block_id: None,
            }),
        )
        .await
        .expect("AddBlock transport")
        .into_inner();
        assert_fs_errno(&header_error(reused_open_call.header), FsErrnoProto::FsErrnoEinval);

        let reused_open_for_delete = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: open_header,
                path: "/mnt/test/cross-method".to_string(),
                recursive: false,
            }),
        )
        .await
        .expect("Delete transport")
        .into_inner();
        assert_fs_errno(
            &header_error(reused_open_for_delete.header),
            FsErrnoProto::FsErrnoEinval,
        );
    }

    #[tokio::test]
    async fn commit_file_public_replay_returns_persisted_result_and_rejects_fingerprint_mismatch() {
        let env = build_env_with_raft_and_workers(
            "/mnt/test",
            DataIoPolicy::Allow,
            Some(worker_manager_for_write_targets()),
        )
        .await;

        let create = FileSystemServiceProto::create_file(
            &env.service,
            Request::new(CreateFileRequestProto {
                header: header(30),
                path: "/mnt/test/replay-file".to_string(),
                attrs: Some(beryl_proto::fs::FileAttrsProto {
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    ..Default::default()
                }),
                layout: Some(beryl_proto::common::FileLayoutProto {
                    block_size: 4096,
                    chunk_size: 4096,
                    replication: 1,
                    block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
                }),
                create_mode: CreateModeProto::CreateNew as i32,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(create.header);

        let data_handle_id = create.data_handle_id.expect("data handle").value;
        let open = FileSystemServiceProto::open_write(
            &env.service,
            Request::new(OpenWriteRequestProto {
                header: header(30),
                path: "/mnt/test/replay-file".to_string(),
                mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
                desired_len: Some(128),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(open.header);
        let write_handle = open.write_handle.expect("write handle");
        let file_inode_id = env
            .storage
            .get_inode_by_data_handle(DataHandleId::new(data_handle_id))
            .unwrap()
            .expect("created inode owner");
        assert!(env.session_registry.get_session(write_handle.handle_id).is_some());

        let target = FileSystemServiceProto::add_block(
            &env.service,
            Request::new(AddBlockRequestProto {
                header: header(30),
                write_handle: Some(write_handle),
                desired_len: Some(128),
                previous_block_id: None,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner()
        .target
        .expect("write target");
        let block_id = target.block_id.expect("target block id");
        let committed_blocks = vec![CommittedBlockProto {
            block_id: Some(block_id),
            file_offset: target.file_offset,
            len: target.effective_len,
            checksum: None,
        }];

        let commit_header = header(30);
        let first = FileSystemServiceProto::commit_file(
            &env.service,
            Request::new(CommitFileRequestProto {
                header: commit_header.clone(),
                write_handle: Some(write_handle),
                data_handle_id: Some(DataHandleIdProto { value: data_handle_id }),
                committed_blocks: committed_blocks.clone(),
                final_size: 128,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(first.header);
        assert_eq!(first.committed_size, 128);
        let first_file_version = first.file_version.expect("first file version");
        assert_ne!(first_file_version, 0);
        assert!(env.session_registry.get_session(write_handle.handle_id).is_none());
        let typed_block_id = BlockId::new(
            DataHandleId::new(block_id.data_handle_id),
            BlockIndex::new(block_id.block_index),
        );
        publish_reported_location(&env, typed_block_id, first_file_version, target.effective_len);

        let locations = FileSystemServiceProto::get_block_locations(
            &env.service,
            Request::new(GetBlockLocationsRequestProto {
                header: header(33),
                target: Some(get_block_locations_request_proto::Target::DataHandleId(
                    DataHandleIdProto { value: data_handle_id },
                )),
                range: Some(beryl_proto::common::ByteRangeProto { offset: 0, len: 128 }),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(locations.header);
        assert_eq!(locations.file_version, Some(first_file_version));
        assert_eq!(locations.locations.len(), 1);
        assert_eq!(locations.locations[0].block_stamp, Some(first_file_version));

        let second = FileSystemServiceProto::commit_file(
            &env.service,
            Request::new(CommitFileRequestProto {
                header: commit_header.clone(),
                write_handle: Some(write_handle),
                data_handle_id: Some(DataHandleIdProto { value: data_handle_id }),
                committed_blocks: committed_blocks.clone(),
                final_size: 128,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(second.header);
        assert_eq!(second.committed_size, first.committed_size);
        assert_eq!(second.file_version, Some(first_file_version));

        let inode = env.storage.get_inode(file_inode_id).unwrap().expect("committed inode");
        assert_eq!(inode.attrs.size, 128);
        match inode.data {
            beryl_types::fs::InodeData::File {
                extents, file_version, ..
            } => {
                assert_eq!(file_version, Some(first_file_version));
                assert_eq!(extents.len(), 1);
                assert_eq!(extents[0].block_id, typed_block_id);
                assert_eq!(extents[0].len, 128);
                assert_eq!(extents[0].block_stamp, Some(first_file_version));
            }
            other => panic!("expected file inode data, got {:?}", other),
        }

        let mismatch = FileSystemServiceProto::commit_file(
            &env.service,
            Request::new(CommitFileRequestProto {
                header: commit_header,
                write_handle: Some(write_handle),
                data_handle_id: Some(DataHandleIdProto { value: data_handle_id }),
                committed_blocks,
                final_size: 129,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        let err = header_error(mismatch.header);
        assert_fs_errno(&err, FsErrnoProto::FsErrnoEinval);
        assert!(err.message.contains("reused with different command payload"));
        let after_mismatch = env
            .storage
            .get_inode(file_inode_id)
            .unwrap()
            .expect("inode after mismatch");
        assert_eq!(after_mismatch.attrs.size, 128);
        match after_mismatch.data {
            beryl_types::fs::InodeData::File { file_version, .. } => {
                assert_eq!(file_version, Some(first_file_version));
            }
            other => panic!("expected file inode data, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn delete_missing_path_returns_structured_header_error() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;

        let response = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: header(13),
                path: "/mnt/test/missing".to_string(),
                recursive: false,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let err = header_error(response.header);
        assert_fs_errno(&err, FsErrnoProto::FsErrnoEnoent);
    }

    #[tokio::test]
    async fn recursive_delete_nested_tree_success_removes_subtree_only() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let dir = InodeId::new(4101);
        let a = InodeId::new(4102);
        let b = InodeId::new(4103);
        let empty_subdir = InodeId::new(4104);
        let file1 = InodeId::new(4105);
        let file2 = InodeId::new(4106);
        let file1_handle = DataHandleId::new(4105);
        let file2_handle = DataHandleId::new(4106);

        put_dir(&env, env.root_inode_id, "dir", dir);
        put_dir(&env, dir, "a", a);
        put_dir(&env, a, "b", b);
        put_dir(&env, dir, "empty_subdir", empty_subdir);
        put_empty_file(&env, a, "file1", file1, file1_handle);
        put_empty_file(&env, b, "file2", file2, file2_handle);

        let response = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: header(141),
                path: "/mnt/test/dir".to_string(),
                recursive: true,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        assert_success_header(response.header);
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir").unwrap(), None);
        for inode_id in [dir, a, b, empty_subdir, file1, file2] {
            assert!(env.storage.get_inode(inode_id).unwrap().is_none());
        }
        assert!(env.storage.get_inode(env.root_inode_id).unwrap().is_some());
        assert_eq!(env.storage.get_inode_by_data_handle(file1_handle).unwrap(), None);
        assert_eq!(env.storage.get_inode_by_data_handle(file2_handle).unwrap(), None);
    }

    #[tokio::test]
    async fn recursive_delete_extent_file_cleans_namespace_layout_and_owner_once() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let parent = InodeId::new(4200);
        let dir = InodeId::new(4201);
        let file = InodeId::new(4202);
        let data_handle_id = DataHandleId::new(4202);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        put_dir(&env, env.root_inode_id, "parent", parent);
        put_dir(&env, parent, "dir", dir);
        put_extent_file(&env, dir, "file", file, data_handle_id, block_id, 64);
        let delete_header = header(142);
        let first = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: delete_header.clone(),
                path: "/mnt/test/parent/dir".to_string(),
                recursive: true,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        assert_success_header(first.header);
        assert_eq!(env.storage.get_dentry(parent, "dir").unwrap(), None);
        assert!(env.storage.get_inode(file).unwrap().is_none());
        assert!(env.storage.get_layout(file).is_err());
        assert_eq!(env.storage.get_inode_by_data_handle(data_handle_id).unwrap(), None);

        let moved_parent = FileSystemServiceProto::rename(
            &env.service,
            Request::new(RenameRequestProto {
                header: header(142),
                src_path: "/mnt/test/parent".to_string(),
                dst_path: "/mnt/test/moved-parent".to_string(),
                flags: 0,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(moved_parent.header);

        let replay = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: delete_header,
                path: "/mnt/test/parent/dir".to_string(),
                recursive: true,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        assert_success_header(replay.header);
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "parent").unwrap(), None);
        assert_eq!(
            env.storage.get_dentry(env.root_inode_id, "moved-parent").unwrap(),
            Some(parent)
        );
    }

    #[tokio::test]
    async fn recursive_delete_rejects_active_write_session_without_half_delete() {
        let env = build_env_with_raft_and_workers(
            "/mnt/test",
            DataIoPolicy::Allow,
            Some(worker_manager_for_write_targets()),
        )
        .await;
        let dir = InodeId::new(4301);
        let empty_subdir = InodeId::new(4302);
        put_dir(&env, env.root_inode_id, "dir", dir);
        put_dir(&env, dir, "empty_subdir", empty_subdir);

        let create = FileSystemServiceProto::create_file(
            &env.service,
            Request::new(CreateFileRequestProto {
                header: header(143),
                path: "/mnt/test/dir/file".to_string(),
                attrs: Some(beryl_proto::fs::FileAttrsProto {
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    ..Default::default()
                }),
                layout: Some(beryl_proto::common::FileLayoutProto {
                    block_size: 4096,
                    chunk_size: 4096,
                    replication: 1,
                    block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
                }),
                create_mode: CreateModeProto::CreateNew as i32,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(create.header);
        let data_handle_id = DataHandleId::new(create.data_handle_id.expect("data handle").value);
        let open = FileSystemServiceProto::open_write(
            &env.service,
            Request::new(OpenWriteRequestProto {
                header: header(144),
                path: "/mnt/test/dir/file".to_string(),
                mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
                desired_len: Some(128),
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(open.header);
        let write_handle = open.write_handle.expect("write handle");
        let file_inode_id = env
            .storage
            .get_inode_by_data_handle(data_handle_id)
            .unwrap()
            .expect("created inode owner");
        assert!(env.session_registry.get_session(write_handle.handle_id).is_some());

        let response = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: header(145),
                path: "/mnt/test/dir".to_string(),
                recursive: true,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let err = header_error(response.header);
        assert_fs_errno(&err, FsErrnoProto::FsErrnoEbusy);
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir").unwrap(), Some(dir));
        assert_eq!(env.storage.get_dentry(dir, "empty_subdir").unwrap(), Some(empty_subdir));
        assert_eq!(env.storage.get_dentry(dir, "file").unwrap(), Some(file_inode_id));
        assert!(env.storage.get_inode(dir).unwrap().is_some());
        assert!(env.storage.get_inode(empty_subdir).unwrap().is_some());
        assert!(env.storage.get_inode(file_inode_id).unwrap().is_some());
        assert!(env.storage.get_layout(file_inode_id).is_ok());
        assert_eq!(
            env.storage.get_inode_by_data_handle(data_handle_id).unwrap(),
            Some(file_inode_id)
        );
        assert!(env.session_registry.get_session(write_handle.handle_id).is_some());
    }

    #[tokio::test]
    async fn recursive_delete_rejects_root_or_mount_root_without_mutation() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;

        let mount_root_response = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: header(145),
                path: "/mnt/test".to_string(),
                recursive: true,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let err = header_error(mount_root_response.header);
        assert_fs_errno(&err, FsErrnoProto::FsErrnoEinval);
        assert!(env.storage.get_inode(env.root_inode_id).unwrap().is_some());

        let root_env = build_env_with_raft("/", DataIoPolicy::Allow).await;
        let root_response = FileSystemServiceProto::delete(
            &root_env.service,
            Request::new(DeleteRequestProto {
                header: header(148),
                path: "/".to_string(),
                recursive: true,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let err = header_error(root_response.header);
        assert_fs_errno(&err, FsErrnoProto::FsErrnoEinval);
        assert!(root_env.storage.get_inode(root_env.root_inode_id).unwrap().is_some());
    }

    #[tokio::test]
    async fn recursive_delete_rejects_cross_mount_subtree_without_half_delete() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let dir = InodeId::new(4401);
        let child_mount_root = InodeId::new(4402);
        put_dir(&env, env.root_inode_id, "dir", dir);
        let child_mount = env
            .mount_table
            .create_mount(
                "/mnt/test/dir/mnt".to_string(),
                MountKind::External,
                Some("file:///tmp/mnt_test_dir_mnt".to_string()),
                DataIoPolicy::Allow,
                group_name("root"),
                child_mount_root,
            )
            .expect("create child mount");
        env.storage
            .put_inode(&Inode::new_dir(
                child_mount_root,
                FileAttrs::new(),
                child_mount.mount_id,
            ))
            .expect("put child mount root inode");
        env.storage
            .put_dentry(dir, "mnt", child_mount_root)
            .expect("put child mount dentry");

        let response = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: header(146),
                path: "/mnt/test/dir".to_string(),
                recursive: true,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let err = header_error(response.header);
        assert_fs_errno(&err, FsErrnoProto::FsErrnoExdev);
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir").unwrap(), Some(dir));
        assert_eq!(env.storage.get_dentry(dir, "mnt").unwrap(), Some(child_mount_root));
        assert!(env.storage.get_inode(dir).unwrap().is_some());
        assert!(env.storage.get_inode(child_mount_root).unwrap().is_some());
    }

    #[tokio::test]
    async fn recursive_delete_fingerprint_mismatch_does_not_mutate_second_tree() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let dir1 = InodeId::new(4501);
        let dir2 = InodeId::new(4502);
        put_dir(&env, env.root_inode_id, "dir1", dir1);
        put_dir(&env, env.root_inode_id, "dir2", dir2);
        let delete_header = header(147);

        let first = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: delete_header.clone(),
                path: "/mnt/test/dir1".to_string(),
                recursive: true,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();
        assert_success_header(first.header);

        let mismatch = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: delete_header,
                path: "/mnt/test/dir2".to_string(),
                recursive: true,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let err = header_error(mismatch.header);
        assert_fs_errno(&err, FsErrnoProto::FsErrnoEinval);
        assert!(err.message.contains("reused with different command payload"));
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir1").unwrap(), None);
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir2").unwrap(), Some(dir2));
        assert!(env.storage.get_inode(dir2).unwrap().is_some());
    }

    #[tokio::test]
    async fn delete_regular_empty_file_success_removes_namespace_layout_and_data_owner() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let file_inode_id = InodeId::new(5001);
        let data_handle_id = DataHandleId::new(5001);
        let parent = env
            .storage
            .get_inode(env.root_inode_id)
            .expect("load parent inode")
            .expect("parent inode must exist");
        let file_inode = Inode::new_file(file_inode_id, FileAttrs::new(), env.mount_id, data_handle_id);
        let layout = FileLayout::new(4096, 4096, 1);
        env.storage
            .create_file_atomic(env.root_inode_id, "file", &file_inode, &parent, layout)
            .expect("create empty file");

        let response = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: header(15),
                path: "/mnt/test/file".to_string(),
                recursive: false,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        assert_success_header(response.header);
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "file").unwrap(), None);
        assert!(env.storage.get_inode(file_inode_id).unwrap().is_none());
        assert!(env.storage.get_layout(file_inode_id).is_err());
        assert_eq!(env.storage.get_inode_by_data_handle(data_handle_id).unwrap(), None);
    }

    #[tokio::test]
    async fn delete_empty_dir_success_removes_namespace_and_inode() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let dir_inode_id = InodeId::new(6001);
        env.storage
            .put_inode(&Inode::new_dir(dir_inode_id, FileAttrs::new(), env.mount_id))
            .expect("put empty directory inode");
        env.storage
            .put_dentry(env.root_inode_id, "dir", dir_inode_id)
            .expect("put empty directory dentry");

        let request = DeleteRequestProto {
            header: header(16),
            path: "/mnt/test/dir".to_string(),
            recursive: false,
        };
        let response = FileSystemServiceProto::delete(&env.service, Request::new(request.clone()))
            .await
            .expect("transport status must remain OK")
            .into_inner();

        assert_success_header(response.header);
        let replay = FileSystemServiceProto::delete(&env.service, Request::new(request))
            .await
            .expect("transport status must remain OK")
            .into_inner();
        assert_success_header(replay.header);
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir").unwrap(), None);
        assert!(env.storage.get_inode(dir_inode_id).unwrap().is_none());
        assert!(env.storage.get_inode(env.root_inode_id).unwrap().is_some());
    }

    #[tokio::test]
    async fn delete_non_empty_dir_recursive_false_returns_structured_error_without_half_delete() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let dir_inode_id = InodeId::new(7001);
        let child_inode_id = InodeId::new(7002);
        env.storage
            .put_inode(&Inode::new_dir(dir_inode_id, FileAttrs::new(), env.mount_id))
            .expect("put directory inode");
        env.storage
            .put_dentry(env.root_inode_id, "dir", dir_inode_id)
            .expect("put directory dentry");
        env.storage
            .put_inode(&Inode::new_file(
                child_inode_id,
                FileAttrs::new(),
                env.mount_id,
                DataHandleId::new(7002),
            ))
            .expect("put child inode");
        env.storage
            .put_dentry(dir_inode_id, "child", child_inode_id)
            .expect("put child dentry");

        let response = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: header(17),
                path: "/mnt/test/dir".to_string(),
                recursive: false,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        let err = header_error(response.header);
        assert_fs_errno(&err, FsErrnoProto::FsErrnoEnotempty);
        assert_eq!(
            env.storage.get_dentry(env.root_inode_id, "dir").unwrap(),
            Some(dir_inode_id)
        );
        assert!(env.storage.get_inode(dir_inode_id).unwrap().is_some());
        assert_eq!(
            env.storage.get_dentry(dir_inode_id, "child").unwrap(),
            Some(child_inode_id)
        );
        assert!(env.storage.get_inode(child_inode_id).unwrap().is_some());
    }

    #[tokio::test]
    async fn delete_symlink_success_preserves_data_handle_owner_zero_mapping() {
        let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow).await;
        let symlink_inode_id = InodeId::new(8001);
        let sentinel_owner_inode_id = InodeId::new(8002);
        let symlink_inode = Inode::new_symlink(
            symlink_inode_id,
            FileAttrs::new(),
            "/mnt/test/target".to_string(),
            env.mount_id,
        );
        env.storage.put_inode(&symlink_inode).expect("put symlink inode");
        env.storage
            .put_dentry(env.root_inode_id, "link", symlink_inode_id)
            .expect("put symlink dentry");
        env.storage
            .put_data_handle_owner(DataHandleId::new(0), sentinel_owner_inode_id)
            .expect("put sentinel owner mapping");

        let response = FileSystemServiceProto::delete(
            &env.service,
            Request::new(DeleteRequestProto {
                header: header(18),
                path: "/mnt/test/link".to_string(),
                recursive: false,
            }),
        )
        .await
        .expect("transport status must remain OK")
        .into_inner();

        assert_success_header(response.header);
        assert_eq!(env.storage.get_dentry(env.root_inode_id, "link").unwrap(), None);
        assert!(env.storage.get_inode(symlink_inode_id).unwrap().is_none());
        assert_eq!(
            env.storage.get_inode_by_data_handle(DataHandleId::new(0)).unwrap(),
            Some(sentinel_owner_inode_id)
        );
    }
}
