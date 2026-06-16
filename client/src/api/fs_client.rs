// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public filesystem-facing facade.

use std::fmt;
use std::sync::Arc;

use proto::metadata::{
    DeleteRequestProto, GetStatusRequestProto, ListStatusRequestProto, OpenFileRequestProto, RenameRequestProto,
};

use super::{CreateMode, CreateOptions, DirectoryListing, FileReader, FileStatus, FileWriter, ListOptions};
use crate::api::handle::{ReadHandle, WriteHandle};
use crate::api::runtime::ClientRuntime;
use crate::config::ClientConfig;
use crate::data::WorkerDataPlane;
use crate::error::{ClientError, ClientResult};
use crate::metadata::{MetadataGateway, TonicMetadataGateway};
use crate::metrics::{ClientMetrics, NoopClientMetrics};
use crate::runtime::{BackoffSleeper, OperationKind, TokioBackoffSleeper};

pub(crate) const DEFAULT_BLOCK_SIZE: u32 = 64 * 1024 * 1024;
pub(super) const DEFAULT_CHUNK_SIZE: u32 = 4 * 1024 * 1024;
pub(crate) const DEFAULT_REPLICATION: u32 = 1;
pub(crate) const MAX_PREALLOCATED_WRITE_BLOCKS: u64 = 10;

/// Public filesystem-facing client facade.
#[derive(Clone)]
pub struct FsClient {
    /// Shared runtime state reused by this facade and the handles it opens.
    pub(crate) runtime: Arc<ClientRuntime>,
}

impl FsClient {
    /// Create a new filesystem client facade.
    pub fn new(config: ClientConfig) -> Self {
        Self::try_new(config).expect("valid client metadata configuration")
    }

    /// Create a new filesystem client facade and return configuration errors.
    pub fn try_new(config: ClientConfig) -> ClientResult<Self> {
        let endpoint = config
            .metadata_endpoints
            .first()
            .cloned()
            .ok_or_else(|| ClientError::Config("client.metadata.endpoints must not be empty".to_string()))?;
        let metrics: Arc<dyn ClientMetrics> = Arc::new(NoopClientMetrics);
        let gateway = Arc::new(TonicMetadataGateway::new_lazy_with_config(
            endpoint,
            &config,
            Arc::clone(&metrics),
        )?);
        let data_plane = WorkerDataPlane::from_config(&config, Arc::clone(&metrics));

        Self::with_runtime_hooks(config, gateway, data_plane, Arc::new(TokioBackoffSleeper), metrics)
    }

    /// Builds a client with injected runtime dependencies for tests and internal wiring.
    pub(crate) fn with_runtime_hooks(
        config: ClientConfig,
        gateway: Arc<dyn MetadataGateway>,
        data_plane: WorkerDataPlane,
        sleeper: Arc<dyn BackoffSleeper>,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        Ok(Self {
            runtime: Arc::new(ClientRuntime::with_hooks(
                config, gateway, data_plane, sleeper, metrics,
            )?),
        })
    }

    /// Return the client configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.runtime.config
    }

    /// Return file or directory status through the metadata runtime.
    pub async fn stat(&self, path: &str) -> ClientResult<FileStatus> {
        validate_path(path)?;
        let response = self
            .runtime
            .executor
            .get_status(
                path,
                GetStatusRequestProto {
                    header: None,
                    path: path.to_string(),
                },
            )
            .await?;
        FileStatus::from_proto(path, response)
    }

    /// Lists a directory using explicit pagination options.
    pub async fn list(&self, path: &str, options: ListOptions) -> ClientResult<DirectoryListing> {
        validate_path(path)?;
        let response = self
            .runtime
            .executor
            .list_status(
                path,
                ListStatusRequestProto {
                    header: None,
                    path: path.to_string(),
                    recursive: options.recursive,
                    cursor: options.cursor.unwrap_or_default(),
                    limit: options.limit.unwrap_or(0),
                },
            )
            .await?;
        Ok(DirectoryListing::from_proto(path, response))
    }

    /// Delete a file, symlink, or directory through the metadata runtime.
    pub async fn delete(&self, path: &str, recursive: bool) -> ClientResult<()> {
        validate_path(path)?;
        self.runtime
            .executor
            .delete(
                path,
                DeleteRequestProto {
                    header: None,
                    path: path.to_string(),
                    recursive,
                },
            )
            .await
            .map(|_| ())
    }

    /// Rename a namespace entry through the metadata runtime.
    pub async fn rename(&self, src: &str, dst: &str) -> ClientResult<()> {
        validate_path(src)?;
        validate_path(dst)?;
        self.runtime
            .executor
            .rename(
                src,
                dst,
                RenameRequestProto {
                    header: None,
                    src_path: src.to_string(),
                    dst_path: dst.to_string(),
                    flags: 0,
                },
            )
            .await
            .map(|_| ())
    }

    /// Opens an existing file for reads and returns a file reader.
    ///
    /// Existing files use the metadata-stored `FileLayout`; there are no
    /// public read-open options until they carry real behavior.
    pub async fn open(&self, path: &str) -> ClientResult<FileReader> {
        validate_path(path)?;
        let response = self
            .runtime
            .executor
            .open_file(
                path,
                OpenFileRequestProto {
                    header: None,
                    path: path.to_string(),
                    range: None,
                    include_locations: false,
                },
            )
            .await?;
        let handle = ReadHandle::from_open_response(path, response)?;
        Ok(FileReader::new(Arc::clone(&self.runtime), handle))
    }

    /// Creates a file write session according to the supplied creation options.
    ///
    /// `CreateOptions` layout fields are create-time intent for new file
    /// creation. Metadata validates and persists the accepted `FileLayout`.
    pub async fn create(&self, path: &str, options: CreateOptions) -> ClientResult<FileWriter> {
        validate_path(path)?;
        let create_mode = match options.create_mode {
            CreateMode::CreateNew => proto::metadata::CreateModeProto::CreateNew,
            CreateMode::CreateOrOverwrite => proto::metadata::CreateModeProto::CreateOrOverwrite,
        };
        let response = match self
            .runtime
            .executor
            .create_file(
                path,
                proto::metadata::CreateFileRequestProto {
                    header: None,
                    path: path.to_string(),
                    attrs: Some(default_file_attrs()),
                    layout: Some(layout_for_new_file(&options)),
                    create_mode: create_mode as i32,
                    desired_len: Some(default_write_preallocation_len()),
                },
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                return Err(self
                    .runtime
                    .normalize_unknown_outcome("CreateFile", OperationKind::MetadataMutation, err));
            }
        };
        Ok(FileWriter::new(
            Arc::clone(&self.runtime),
            WriteHandle::from_create_response(path, response)?,
        ))
    }

    /// Opens an append write session for an existing file.
    ///
    /// Append uses the metadata-stored `FileLayout` and does not send a new
    /// layout override.
    pub async fn append(&self, path: &str) -> ClientResult<FileWriter> {
        validate_path(path)?;
        let response = match self
            .runtime
            .executor
            .append_file(
                path,
                proto::metadata::AppendFileRequestProto {
                    header: None,
                    path: path.to_string(),
                    desired_len: Some(default_write_preallocation_len()),
                },
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                return Err(self
                    .runtime
                    .normalize_unknown_outcome("AppendFile", OperationKind::MetadataMutation, err));
            }
        };
        Ok(FileWriter::new(
            Arc::clone(&self.runtime),
            WriteHandle::from_append_response(path, response)?,
        ))
    }
}

impl fmt::Debug for FsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FsClient")
            .field("config", &self.runtime.config)
            .field("executor", &self.runtime.executor)
            .field("data_plane", &self.runtime.data_plane)
            .finish_non_exhaustive()
    }
}

pub(crate) fn validate_path(path: &str) -> ClientResult<()> {
    if path.is_empty() {
        Err(ClientError::InvalidArgument("path must not be empty".to_string()))
    } else {
        Ok(())
    }
}

fn default_write_preallocation_len() -> u64 {
    u64::from(DEFAULT_BLOCK_SIZE) * MAX_PREALLOCATED_WRITE_BLOCKS
}

fn default_file_attrs() -> proto::fs::FileAttrsProto {
    proto::fs::FileAttrsProto {
        mode: 0o644,
        uid: 0,
        gid: 0,
        size: 0,
        atime_ms: 0,
        mtime_ms: 0,
        ctime_ms: 0,
        nlink: 1,
    }
}

fn layout_for_new_file(options: &CreateOptions) -> proto::common::FileLayoutProto {
    proto::common::FileLayoutProto {
        block_size: options.block_size,
        chunk_size: options.chunk_size,
        replication: DEFAULT_REPLICATION,
        block_format_id: options.block_format_id.as_raw(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fs_client_creates_runtime_identity_from_client_name_config() {
        let default_client = FsClient::try_new(ClientConfig::default()).expect("default client");
        assert!(!default_client.runtime.executor.client_id().is_zero());
        assert_eq!(
            default_client.runtime.executor.client_name(),
            crate::config::DEFAULT_CLIENT_NAME
        );

        let mut flat = common::FlatConfig::new();
        flat.set("client.name", "prod_ns01");
        let config = ClientConfig::from_flat(flat).expect("config");
        let named_client = FsClient::try_new(config).expect("named client");

        assert!(!named_client.runtime.executor.client_id().is_zero());
        assert_eq!(named_client.runtime.executor.client_name(), "prod_ns01");
    }
}
