// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public filesystem-facing facade.

use std::fmt;
use std::sync::Arc;

use super::{CreateOptions, DirectoryListing, FileReader, FileStatus, FileWriter, ListOptions};
use crate::api::path::NamespacePathBuf;
use crate::api::runtime::ClientRuntime;
use crate::config::ClientConfig;
use crate::data::WorkerDataPlane;
use crate::error::ClientResult;
use crate::metadata::{GrpcMetadataGateway, MetadataGateway};
use crate::metrics::{ClientMetrics, NoopClientMetrics};
use crate::runtime::{BackoffSleeper, MetadataTargets, OperationKind, TokioBackoffSleeper};

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
        let metrics: Arc<dyn ClientMetrics> = Arc::new(NoopClientMetrics);
        let metadata_targets = MetadataTargets::from_config(&config)?;
        let gateway = Arc::new(GrpcMetadataGateway::new_lazy_with_config(
            &config,
            Arc::clone(&metrics),
        )?);
        let data_plane = WorkerDataPlane::from_config(&config, Arc::clone(&metrics));

        Self::with_runtime_hooks(
            config,
            gateway,
            metadata_targets,
            data_plane,
            Arc::new(TokioBackoffSleeper),
            metrics,
        )
    }

    /// Builds a client with injected runtime dependencies for tests and internal wiring.
    pub(crate) fn with_runtime_hooks(
        config: ClientConfig,
        gateway: Arc<dyn MetadataGateway>,
        metadata_targets: MetadataTargets,
        data_plane: WorkerDataPlane,
        sleeper: Arc<dyn BackoffSleeper>,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        Ok(Self {
            runtime: Arc::new(ClientRuntime::new(
                config,
                gateway,
                metadata_targets,
                data_plane,
                sleeper,
                metrics,
            )?),
        })
    }

    /// Return the client configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.runtime.config
    }

    /// Return file or directory status through the metadata runtime.
    pub async fn stat(&self, path: &str) -> ClientResult<FileStatus> {
        let path = NamespacePathBuf::parse(path)?;
        self.runtime.executor.stat(path).await
    }

    /// Lists a directory using explicit pagination options.
    pub async fn list(&self, path: &str, options: ListOptions) -> ClientResult<DirectoryListing> {
        let path = NamespacePathBuf::parse(path)?;
        self.runtime.executor.list(path, options).await
    }

    /// Delete a file, symlink, or directory through the metadata runtime.
    pub async fn delete(&self, path: &str, recursive: bool) -> ClientResult<()> {
        let path = NamespacePathBuf::parse(path)?;
        self.runtime.executor.delete(path, recursive).await
    }

    /// Rename a namespace entry through the metadata runtime.
    pub async fn rename(&self, src: &str, dst: &str) -> ClientResult<()> {
        let src = NamespacePathBuf::parse(src)?;
        let dst = NamespacePathBuf::parse(dst)?;
        self.runtime.executor.rename(src, dst).await
    }

    /// Opens an existing file for reads and returns a file reader.
    ///
    /// Existing files use the metadata-stored `FileLayout`; there are no
    /// public read-open options until they carry real behavior.
    pub async fn open(&self, path: &str) -> ClientResult<FileReader> {
        let path = NamespacePathBuf::parse(path)?;
        let handle = self.runtime.executor.open_file(path).await?;
        Ok(FileReader::new(Arc::clone(&self.runtime), handle))
    }

    /// Creates a file write session according to the supplied creation options.
    ///
    /// `CreateOptions` layout fields are create-time intent for new file
    /// creation. Metadata validates and persists the accepted `FileLayout`.
    pub async fn create(&self, path: &str, options: CreateOptions) -> ClientResult<FileWriter> {
        let path = NamespacePathBuf::parse(path)?;
        let response = match self.runtime.executor.create_file(path, options).await {
            Ok(response) => response,
            Err(err) => {
                return Err(self
                    .runtime
                    .normalize_outcome_error("CreateFile", OperationKind::MetadataMutation, err));
            }
        };
        Ok(FileWriter::new(Arc::clone(&self.runtime), response))
    }

    /// Opens an append write session for an existing file.
    ///
    /// Append uses the metadata-stored `FileLayout` and does not send a new
    /// layout override.
    pub async fn append(&self, path: &str) -> ClientResult<FileWriter> {
        let path = NamespacePathBuf::parse(path)?;
        let response = match self.runtime.executor.append_file(path).await {
            Ok(response) => response,
            Err(err) => {
                return Err(self
                    .runtime
                    .normalize_outcome_error("AppendFile", OperationKind::MetadataMutation, err));
            }
        };
        Ok(FileWriter::new(Arc::clone(&self.runtime), response))
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

    #[test]
    fn try_new_uses_group_scoped_metadata_targets() {
        let mut flat = common::FlatConfig::new();
        flat.set("client.metadata.group.names", "analytics");
        flat.set("client.metadata.group.analytics.endpoints", "10.0.1.1:18080");
        let config = ClientConfig::from_flat(flat).expect("group-scoped metadata config");

        FsClient::try_new(config).expect("client should not require legacy endpoint config");
    }
}
