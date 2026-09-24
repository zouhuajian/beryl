// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared routing and Raft proposal boundary for filesystem writes.

use super::{fs_failure_from_metadata_error, FsFailure, MetadataFileSystem, RequestHeader};
use crate::error::{MetadataError, MetadataResult};
use crate::mount::MountEntry;
use crate::observe;
use crate::raft::{ApplySuccess, Command};
use crate::session_registry::WritePublication;
use beryl_types::ids::{BlockId, InodeId, MountId};
use beryl_types::{ContentGeneration, GroupName, LeaseEpoch};
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

impl MetadataFileSystem {
    pub(super) fn route_ctx_for_write(
        &self,
        req_ctx: &RequestHeader,
        parent_inode_id: InodeId,
    ) -> Result<MountEntry, FsFailure> {
        self.route_ctx_for_write_with_error_hints(req_ctx, parent_inode_id, None)
    }

    pub(super) fn route_ctx_for_write_with_error_hints(
        &self,
        req_ctx: &RequestHeader,
        parent_inode_id: InodeId,
        error_group_name: Option<GroupName>,
    ) -> Result<MountEntry, FsFailure> {
        let ctx = match self.storage.get_inode(parent_inode_id).and_then(|inode| {
            let inode =
                inode.ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {parent_inode_id}")))?;
            self.route_fs_write_ctx(inode.mount_id)
        }) {
            Ok(ctx) => ctx,
            Err(err) => {
                return Err(fs_failure_from_metadata_error(req_ctx, err, error_group_name));
            }
        };

        Ok(ctx)
    }

    pub(super) fn route_fs_write_ctx(&self, mount_id: MountId) -> MetadataResult<MountEntry> {
        let mount_entry = self
            .mount_table
            .get_mount(mount_id)
            .ok_or_else(|| MetadataError::NotFound(format!("Mount not found: {:?}", mount_id)))?;

        debug!(
            mount_id = %mount_id.as_raw(),
            owner_group_name = %mount_entry.namespace_owner_group_name,
            "FS write routed to mount namespace owner group"
        );

        Ok(mount_entry)
    }

    /// Propose one filesystem command and record its fully validated outcome.
    ///
    /// The Raft node has already converted committed application rejections to
    /// `MetadataError`. `decode_success` must accept only the exact success
    /// variant expected by the submitted command, so FS metrics cannot report
    /// success before that invariant is checked.
    pub(super) async fn propose_fs_write_command<T>(
        &self,
        command: Command,
        decode_success: impl FnOnce(ApplySuccess) -> MetadataResult<T>,
    ) -> MetadataResult<T> {
        let started = Instant::now();
        let operation_name = command.operation_name();
        let result = match self.propose_write_command(command).await {
            Ok(success) => decode_success(success),
            Err(error) => Err(error),
        };
        record_fs_write_result(operation_name, started, &result);
        result
    }

    /// Allocate the next durable block ordinal for one exact inode and lease epoch.
    pub(super) async fn propose_block_allocation(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
    ) -> MetadataResult<BlockId> {
        let command = Command::AllocateBlock { inode_id, lease_epoch };
        self.propose_fs_write_command(command, |success| match success {
            ApplySuccess::BlockAllocated(block_id) => Ok(block_id),
            unexpected => Err(unexpected_raft_apply_success("AllocateBlock", unexpected)),
        })
        .await
    }

    /// Keep submitted visibility changes alive independently of the RPC waiter.
    ///
    /// The session pins unpublished blocks and namespace exclusion until apply
    /// finishes or an ordered lease fence rules out a delayed publication.
    pub(super) async fn propose_file_publication(
        &self,
        command: Command,
        mut publication: WritePublication,
    ) -> MetadataResult<ContentGeneration> {
        let (inode_id, lease_epoch, file_len, closes) = match &command {
            Command::CommitFile {
                inode_id, publication, ..
            } => (*inode_id, publication.lease_epoch, publication.target_len, true),
            Command::PublishFile {
                inode_id, publication, ..
            } => (*inode_id, publication.lease_epoch, publication.target_len, false),
            _ => unreachable!("file publication command required"),
        };
        publication.mark_submitted().map_err(MetadataError::Again)?;
        let operation_name = command.operation_name();
        let fence = self.propose_write_command(Command::EndWriteLease { inode_id, lease_epoch });
        let proposal = self.propose_write_command(command);
        tokio::spawn(async move {
            let started = Instant::now();
            let result = match proposal.await {
                Ok(ApplySuccess::FileCommitted { generation }) if closes => Ok(generation),
                Ok(ApplySuccess::FilePublished { generation }) if !closes => Ok(generation),
                Ok(unexpected) => Err(unexpected_raft_apply_success(operation_name, unexpected)),
                Err(error) => Err(error),
            };
            let result = match result {
                Ok(generation) => {
                    if closes {
                        publication.complete_commit();
                        Ok(generation)
                    } else {
                        publication
                            .complete_sync(generation, file_len)
                            .map(|()| generation)
                            .map_err(MetadataError::Internal)
                    }
                }
                Err(error) => {
                    // Transport or apply-worker failure may leave the mutation
                    // queued. Only ordered authority may release its GC pin.
                    if matches!(fence.await, Ok(ApplySuccess::WriteLeaseEnded)) {
                        publication.complete_commit();
                    }
                    Err(error)
                }
            };
            record_fs_write_result(operation_name, started, &result);
            result
        })
        .await
        .map_err(|error| MetadataError::Internal(format!("file publication task failed: {error}")))?
    }

    /// Own the proposal dependencies so submitted publication can outlive its RPC waiter.
    fn propose_write_command(
        &self,
        command: Command,
    ) -> impl std::future::Future<Output = MetadataResult<ApplySuccess>> + Send + 'static {
        let raft_node = Arc::clone(&self.raft_node);
        async move { raft_node.propose(command).await }
    }
}

fn record_fs_write_result<T>(operation_name: &'static str, started: Instant, result: &MetadataResult<T>) {
    match result {
        Ok(_) => {
            observe::record_fs_op(operation_name, "ok", "none", started.elapsed().as_secs_f64());
        }
        Err(error) => {
            observe::record_fs_op(
                operation_name,
                "error",
                observe::metadata_error_kind(error),
                started.elapsed().as_secs_f64(),
            );
        }
    }
}

/// Fail closed when Raft returns a success variant for a different command.
pub(super) fn unexpected_raft_apply_success(operation_name: &'static str, success: ApplySuccess) -> MetadataError {
    MetadataError::Internal(format!(
        "{operation_name} Raft command returned unexpected success: {success:?}"
    ))
}
