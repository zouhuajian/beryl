// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Response validation helpers for client operation flows.

use crate::data::{WorkerBlockSyncResult, WorkerCommitResult};
use crate::error::{invalid_response, side_effect_response_body_mismatch, ClientResult};
use crate::session::write_session::PendingBlock;

pub(crate) fn validate_commit_file_size(committed_size: u64, final_size: u64) -> ClientResult<()> {
    if committed_size < final_size {
        return Err(invalid_response(
            "CommitFile",
            format!(
                "committed_size {} is smaller than final_size {}",
                committed_size, final_size
            ),
        ));
    }
    Ok(())
}

pub(crate) fn validate_sync_write_size(synced_size: u64, target_size: u64) -> ClientResult<()> {
    if synced_size < target_size {
        return Err(invalid_response(
            "SyncWrite",
            format!(
                "synced_size {} is smaller than target_size {}",
                synced_size, target_size
            ),
        ));
    }
    Ok(())
}

pub(crate) fn validate_worker_commit_result(pending: &PendingBlock, result: WorkerCommitResult) -> ClientResult<()> {
    let expected_len = pending.written_len();
    if result.effective_len != expected_len {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!("effective_len expected {}, got {}", expected_len, result.effective_len),
        ));
    }
    if result.written_through != expected_len {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!(
                "written_through expected {}, got {}",
                expected_len, result.written_through
            ),
        ));
    }
    let expected_stamp = pending.target().block_stamp;
    if result.block_stamp != expected_stamp {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!("block_stamp expected {}, got {}", expected_stamp, result.block_stamp),
        ));
    }
    Ok(())
}

pub(crate) fn validate_worker_block_sync_result(
    pending: &PendingBlock,
    result: WorkerBlockSyncResult,
) -> ClientResult<()> {
    let expected_len = pending.written_len();
    if result.effective_len != expected_len {
        return Err(side_effect_response_body_mismatch(
            "SyncCommittedBlock",
            format!("effective_len expected {}, got {}", expected_len, result.effective_len),
        ));
    }
    let expected_stamp = pending.target().block_stamp;
    if result.block_stamp != expected_stamp {
        return Err(side_effect_response_body_mismatch(
            "SyncCommittedBlock",
            format!("block_stamp expected {}, got {}", expected_stamp, result.block_stamp),
        ));
    }
    Ok(())
}
