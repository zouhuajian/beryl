// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Conversion utility from openraft::LogId to types::RaftLogId.

use openraft::LogId;
use types::RaftLogId;

pub(crate) fn from_openraft_log_id(log_id: LogId<u64>) -> RaftLogId {
    RaftLogId::new(log_id.leader_id.term, log_id.leader_id.node_id, log_id.index)
}
