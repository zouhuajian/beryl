// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Snapshot codec and file lifecycle used by the OpenRaft state-machine store.

mod codec;
mod io;

pub(crate) use codec::{is_node_local_meta_key, SnapshotCodecError, SnapshotIdentity, SnapshotWriter};
pub(crate) use io::{snapshot_file_in_use, SnapshotFile};
