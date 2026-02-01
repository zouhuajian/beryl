// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft state machine implementation for metadata service.
//!
//! This module implements a Raft-driven state machine that replaces the in-memory
//! state store with a strongly consistent, distributed state machine.

mod command;
mod log_store;
mod network;
mod node;
mod router;
mod snapshot;
mod state_machine;
mod state_machine_store;
mod storage;
mod types;

pub use command::Command;
pub use log_store::{AppLogReader, AppLogStorage};
pub use network::{Network, NetworkFactory};
pub use node::AppRaftNode;
pub use snapshot::SnapshotFile;
pub use state_machine::AppRaftStateMachine;
pub use state_machine_store::{AppSnapshotBuilder, StateMachineStorage};
pub use storage::{AppliedResult, RocksDBStorage};
pub use types::{
    AppDataResponse, AppMetadataRaftState, BlockCommandResult, CommandFingerprint, DedupKey, DeleteIntentsResult,
    FsCommandResult, FsErrnoResult, FsOkResult, LeaseCommandResult, MetadataNode, MetadataRaftTypeConfig,
    MountCommandResult, ShardGroupInfo, WorkerCommandResult,
};
