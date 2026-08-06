// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata Raft subsystem.
//!
//! Protocol types, node/network adapters, the application state machine, and
//! OpenRaft storage-v2 implementations are kept as separate capabilities.

mod command;
mod network;
mod node;
mod read_view;
mod response;
mod state_machine;
mod storage;
mod types;

pub(crate) use command::proposal_timestamp_ms;
pub(crate) use command::{
    Command, PublishMode, MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES, MAX_RECLAIM_DETACHED_ROOT_CANDIDATES,
    MAX_RECLAIM_DETACHED_ROOT_ENTRIES, MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
};
pub(crate) use node::AppRaftNode;
pub(crate) use read_view::{MetadataReadView, RoutingDelta};
pub(crate) use response::{ApplySuccess, DetachedRootReclaimResult};
pub(crate) use state_machine::AppRaftStateMachine;
pub(crate) use storage::{RocksDBStorage, StorageIdentity};
pub(crate) use types::AppMetadataRaftState;
