// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service client.

pub mod client;
pub mod filesystem;
pub mod msync;
pub mod rpc_helper;

pub use client::MetadataClient;
pub use filesystem::{
    replay_policy_for_method, ActionMachine, ActionMachinePolicy, FileSystemRpc, FileSystemRpcMethod, ReplayPolicy,
    RpcOp, TonicFileSystemRpc,
};
pub use msync::MsyncClient;
pub use rpc_helper::MetadataRpcHelper;
