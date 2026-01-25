// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service client.

pub mod client;
pub mod msync;
pub mod rpc_helper;

pub use client::MetadataClient;
pub use msync::MsyncClient;
pub use rpc_helper::MetadataRpcHelper;
