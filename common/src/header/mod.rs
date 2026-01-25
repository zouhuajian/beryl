// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Request and Response Headers for RPC calls.
//!
//! This module provides Rust domain types that correspond to protobuf header messages.
//! Conversions between these types and proto types are implemented in proto::convert.
//!
//! ## Import Paths
//!
//! - **Internal domain side**: `common::header::RequestHeader` (this module)
//! - **Protobuf side**: `proto::common::RequestHeaderProto`
//!
//! ## Conversions
//!
//! Conversions between `proto::common::RequestHeaderProto` and `common::header::RequestHeader`
//! are implemented in `proto::convert` module:
//!
//! - `TryFrom<proto::common::RequestHeaderProto> for RequestHeader`
//! - `From<&RequestHeader> for proto::common::RequestHeaderProto`
//!
//! These are the **authoritative** implementations. Do not implement these
//! conversions elsewhere.

mod codec;
mod types;

#[cfg(test)]
mod tests;

pub use codec::RequestHeaderCodec;
pub use types::*;
