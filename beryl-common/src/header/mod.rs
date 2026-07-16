// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Request and Response Headers for RPC calls.
//!
//! This module provides Rust domain types that correspond to protobuf header messages.
//! Conversions between these types and proto types are implemented in beryl_proto::convert.
//!
//! ## Import Paths
//!
//! - **Internal domain side**: `beryl_common::header::RequestHeader` (this module)
//! - **Protobuf side**: `beryl_proto::common::RequestHeaderProto`
//!
//! ## Conversions
//!
//! Conversions between `beryl_proto::common::RequestHeaderProto` and `beryl_common::header::RequestHeader`
//! are implemented in `beryl_proto::convert` module:
//!
//! - `TryFrom<beryl_proto::common::RequestHeaderProto> for RequestHeader`
//! - `From<&RequestHeader> for beryl_proto::common::RequestHeaderProto`
//!
//! These are the **authoritative** implementations. Do not implement these
//! conversions elsewhere.

mod codec;
mod types;

#[cfg(test)]
mod tests;

pub use codec::RequestHeaderCodec;
pub use types::*;
