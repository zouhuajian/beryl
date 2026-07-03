// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Inactive worker peer client placeholders.
//!
//! This module is not declared by the current worker net surface. Current worker
//! data access uses the gRPC data server only.

mod client;
mod grpc;
mod quic;
mod rdma;
mod router;
