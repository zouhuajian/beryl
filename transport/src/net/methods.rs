// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! gRPC method path constants for WorkerDataService.
//!
//! These constants define the full gRPC method paths used by the transport layer.
//! Format: /{package}.{Service}/{Method}

/// WorkerDataService method paths
pub mod worker_data {
    /// OpenReadStream: /worker.WorkerDataService/OpenReadStream
    pub const OPEN_READ_STREAM: &str = "/worker.WorkerDataService/OpenReadStream";

    /// ReadStream: /worker.WorkerDataService/ReadStream
    pub const READ_STREAM: &str = "/worker.WorkerDataService/ReadStream";

    /// OpenWriteStream: /worker.WorkerDataService/OpenWriteStream
    pub const OPEN_WRITE_STREAM: &str = "/worker.WorkerDataService/OpenWriteStream";

    /// WriteStream: /worker.WorkerDataService/WriteStream
    pub const WRITE_STREAM: &str = "/worker.WorkerDataService/WriteStream";

    /// CommitWrite: /worker.WorkerDataService/CommitWrite
    pub const COMMIT_WRITE: &str = "/worker.WorkerDataService/CommitWrite";

    /// AbortWrite: /worker.WorkerDataService/AbortWrite
    pub const ABORT_WRITE: &str = "/worker.WorkerDataService/AbortWrite";
}
