// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! gRPC method path constants for WorkerDataService.
//!
//! These constants define the full gRPC method paths used by the transport layer.
//! Format: /{package}.{Service}/{Method}

/// WorkerDataService method paths
pub mod worker_data {
    /// WriteChunk: /worker.WorkerDataService/WriteChunk
    pub const WRITE_CHUNK: &str = "/worker.WorkerDataService/WriteChunk";

    /// ReadChunk: /worker.WorkerDataService/ReadChunk
    pub const READ_CHUNK: &str = "/worker.WorkerDataService/ReadChunk";

    /// ReadRange: /worker.WorkerDataService/ReadRange
    pub const READ_RANGE: &str = "/worker.WorkerDataService/ReadRange";

    /// OpenReadStream: /worker.WorkerDataService/OpenReadStream
    pub const OPEN_READ_STREAM: &str = "/worker.WorkerDataService/OpenReadStream";

    /// ReadStream: /worker.WorkerDataService/ReadStream
    pub const READ_STREAM: &str = "/worker.WorkerDataService/ReadStream";

    /// OpenWriteStream: /worker.WorkerDataService/OpenWriteStream
    pub const OPEN_WRITE_STREAM: &str = "/worker.WorkerDataService/OpenWriteStream";

    /// WriteStream: /worker.WorkerDataService/WriteStream
    pub const WRITE_STREAM: &str = "/worker.WorkerDataService/WriteStream";

    /// CloseStream: /worker.WorkerDataService/CloseStream
    pub const CLOSE_STREAM: &str = "/worker.WorkerDataService/CloseStream";
}
