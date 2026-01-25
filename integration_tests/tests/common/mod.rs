// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::net::TcpListener;

pub mod logging;
pub mod mock_metadata;
pub mod mock_worker;

pub use logging::init_logging;
pub use mock_metadata::{create_test_file_meta, MockMetadataServer};
pub use mock_worker::MockWorkerServer;

/// Create a temporary directory for integration tests.
pub fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("failed to create temp dir")
}

/// Allocate an ephemeral TCP port (without keeping the socket open).
pub fn ephemeral_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("failed to bind ephemeral port")
        .local_addr()
        .expect("failed to read socket address")
        .port()
}
