// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::path::PathBuf;

use beryl_worker::config::WorkerConfig;

#[test]
fn repository_worker_config_defines_shutdown_contract() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("Worker lives under workspace root")
        .to_path_buf();
    let config = WorkerConfig::load(repo_root.join("conf/worker.yaml")).expect("Worker config loads");

    assert_eq!(config.shutdown_timeout_ms, 30_000);
    assert_eq!(config.rpc_port, 19090);
    assert_eq!(config.http_port, 19091);
}
