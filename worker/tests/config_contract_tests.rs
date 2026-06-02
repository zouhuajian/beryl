// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::path::{Path, PathBuf};

use worker::config::WorkerConfig;

#[test]
fn repository_core_site_parses_worker_storage_paths() {
    assert_worker_storage_paths(
        &repo_root().join("conf/core-site.yaml"),
        Path::new("data/worker"),
        Path::new("data/worker/worker.identity"),
    );
    assert_worker_storage_paths(
        &repo_root().join("conf/local/core-site.yaml"),
        Path::new("./data/worker"),
        Path::new("./data/worker/worker.identity"),
    );
}

fn assert_worker_storage_paths(config_path: &Path, expected_root: &Path, expected_identity_path: &Path) {
    let config = WorkerConfig::load(config_path).expect("worker config loads");

    assert_eq!(config.storage_root, expected_root);
    assert_eq!(config.identity_path, expected_identity_path);
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("worker lives under workspace root")
        .to_path_buf()
}
