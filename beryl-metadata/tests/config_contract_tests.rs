// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::path::{Path, PathBuf};

use beryl_metadata::MetadataConfig;

#[test]
fn repository_metadata_configs_define_runtime_contract() {
    let config = MetadataConfig::load(repo_root().join("conf/metadata.yaml")).expect("metadata config loads");

    assert_eq!(config.storage_dir, Path::new("data/metadata"));
    assert_eq!(config.block_cleanup.scan_interval_ms, 30_000);
    assert_eq!(config.block_cleanup.reclaim_grace_ms, 300_000);
    assert_eq!(config.startup.root_readiness.timeout_ms, 120_000);
    assert_eq!(config.rpc_port, 18080);
    assert_eq!(config.http_port, 18081);
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("metadata lives under workspace root")
        .to_path_buf()
}
