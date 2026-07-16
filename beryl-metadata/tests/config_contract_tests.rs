// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::path::{Path, PathBuf};

use beryl_metadata::MetadataConfig;

#[test]
fn repository_metadata_config_parses_metadata_storage_dir() {
    assert_metadata_storage_dir(&repo_root().join("conf/metadata.yaml"), Path::new("data/metadata"));
    assert_metadata_storage_dir(
        &repo_root().join("conf/local/metadata.yaml"),
        Path::new("./data/metadata"),
    );
}

fn assert_metadata_storage_dir(config_path: &Path, expected: &Path) {
    let config = MetadataConfig::load(config_path).expect("metadata config loads");

    assert_eq!(config.storage_dir, expected);
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("metadata lives under workspace root")
        .to_path_buf()
}
