// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::path::{Path, PathBuf};

use metadata::MetadataConfig;

#[test]
fn repository_core_site_parses_metadata_storage_dir() {
    assert_metadata_storage_dir(&repo_root().join("conf/core-site.yaml"), Path::new("data/metadata"));
    assert_metadata_storage_dir(
        &repo_root().join("conf/local/core-site.yaml"),
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
