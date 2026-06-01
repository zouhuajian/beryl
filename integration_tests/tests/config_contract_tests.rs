// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::path::{Component, Path, PathBuf};

use metadata::MetadataConfig;
use worker::config::WorkerConfig;

#[test]
fn repository_core_site_storage_roots_do_not_overlap() {
    assert_storage_roots_do_not_overlap(
        &repo_root().join("conf/core-site.yaml"),
        Path::new("data/metadata"),
        Path::new("data/worker"),
        Path::new("data/worker/worker.identity"),
    );
    assert_storage_roots_do_not_overlap(
        &repo_root().join("conf/local/core-site.yaml"),
        Path::new("./data/metadata"),
        Path::new("./data/worker"),
        Path::new("./data/worker/worker.identity"),
    );
}

fn assert_storage_roots_do_not_overlap(
    config_path: &Path,
    expected_metadata_dir: &Path,
    expected_worker_root: &Path,
    expected_identity_path: &Path,
) {
    let metadata = MetadataConfig::load(config_path).expect("metadata config loads");
    let worker = WorkerConfig::load(config_path).expect("worker config loads");

    assert_eq!(metadata.storage_dir, expected_metadata_dir);
    assert_eq!(worker.storage_root, expected_worker_root);
    assert_eq!(worker.identity_path, expected_identity_path);
    assert!(
        !same_or_ancestor(&metadata.storage_dir, &worker.storage_root),
        "metadata storage dir must not contain worker storage root"
    );
    assert!(
        !same_or_ancestor(&worker.storage_root, &metadata.storage_dir),
        "worker storage root must not contain metadata storage dir"
    );
    assert!(
        same_or_ancestor(&worker.storage_root, &worker.identity_path),
        "worker identity path must live under worker storage root in repository configs"
    );
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("integration_tests lives under workspace root")
        .to_path_buf()
}

fn same_or_ancestor(parent: &Path, child: &Path) -> bool {
    let parent = normalized_parts(parent);
    let child = normalized_parts(child);
    parent.len() <= child.len() && child.starts_with(&parent)
}

fn normalized_parts(path: &Path) -> Vec<PathBuf> {
    path.components()
        .filter_map(|component| match component {
            Component::Prefix(prefix) => Some(PathBuf::from(prefix.as_os_str())),
            Component::RootDir => Some(PathBuf::from(std::path::MAIN_SEPARATOR.to_string())),
            Component::CurDir => None,
            Component::ParentDir => Some(PathBuf::from("..")),
            Component::Normal(part) => Some(PathBuf::from(part)),
        })
        .collect()
}
