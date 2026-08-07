// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::path::{Component, Path, PathBuf};

use beryl_common::{FlatConfig, ServerConfig};

#[test]
fn repository_config_store_dirs_do_not_overlap() {
    let root = repo_root();
    let metadata_config = ServerConfig::load(root.join("conf/metadata.yaml")).expect("metadata config loads");
    let worker_config = ServerConfig::load(root.join("conf/worker.yaml")).expect("worker config loads");
    let metadata_dir = required_path(metadata_config.as_flat(), "beryl.metadata.storage.dir");
    let worker_root = required_store_dir(worker_config.as_flat());
    let identity_path = required_path(worker_config.as_flat(), "beryl.worker.identity-file");

    assert_eq!(metadata_dir, Path::new("data/metadata"));
    assert_eq!(worker_root, Path::new("data/worker/hdd0"));
    assert_eq!(identity_path, Path::new("data/worker/worker.identity"));
    assert!(
        !same_or_ancestor(&metadata_dir, &worker_root),
        "metadata storage dir must not contain worker store dir"
    );
    assert!(
        !same_or_ancestor(&worker_root, &metadata_dir),
        "worker store dir must not contain metadata storage dir"
    );
    assert!(
        !same_or_ancestor(&worker_root, &identity_path),
        "worker store dir must not contain worker identity path"
    );
}

fn required_path(flat: &FlatConfig, key: &str) -> PathBuf {
    PathBuf::from(
        flat.get_str(key)
            .unwrap_or_else(|| panic!("repository config must define {key}")),
    )
}

fn required_store_dir(flat: &FlatConfig) -> PathBuf {
    let dirs = flat
        .get_mapping("beryl.worker.storage.dirs")
        .expect("repository config must define Worker storage dirs");
    let entry = dirs
        .values()
        .next()
        .expect("repository config must define one storage dir");
    assert_eq!(dirs.len(), 1, "repository config must define one storage dir");
    let path = entry
        .as_mapping()
        .and_then(|entry| entry.get(serde_yaml::Value::String("path".to_string())))
        .and_then(serde_yaml::Value::as_str)
        .expect("repository storage dir must define a path");
    PathBuf::from(path)
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("common lives under workspace root")
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
