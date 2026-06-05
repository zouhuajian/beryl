// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::path::{Component, Path, PathBuf};

use common::{CoreConfig, FlatConfig};

#[test]
fn repository_core_site_store_dirs_do_not_overlap() {
    assert_store_dirs_do_not_overlap(
        &repo_root().join("conf/core-site.yaml"),
        Path::new("data/metadata"),
        Path::new("data/worker/hdd0"),
        Path::new("data/worker/worker.identity"),
    );
    assert_store_dirs_do_not_overlap(
        &repo_root().join("conf/local/core-site.yaml"),
        Path::new("./data/metadata"),
        Path::new("./data/worker/hdd0"),
        Path::new("./data/worker/worker.identity"),
    );
}

fn assert_store_dirs_do_not_overlap(
    config_path: &Path,
    expected_metadata_dir: &Path,
    expected_worker_root: &Path,
    expected_identity_path: &Path,
) {
    let config = CoreConfig::load(config_path).expect("core config loads");
    let flat = config.as_flat();
    let metadata_dir = required_path(flat, "metadata.storage.dir");
    let worker_root = required_store_dir(flat);
    let identity_path = required_path(flat, "worker.identity.path");

    assert_eq!(metadata_dir, expected_metadata_dir);
    assert_eq!(worker_root, expected_worker_root);
    assert_eq!(identity_path, expected_identity_path);
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
    let path_keys: Vec<String> = flat
        .keys()
        .filter(|key| key.starts_with("worker.store.dirs.") && key.ends_with(".path"))
        .cloned()
        .collect();
    assert_eq!(
        path_keys.len(),
        1,
        "repository config must define one worker store dir path"
    );
    PathBuf::from(
        flat.get_str(&path_keys[0])
            .expect("repository worker.store.dirs.<dir_id>.path must be a string"),
    )
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
