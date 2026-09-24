// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_types::ids::{BlockId, BlockIndex, InodeId};
use beryl_types::{GroupName, Tier};
use beryl_worker::config::StoreDirConfig;
use beryl_worker::store::block::{FullBlockFileStore, LocalBlockStore, ReclaimBlockRequest};
use beryl_worker::store::dirs::StoreDirs;
use beryl_worker::WorkerError;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

fn group_name() -> GroupName {
    GroupName::parse("root").unwrap()
}

fn block_id(index: u32) -> BlockId {
    BlockId::new(InodeId::new(42), BlockIndex::new(index))
}

fn dir_config(path: PathBuf, capacity_bytes: u64) -> (String, StoreDirConfig) {
    dir_config_with("hdd0", Tier::Hdd, path, capacity_bytes)
}

fn dir_config_with(id: &str, tier: Tier, path: PathBuf, capacity_bytes: u64) -> (String, StoreDirConfig) {
    (
        id.to_string(),
        StoreDirConfig {
            path,
            tier,
            capacity_bytes,
        },
    )
}

fn store_dirs(configs: Vec<(String, StoreDirConfig)>) -> BTreeMap<String, StoreDirConfig> {
    configs.into_iter().collect()
}

fn wait_for_refresh() {
    std::thread::sleep(Duration::from_millis(10));
}

#[test]
fn store_directory_has_one_process_owner() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("hdd0");
    let first = StoreDirs::open(
        group_name(),
        store_dirs(vec![dir_config(path.clone(), 32 * 1024)]),
        0,
        30_000,
    )
    .unwrap();

    assert!(matches!(
        StoreDirs::open(
            group_name(),
            store_dirs(vec![dir_config(path.clone(), 32 * 1024)]),
            0,
            30_000
        ),
        Err(WorkerError::Unavailable(_))
    ));
    drop(first);
    StoreDirs::open(group_name(), store_dirs(vec![dir_config(path, 32 * 1024)]), 0, 30_000)
        .expect("dropping the owner must release the directory lock");
}

#[test]
fn reclaim_fails_closed_on_unidentified_data_in_any_store_dir() {
    let temp = TempDir::new().unwrap();
    let hdd0 = temp.path().join("hdd0");
    let hdd1 = temp.path().join("hdd1");
    let store = StoreDirs::open(
        group_name(),
        store_dirs(vec![
            dir_config_with("hdd0", Tier::Hdd, hdd0, 32 * 1024),
            dir_config_with("hdd1", Tier::Hdd, hdd1.clone(), 32 * 1024),
        ]),
        0,
        30_000,
    )
    .unwrap();
    let raw_store = FullBlockFileStore::new(hdd1);
    let paths = raw_store.paths(&group_name(), block_id(0));
    std::fs::create_dir_all(paths.data_path.parent().unwrap()).unwrap();
    std::fs::write(&paths.data_path, b"unidentified").unwrap();
    let req = ReclaimBlockRequest {
        group_name: group_name(),
        block_id: block_id(0),
    };

    assert!(matches!(store.reclaim_block(&req), Err(WorkerError::Corrupt(_))));
    let paths = raw_store.paths(&group_name(), block_id(0));
    assert!(paths.data_path.exists());
    assert!(!paths.meta_path.exists());
}

#[test]
fn reports_do_not_convert_directory_io_errors_into_absence() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("hdd0");
    let store = StoreDirs::open(
        group_name(),
        store_dirs(vec![dir_config(path.clone(), 32 * 1024)]),
        0,
        30_000,
    )
    .unwrap();
    assert!(matches!(
        store.load_report_meta(&group_name(), block_id(0)),
        Err(WorkerError::NotFound(_))
    ));
    assert!(store.scan_group_blocks(&group_name()).unwrap().is_empty());
    // ENOTDIR is reproducible without depending on process privileges or a permission race.
    std::fs::create_dir_all(path.join("groups")).unwrap();
    std::fs::write(path.join("groups/root"), b"invalid directory").unwrap();
    assert!(!matches!(
        store.load_report_meta(&group_name(), block_id(0)),
        Ok(_) | Err(WorkerError::NotFound(_))
    ));
    assert!(store.scan_group_blocks(&group_name()).is_err());
}

#[test]
fn report_succeeds_with_zero_capacity_when_all_dirs_fail() {
    let temp = TempDir::new().unwrap();
    let nvme_path = temp.path().join("nvme0");
    let hdd_path = temp.path().join("hdd0");
    let store = StoreDirs::open(
        group_name(),
        store_dirs(vec![
            dir_config_with("nvme0", Tier::Nvme, nvme_path.clone(), 64 * 1024),
            dir_config_with("hdd0", Tier::Hdd, hdd_path.clone(), 64 * 1024),
        ]),
        0,
        1,
    )
    .unwrap();
    std::fs::remove_dir_all(&nvme_path).unwrap();
    std::fs::remove_dir_all(&hdd_path).unwrap();
    wait_for_refresh();

    let report = store.report();

    assert_eq!(report.free_bytes, 0);
    assert!(report.tier_free.is_empty());
    assert_eq!(report.dirs.iter().filter(|dir| dir.writable).count(), 0);
    assert!(report.dirs.iter().all(|dir| dir.free_bytes == 0));
}
