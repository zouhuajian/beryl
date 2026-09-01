// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_types::ids::{BlockId, BlockIndex, InodeId};
use beryl_types::{BlockFormatId, GroupName, Tier};
use beryl_worker::config::StoreDirConfig;
use beryl_worker::store::block::{ChecksumKind, CreateStagingBlockRequest, LocalBlockStore, ReclaimBlockRequest};
use beryl_worker::store::dirs::StoreDirs;
use beryl_worker::WorkerError;
use std::collections::BTreeMap;
use std::time::Duration;
use tempfile::TempDir;

const BLOCK_SIZE: u64 = 4096;

fn chunk_size() -> u32 {
    BlockFormatId::FULL_EFFECTIVE.spec().unwrap().storage_chunk_size
}

fn group_name() -> GroupName {
    GroupName::parse("root").unwrap()
}

fn block_id(index: u32) -> BlockId {
    BlockId::new(InodeId::new(42), BlockIndex::new(index))
}

fn dir_config(path: std::path::PathBuf, capacity_bytes: u64) -> (String, StoreDirConfig) {
    dir_config_with("hdd0", Tier::Hdd, path, capacity_bytes)
}

fn dir_config_with(id: &str, tier: Tier, path: std::path::PathBuf, capacity_bytes: u64) -> (String, StoreDirConfig) {
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

fn store_dir_config(path: std::path::PathBuf, tier: Tier, capacity_bytes: u64) -> StoreDirConfig {
    StoreDirConfig {
        path,
        tier,
        capacity_bytes,
    }
}

fn wait_for_refresh() {
    std::thread::sleep(Duration::from_millis(10));
}

fn staging_req(index: u32) -> CreateStagingBlockRequest {
    CreateStagingBlockRequest {
        group_name: group_name(),
        block_id: block_id(index),
        block_size: BLOCK_SIZE,
        block_format_id: BlockFormatId::FULL_EFFECTIVE,
        chunk_size: chunk_size(),
        checksum_kind: ChecksumKind::None,
        tier: Tier::Hdd,
    }
}

#[test]
fn store_directory_has_one_process_owner() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("hdd0");
    let first = StoreDirs::open(store_dirs(vec![dir_config(path.clone(), 32 * 1024)]), 0, 30_000).unwrap();

    assert!(matches!(
        StoreDirs::open(store_dirs(vec![dir_config(path.clone(), 32 * 1024)]), 0, 30_000),
        Err(WorkerError::Unavailable(_))
    ));
    drop(first);
    StoreDirs::open(store_dirs(vec![dir_config(path, 32 * 1024)]), 0, 30_000)
        .expect("dropping the owner must release the directory lock");
}

#[test]
fn reclaim_fails_closed_on_staging_artifact_in_any_store_dir() {
    let temp = TempDir::new().unwrap();
    let hdd0 = temp.path().join("hdd0");
    let hdd1 = temp.path().join("hdd1");
    let store = StoreDirs::open(
        store_dirs(vec![
            dir_config_with("hdd0", Tier::Hdd, hdd0, 32 * 1024),
            dir_config_with("hdd1", Tier::Hdd, hdd1.clone(), 32 * 1024),
        ]),
        0,
        30_000,
    )
    .unwrap();
    let raw_store = beryl_worker::store::block::FullBlockFileStore::new(
        beryl_worker::store::block::FullBlockFileStoreConfig::new(hdd1),
    );
    raw_store.create_staging_block(staging_req(0)).unwrap();
    let req = ReclaimBlockRequest {
        group_name: group_name(),
        block_id: block_id(0),
        expected_block_stamp: 7,
    };

    assert!(matches!(store.reclaim_block(&req), Err(WorkerError::Corrupt(_))));
    let paths = raw_store.paths(&group_name(), block_id(0));
    assert!(paths.staging_data_path.exists());
    assert!(paths.staging_meta_path.exists());
}

#[test]
fn create_failure_releases_pending_reservation() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("hdd0");
    let store = StoreDirs::open(store_dirs(vec![dir_config(path.clone(), 32 * 1024)]), 0, 30_000).unwrap();
    let raw_store = beryl_worker::store::block::FullBlockFileStore::new(
        beryl_worker::store::block::FullBlockFileStoreConfig::new(path),
    );
    raw_store.create_staging_block(staging_req(0)).unwrap();

    let duplicate = store.create_staging_block(staging_req(0));

    assert!(duplicate.is_err());
    assert_eq!(store.report().unwrap().pending_bytes, 0);
}

#[test]
fn duplicate_block_reservation_is_rejected_across_store_dirs() {
    let temp = TempDir::new().unwrap();
    let store = StoreDirs::open(
        store_dirs(vec![
            (
                "hdd0".to_string(),
                store_dir_config(temp.path().join("hdd0"), Tier::Hdd, 32 * 1024),
            ),
            (
                "hdd1".to_string(),
                store_dir_config(temp.path().join("hdd1"), Tier::Hdd, 32 * 1024),
            ),
        ]),
        0,
        30_000,
    )
    .unwrap();

    store.create_staging_block(staging_req(0)).unwrap();
    assert!(matches!(
        store.create_staging_block(staging_req(0)),
        Err(WorkerError::InvalidArgument(_))
    ));
    assert_eq!(store.report().unwrap().pending_bytes, BLOCK_SIZE);
}

#[test]
fn report_succeeds_with_zero_capacity_when_all_dirs_fail() {
    let temp = TempDir::new().unwrap();
    let nvme_path = temp.path().join("nvme0");
    let hdd_path = temp.path().join("hdd0");
    let store = StoreDirs::open(
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

    let report = store.report().expect("all failed dirs should still report");

    assert_eq!(report.free_bytes, 0);
    assert!(report.tier_free.is_empty());
    assert_eq!(report.dirs.iter().filter(|dir| dir.writable).count(), 0);
    assert!(report.dirs.iter().all(|dir| dir.free_bytes == 0));
    assert!(report.dirs.iter().all(|dir| dir.error.is_some()));
}
