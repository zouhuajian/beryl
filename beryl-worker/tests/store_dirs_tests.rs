// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_types::ids::{BlockId, BlockIndex, DataHandleId};
use beryl_types::{BlockFormatId, GroupName, Tier, TierFree};
use beryl_worker::config::StoreDirConfig;
use beryl_worker::store::block::{ChecksumKind, CreateStagingBlockRequest, LocalBlockStore, PublishReadyRequest};
use beryl_worker::store::dirs::StoreDirs;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::time::Duration;
use tempfile::TempDir;

const BLOCK_SIZE: u64 = 4096;
const CHUNK_SIZE: u32 = 1024;

fn group_name() -> GroupName {
    GroupName::parse("root").unwrap()
}

fn block_id(index: u32) -> BlockId {
    BlockId::new(DataHandleId::new(42), BlockIndex::new(index))
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
        chunk_size: CHUNK_SIZE,
        checksum_kind: ChecksumKind::None,
        tier: Tier::Hdd,
    }
}

fn publish_req(index: u32) -> PublishReadyRequest {
    PublishReadyRequest {
        group_name: group_name(),
        block_id: block_id(index),
        effective_len: BLOCK_SIZE,
        block_stamp: 7,
    }
}

#[test]
fn single_dir_reports_configured_capacity_and_free_bytes() {
    let temp = TempDir::new().unwrap();
    let store = StoreDirs::open(
        store_dirs(vec![dir_config(temp.path().join("hdd0"), 10 * 1024)]),
        0,
        30_000,
    )
    .unwrap();

    let report = store.report().unwrap();

    assert_eq!(report.total_bytes, 10 * 1024);
    assert_eq!(report.used_bytes, 0);
    assert_eq!(report.pending_bytes, 0);
    assert_eq!(report.dirs.len(), 1);
    assert_eq!(report.dirs[0].tier, Tier::Hdd);
    assert_eq!(report.free_bytes, report.dirs[0].free_bytes);
    assert_eq!(
        report.tier_free,
        vec![TierFree {
            tier: Tier::Hdd,
            free_bytes: report.dirs[0].free_bytes,
        }]
    );
    assert!(report.free_bytes <= 10 * 1024);
}

#[test]
fn report_exposes_largest_writable_free_bytes_by_tier() {
    let temp = TempDir::new().unwrap();
    let store = StoreDirs::open(
        store_dirs(vec![
            dir_config_with("ssd0", Tier::Ssd, temp.path().join("ssd0"), 10 * 1024),
            dir_config_with("ssd1", Tier::Ssd, temp.path().join("ssd1"), 20 * 1024),
            dir_config_with("nvme0", Tier::Nvme, temp.path().join("nvme0"), 30 * 1024),
        ]),
        0,
        30_000,
    )
    .unwrap();

    let report = store.report().unwrap();

    let ssd_max = report
        .dirs
        .iter()
        .filter(|dir| dir.tier == Tier::Ssd)
        .map(|dir| dir.free_bytes)
        .max()
        .unwrap();
    let nvme_max = report
        .dirs
        .iter()
        .filter(|dir| dir.tier == Tier::Nvme)
        .map(|dir| dir.free_bytes)
        .max()
        .unwrap();
    assert_eq!(
        report.tier_free,
        vec![
            TierFree {
                tier: Tier::Nvme,
                free_bytes: nvme_max,
            },
            TierFree {
                tier: Tier::Ssd,
                free_bytes: ssd_max,
            },
        ]
    );
}

#[test]
fn capacity_reserve_space_and_pending_reduce_free_bytes() {
    let temp = TempDir::new().unwrap();
    let store = StoreDirs::open(
        store_dirs(vec![dir_config(temp.path().join("hdd0"), 10 * 1024)]),
        1024,
        30_000,
    )
    .unwrap();
    let before = store.report().unwrap().free_bytes;

    store.create_staging_block(staging_req(0)).unwrap();
    let after_pending = store.report().unwrap();

    assert_eq!(after_pending.pending_bytes, BLOCK_SIZE);
    assert!(after_pending.free_bytes <= before.saturating_sub(BLOCK_SIZE));
}

#[test]
fn publish_and_abort_release_pending_reservations() {
    let temp = TempDir::new().unwrap();
    let store = StoreDirs::open(
        store_dirs(vec![dir_config(temp.path().join("hdd0"), 32 * 1024)]),
        0,
        30_000,
    )
    .unwrap();

    store.create_staging_block(staging_req(0)).unwrap();
    store
        .write_at(&group_name(), block_id(0), 0, Bytes::from(vec![1; BLOCK_SIZE as usize]))
        .unwrap();
    store.publish_ready(publish_req(0)).unwrap();
    let after_publish = store.report().unwrap();
    assert_eq!(after_publish.pending_bytes, 0);
    assert_eq!(after_publish.used_bytes, BLOCK_SIZE);
    assert_eq!(after_publish.dirs[0].block_count, 1);

    store.create_staging_block(staging_req(1)).unwrap();
    store.abort_staging_block(&group_name(), block_id(1)).unwrap();
    let after_abort = store.report().unwrap();
    assert_eq!(after_abort.pending_bytes, 0);
    assert_eq!(after_abort.used_bytes, BLOCK_SIZE);
    assert_eq!(after_abort.dirs[0].block_count, 1);
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
fn same_tier_multi_dir_round_robin_uses_each_dir() {
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
    store.create_staging_block(staging_req(1)).unwrap();

    let reports = store.report().unwrap().dirs;
    assert_eq!(reports[0].pending_bytes, BLOCK_SIZE);
    assert_eq!(reports[1].pending_bytes, BLOCK_SIZE);
}

#[test]
fn requested_tier_filters_store_dirs() {
    let temp = TempDir::new().unwrap();
    let store = StoreDirs::open(
        store_dirs(vec![dir_config(temp.path().join("hdd0"), 32 * 1024)]),
        0,
        30_000,
    )
    .unwrap();
    let mut req = staging_req(0);
    req.tier = Tier::Ssd;

    let err = store.create_staging_block(req).unwrap_err();

    assert!(err.to_string().contains("SSD"));
}

#[test]
fn report_excludes_failed_dir_and_keeps_healthy_same_mount_capacity() {
    let temp = TempDir::new().unwrap();
    let failed_path = temp.path().join("ssd0");
    let healthy_path = temp.path().join("ssd1");
    let store = StoreDirs::open(
        store_dirs(vec![
            dir_config_with("ssd0", Tier::Ssd, failed_path.clone(), 64 * 1024),
            dir_config_with("ssd1", Tier::Ssd, healthy_path, 64 * 1024),
        ]),
        0,
        1,
    )
    .unwrap();
    std::fs::remove_dir_all(&failed_path).unwrap();
    wait_for_refresh();

    let report = store.report().expect("report should degrade per failed dir");

    let failed = report.dirs.iter().find(|dir| dir.id == "ssd0").unwrap();
    let healthy = report.dirs.iter().find(|dir| dir.id == "ssd1").unwrap();
    assert!(!failed.writable);
    assert_eq!(failed.fs_free_bytes, 0);
    assert_eq!(failed.free_bytes, 0);
    assert!(failed.error.as_deref().unwrap_or_default().contains("ssd0"));
    assert!(healthy.writable);
    assert_eq!(healthy.error, None);
    assert!(healthy.free_bytes > 0);
    assert_eq!(report.free_bytes, healthy.free_bytes);
    assert_eq!(
        report.tier_free,
        vec![TierFree {
            tier: Tier::Ssd,
            free_bytes: healthy.free_bytes,
        }]
    );
}

#[test]
fn report_succeeds_with_zero_capacity_when_only_dir_fails() {
    let temp = TempDir::new().unwrap();
    let failed_path = temp.path().join("hdd0");
    let store = StoreDirs::open(store_dirs(vec![dir_config(failed_path.clone(), 64 * 1024)]), 0, 1).unwrap();
    let initial = store.report().unwrap();
    assert!(initial.free_bytes > 0);
    std::fs::remove_dir_all(&failed_path).unwrap();
    wait_for_refresh();

    let report = store.report().expect("single failed dir should still report");

    assert_eq!(report.free_bytes, 0);
    assert!(report.tier_free.is_empty());
    assert_eq!(report.dirs.len(), 1);
    assert!(!report.dirs[0].writable);
    assert_eq!(report.dirs[0].fs_free_bytes, 0);
    assert_eq!(report.dirs[0].free_bytes, 0);
    assert!(report.dirs[0].error.is_some());
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

#[test]
fn failed_higher_tier_does_not_hide_healthy_lower_tier() {
    let temp = TempDir::new().unwrap();
    let nvme_path = temp.path().join("nvme0");
    let hdd_path = temp.path().join("hdd0");
    let store = StoreDirs::open(
        store_dirs(vec![
            dir_config_with("nvme0", Tier::Nvme, nvme_path.clone(), 64 * 1024),
            dir_config_with("hdd0", Tier::Hdd, hdd_path, 64 * 1024),
        ]),
        0,
        1,
    )
    .unwrap();
    std::fs::remove_dir_all(&nvme_path).unwrap();
    wait_for_refresh();

    let report = store.report().expect("lower tier should remain reportable");
    let hdd = report.dirs.iter().find(|dir| dir.id == "hdd0").unwrap();

    assert!(hdd.writable);
    assert!(hdd.free_bytes > 0);
    assert_eq!(
        report.tier_free,
        vec![TierFree {
            tier: Tier::Hdd,
            free_bytes: hdd.free_bytes,
        }]
    );
}
