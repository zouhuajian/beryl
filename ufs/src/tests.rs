// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for UFS module.

use crate::registry::UfsRegistry;
use crate::spec::{BackendConfig, BackendKind, CapabilityOverrides, FsConfig};
use common::header::RequestHeader;
use tempfile::TempDir;
use types::ClientId;

#[tokio::test]
#[ignore = "requires filesystem permissions not available in default test env"]
async fn test_fs_backend_basic_ops() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path().to_string_lossy().to_string();

    let spec = crate::UfsSpec::new("test-fs", BackendKind::Fs, BackendConfig::Fs(FsConfig { root }));

    let registry = UfsRegistry::new();
    registry.upsert(spec).unwrap();

    let ufs = registry.get(&crate::UfsId::new("test-fs")).unwrap();
    let ctx = RequestHeader::new(ClientId::new(1));

    // Test write_all
    let test_data = b"Hello, World!";
    ufs.write_all("test.txt", bytes::Bytes::from_static(test_data), &ctx)
        .await
        .unwrap();

    // Test read_all
    let data = ufs.read_all("test.txt", &ctx).await.unwrap();
    assert_eq!(data.as_ref(), test_data);

    // Test read_range
    let data = ufs.read_range("test.txt", 0, 5, &ctx).await.unwrap();
    assert_eq!(data.as_ref(), b"Hello");

    let data = ufs.read_range("test.txt", 7, 5, &ctx).await.unwrap();
    assert_eq!(data.as_ref(), b"World");

    // Test stat
    let status = ufs.stat("test.txt", &ctx).await.unwrap();
    assert!(!status.is_dir);
    assert_eq!(status.size, Some(test_data.len() as u64));

    // Test exists
    assert!(ufs.exists("test.txt", &ctx).await.unwrap());
    assert!(!ufs.exists("nonexistent.txt", &ctx).await.unwrap());

    // Test mkdirs
    ufs.mkdirs("dir1/dir2", &ctx).await.unwrap();
    assert!(ufs.exists("dir1/dir2", &ctx).await.unwrap());

    // Test list
    let entries = ufs.list("", &ctx).await.unwrap();
    assert!(entries.iter().any(|e| e.path == "test.txt"));
    assert!(entries.iter().any(|e| e.path == "dir1/"));

    // Test rename
    ufs.rename("test.txt", "renamed.txt", &ctx).await.unwrap();
    assert!(!ufs.exists("test.txt", &ctx).await.unwrap());
    assert!(ufs.exists("renamed.txt", &ctx).await.unwrap());

    // Test delete
    ufs.delete("renamed.txt", false, &ctx).await.unwrap();
    assert!(!ufs.exists("renamed.txt", &ctx).await.unwrap());
}

#[tokio::test]
async fn test_registry_operations() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path().to_string_lossy().to_string();

    let registry = UfsRegistry::new();

    // Test upsert
    let spec1 = crate::UfsSpec::new(
        "fs1",
        BackendKind::Fs,
        BackendConfig::Fs(FsConfig { root: root.clone() }),
    );
    assert!(!registry.upsert(spec1).unwrap());
    assert_eq!(registry.len(), 1);

    // Test get
    let ufs1 = registry.get(&crate::UfsId::new("fs1"));
    assert!(ufs1.is_some());

    // Test upsert update
    let spec1_updated = crate::UfsSpec::new("fs1", BackendKind::Fs, BackendConfig::Fs(FsConfig { root }));
    assert!(registry.upsert(spec1_updated).unwrap());
    assert_eq!(registry.len(), 1);

    // Test remove
    assert!(registry.remove(&crate::UfsId::new("fs1")));
    assert_eq!(registry.len(), 0);
    assert!(registry.get(&crate::UfsId::new("fs1")).is_none());
}

#[tokio::test]
async fn test_registry_apply() {
    let temp_dir1 = TempDir::new().unwrap();
    let temp_dir2 = TempDir::new().unwrap();
    let root1 = temp_dir1.path().to_string_lossy().to_string();
    let root2 = temp_dir2.path().to_string_lossy().to_string();

    let registry = UfsRegistry::new();

    // Add initial instance
    let spec1 = crate::UfsSpec::new(
        "fs1",
        BackendKind::Fs,
        BackendConfig::Fs(FsConfig { root: root1.clone() }),
    );
    registry.upsert(spec1).unwrap();
    assert_eq!(registry.len(), 1);

    // Apply new set (replaces all)
    let spec2 = crate::UfsSpec::new("fs2", BackendKind::Fs, BackendConfig::Fs(FsConfig { root: root2 }));
    registry.apply(vec![spec2]).unwrap();
    assert_eq!(registry.len(), 1);
    assert!(registry.get(&crate::UfsId::new("fs1")).is_none());
    assert!(registry.get(&crate::UfsId::new("fs2")).is_some());
}

#[tokio::test]
async fn test_rename_fallback() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path().to_string_lossy().to_string();

    // Create spec with rename fallback enabled
    let spec = crate::UfsSpec::new("test-fs", BackendKind::Fs, BackendConfig::Fs(FsConfig { root }))
        .with_capability_overrides(CapabilityOverrides {
            rename_fallback_enabled: true,
        });

    let registry = UfsRegistry::new();
    registry.upsert(spec).unwrap();

    let ufs = registry.get(&crate::UfsId::new("test-fs")).unwrap();
    let ctx = RequestHeader::new(ClientId::new(1));

    // Write a file
    ufs.write_all("source.txt", bytes::Bytes::from_static(b"test data"), &ctx)
        .await
        .unwrap();

    // Rename should work (native rename for Fs backend)
    ufs.rename("source.txt", "dest.txt", &ctx).await.unwrap();
    assert!(!ufs.exists("source.txt", &ctx).await.unwrap());
    assert!(ufs.exists("dest.txt", &ctx).await.unwrap());

    // Verify content
    let data = ufs.read_all("dest.txt", &ctx).await.unwrap();
    assert_eq!(data.as_ref(), b"test data");
}

#[test]
fn test_ufs_id() {
    let id1 = crate::UfsId::new("test-id");
    let id2 = crate::UfsId::from("test-id");
    let id3 = crate::UfsId::from(String::from("test-id"));

    assert_eq!(id1, id2);
    assert_eq!(id1, id3);
    assert_eq!(id1.as_str(), "test-id");
}

#[test]
fn test_capability() {
    let fs_cap = crate::Capability::for_filesystem();
    assert!(fs_cap.supports_rename);
    assert!(fs_cap.supports_recursive_delete);
    assert!(fs_cap.supports_dir);

    let obj_cap = crate::Capability::for_object_storage();
    assert!(!obj_cap.supports_rename);
    assert!(!obj_cap.supports_recursive_delete);
    assert!(!obj_cap.supports_dir);

    let hdfs_cap = crate::Capability::for_hdfs();
    assert!(hdfs_cap.supports_rename);
    assert!(hdfs_cap.supports_recursive_delete);
    assert!(hdfs_cap.supports_dir);
}
