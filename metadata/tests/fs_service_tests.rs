// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! FS service tests.

mod common;
use common::FsTestHarness;
use metadata::error::MetadataError;
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProto;
use proto::metadata::*;
use std::collections::HashSet;
use tonic::Request;
use types::fs::{FileAttrs, InodeId, InodeKind};
use types::ids::ShardGroupId;
use types::layout::FileLayout;

/// B1: Test rename within same mount is atomic.
#[tokio::test]
#[ignore = "pending inode/data_handle alignment in fs service tests"]
async fn test_rename_same_mount_atomic() {
    let harness = FsTestHarness::new().await.unwrap();

    // Arrange: Create mount and root inode
    let (mount_id, root_inode_id) = harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    // Create file "a" in root
    let req_header = FsTestHarness::create_test_request_header();
    let mut attrs = FileAttrs::new();
    attrs.mode = 0o644;
    let layout = FileLayout::new(1024, 512, 3);

    let create_req = CreateRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        name: "a".to_string(),
        attrs: Some(proto::fs::FileAttrsProto {
            mode: attrs.mode,
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            atime_ms: attrs.atime_ms,
            mtime_ms: attrs.mtime_ms,
            ctime_ms: attrs.ctime_ms,
            nlink: attrs.nlink,
        }),
        layout: Some(proto::common::FileLayoutProto {
            block_size: layout.block_size,
            chunk_size: layout.chunk_size,
            replication: layout.replication as u32,
        }),
    };

    let create_resp = MetadataFsServiceProto::create(&harness.fs_service, Request::new(create_req))
        .await
        .unwrap()
        .into_inner();

    // Extract created inode_id from response (TODO: when Create returns inode)
    // For now, we'll use Lookup to get it
    let lookup_req = LookupRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        name: "a".to_string(),
    };

    let lookup_resp = MetadataFsServiceProto::lookup(&harness.fs_service, Request::new(lookup_req.clone()))
        .await
        .unwrap()
        .into_inner();

    let inode_id_a = FsTestHarness::extract_lookup_inode_id(&lookup_resp).unwrap();

    // Act: Rename "a" -> "b"
    let rename_req = FsRenameRequestProto {
        header: req_header.clone(),
        src_parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        src_name: "a".to_string(),
        dst_parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        dst_name: "b".to_string(),
        flags: 0,
    };

    let rename_resp = MetadataFsServiceProto::rename(&harness.fs_service, Request::new(rename_req))
        .await
        .unwrap()
        .into_inner();

    // Assert: No error
    assert_eq!(FsTestHarness::extract_error_code(&rename_resp.header), None);

    // Assert: Lookup(root, "a") returns ENOENT
    let lookup_a_req = LookupRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        name: "a".to_string(),
    };

    let lookup_a_resp = MetadataFsServiceProto::lookup(&harness.fs_service, Request::new(lookup_a_req)).await;

    assert!(lookup_a_resp.is_err());
    let status = lookup_a_resp.unwrap_err();
    assert!(status.message().contains("not found") || status.message().contains("ENOENT"));

    // Assert: Lookup(root, "b") returns inode_id_a
    let lookup_b_req = LookupRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        name: "b".to_string(),
    };

    let lookup_b_resp = MetadataFsServiceProto::lookup(&harness.fs_service, Request::new(lookup_b_req))
        .await
        .unwrap()
        .into_inner();

    let inode_id_b = FsTestHarness::extract_lookup_inode_id(&lookup_b_resp).unwrap();
    assert_eq!(inode_id_a, inode_id_b);

    // Assert: ReadDir(root) contains only "b" (not "a")
    let readdir_req = ReadDirRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        cursor_key: vec![],
        max_entries: 100,
    };

    let readdir_resp = MetadataFsServiceProto::read_dir(&harness.fs_service, Request::new(readdir_req))
        .await
        .unwrap()
        .into_inner();

    let entry_names: HashSet<String> = readdir_resp.entries.iter().map(|e| e.name.clone()).collect();

    assert!(!entry_names.contains("a"));
    assert!(entry_names.contains("b"));

    // Verify "b" points to inode_id_a
    let b_entry = readdir_resp.entries.iter().find(|e| e.name == "b").unwrap();
    assert_eq!(b_entry.inode_id.as_ref().unwrap().value, inode_id_a.as_raw());
}

/// B2: Test rename across mounts returns EXDEV.
#[tokio::test]
async fn test_rename_cross_mount_exdev() {
    let harness = FsTestHarness::new().await.unwrap();

    // Arrange: Create two mounts
    let (mount1_id, mount1_root) = harness
        .create_mount_with_root(
            "/mnt/mount1".to_string(),
            "file:///tmp/mount1".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let (mount2_id, mount2_root) = harness
        .create_mount_with_root(
            "/mnt/mount2".to_string(),
            "file:///tmp/mount2".to_string(),
            ShardGroupId::new(2),
        )
        .await
        .unwrap();

    // Create file "a" in mount1
    let req_header = FsTestHarness::create_test_request_header();
    let mut attrs = FileAttrs::new();
    attrs.mode = 0o644;
    let layout = FileLayout::new(1024, 512, 3);

    let create_req = CreateRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: mount1_root.as_raw(),
        }),
        name: "a".to_string(),
        attrs: Some(proto::fs::FileAttrsProto {
            mode: attrs.mode,
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            atime_ms: attrs.atime_ms,
            mtime_ms: attrs.mtime_ms,
            ctime_ms: attrs.ctime_ms,
            nlink: attrs.nlink,
        }),
        layout: Some(proto::common::FileLayoutProto {
            block_size: layout.block_size,
            chunk_size: layout.chunk_size,
            replication: layout.replication as u32,
        }),
    };

    MetadataFsServiceProto::create(&harness.fs_service, Request::new(create_req))
        .await
        .unwrap();

    // Act: Rename from mount1 to mount2
    let rename_req = FsRenameRequestProto {
        header: req_header.clone(),
        src_parent_inode_id: Some(proto::fs::InodeIdProto {
            value: mount1_root.as_raw(),
        }),
        src_name: "a".to_string(),
        dst_parent_inode_id: Some(proto::fs::InodeIdProto {
            value: mount2_root.as_raw(),
        }),
        dst_name: "a".to_string(),
        flags: 0,
    };

    let rename_resp = MetadataFsServiceProto::rename(&harness.fs_service, Request::new(rename_req))
        .await
        .unwrap()
        .into_inner();

    // Assert: Returns EXDEV (error_code = 18)
    let error_code = FsTestHarness::extract_error_code(&rename_resp.header);
    assert_eq!(error_code, Some(18)); // FS_ERR_EXDEV = 18

    // Assert: mount1 still has "a"
    let lookup_a_req = LookupRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: mount1_root.as_raw(),
        }),
        name: "a".to_string(),
    };

    let lookup_a_resp = harness
        .fs_service
        .lookup(Request::new(lookup_a_req))
        .await
        .unwrap()
        .into_inner();

    assert!(lookup_a_resp.inode.is_some());

    // Assert: mount2 does not have "a"
    let lookup_a_mount2_req = LookupRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: mount2_root.as_raw(),
        }),
        name: "a".to_string(),
    };

    let lookup_a_mount2_resp = harness
        .fs_service
        .lookup(Request::new(lookup_a_mount2_req))
        .await
        .unwrap()
        .into_inner();
    assert!(lookup_a_mount2_resp.inode.is_none());
}

/// B3: Test ReadDir pagination with cursor_key.
#[tokio::test]
#[ignore = "pending inode/data_handle alignment in fs service tests"]
async fn test_mkdir_create_readdir_pagination() {
    let harness = FsTestHarness::new().await.unwrap();

    // Arrange: Create mount and root
    let (_mount_id, root_inode_id) = harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    // Create N entries (N > max_entries, e.g., 50)
    let n = 50;
    let req_header = FsTestHarness::create_test_request_header();
    let mut attrs = FileAttrs::new();
    attrs.mode = 0o644;
    let layout = FileLayout::new(1024, 512, 3);

    let mut created_names = HashSet::new();
    for i in 0..n {
        let name = format!("file_{:03}", i);
        created_names.insert(name.clone());

        let create_req = CreateRequestProto {
            header: req_header.clone(),
            parent_inode_id: Some(proto::fs::InodeIdProto {
                value: root_inode_id.as_raw(),
            }),
            name: name.clone(),
            attrs: Some(proto::fs::FileAttrsProto {
                mode: attrs.mode,
                uid: attrs.uid,
                gid: attrs.gid,
                size: attrs.size,
                atime_ms: attrs.atime_ms,
                mtime_ms: attrs.mtime_ms,
                ctime_ms: attrs.ctime_ms,
                nlink: attrs.nlink,
            }),
            layout: Some(proto::common::FileLayoutProto {
                block_size: layout.block_size,
                chunk_size: layout.chunk_size,
                replication: layout.replication as u32,
            }),
        };

        harness.fs_service.create(Request::new(create_req)).await.unwrap();
    }

    // Act: ReadDir with pagination (max_entries=10)
    let max_entries = 10;
    let mut all_entries = Vec::new();
    let mut cursor_key = vec![];
    let mut eof = false;

    while !eof {
        let readdir_req = ReadDirRequestProto {
            header: req_header.clone(),
            parent_inode_id: Some(proto::fs::InodeIdProto {
                value: root_inode_id.as_raw(),
            }),
            cursor_key: cursor_key.clone(),
            max_entries,
        };

        let readdir_resp = harness
            .fs_service
            .read_dir(Request::new(readdir_req))
            .await
            .unwrap()
            .into_inner();

        all_entries.extend(readdir_resp.entries.iter().map(|e| e.name.clone()));
        cursor_key = readdir_resp.next_cursor_key;
        eof = readdir_resp.eof;
    }

    // Assert: All entries collected (no duplicates, no missing)
    let collected_names: HashSet<String> = all_entries.iter().cloned().collect();
    assert_eq!(collected_names.len(), n);
    assert_eq!(collected_names, created_names);

    // Assert: No duplicates in all_entries
    assert_eq!(all_entries.len(), n);
}

/// B4: Test rmdir fails on non-empty directory.
#[tokio::test]
#[ignore = "pending inode/data_handle alignment in fs service tests"]
async fn test_rmdir_nonempty_fails() {
    let harness = FsTestHarness::new().await.unwrap();

    // Arrange: Create mount and root
    let (_mount_id, root_inode_id) = harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let req_header = FsTestHarness::create_test_request_header();

    // Create directory "dir"
    let mut dir_attrs = FileAttrs::new();
    dir_attrs.mode = 0o755;

    let mkdir_req = MkdirRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        name: "dir".to_string(),
        attrs: Some(proto::fs::FileAttrsProto {
            mode: dir_attrs.mode,
            uid: dir_attrs.uid,
            gid: dir_attrs.gid,
            size: dir_attrs.size,
            atime_ms: dir_attrs.atime_ms,
            mtime_ms: dir_attrs.mtime_ms,
            ctime_ms: dir_attrs.ctime_ms,
            nlink: dir_attrs.nlink,
        }),
    };

    let mkdir_resp = MetadataFsServiceProto::mkdir(&harness.fs_service, Request::new(mkdir_req))
        .await
        .unwrap()
        .into_inner();

    // Get dir_inode_id from response (TODO: when Mkdir returns inode)
    // For now, use Lookup
    let lookup_dir_req = LookupRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        name: "dir".to_string(),
    };

    let lookup_dir_resp = harness
        .fs_service
        .lookup(Request::new(lookup_dir_req))
        .await
        .unwrap()
        .into_inner();

    let dir_inode_id = FsTestHarness::extract_lookup_inode_id(&lookup_dir_resp).unwrap();

    // Create file "x" in "dir"
    let mut file_attrs = FileAttrs::new();
    file_attrs.mode = 0o644;
    let layout = FileLayout::new(1024, 512, 3);

    let create_req = CreateRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: dir_inode_id.as_raw(),
        }),
        name: "x".to_string(),
        attrs: Some(proto::fs::FileAttrsProto {
            mode: file_attrs.mode,
            uid: file_attrs.uid,
            gid: file_attrs.gid,
            size: file_attrs.size,
            atime_ms: file_attrs.atime_ms,
            mtime_ms: file_attrs.mtime_ms,
            ctime_ms: file_attrs.ctime_ms,
            nlink: file_attrs.nlink,
        }),
        layout: Some(proto::common::FileLayoutProto {
            block_size: layout.block_size,
            chunk_size: layout.chunk_size,
            replication: layout.replication as u32,
        }),
    };

    MetadataFsServiceProto::create(&harness.fs_service, Request::new(create_req))
        .await
        .unwrap();

    // Act: Try to rmdir "dir"
    let rmdir_req = RmdirRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        name: "dir".to_string(),
    };

    let rmdir_resp = MetadataFsServiceProto::rmdir(&harness.fs_service, Request::new(rmdir_req))
        .await
        .unwrap()
        .into_inner();

    // Assert: Returns ENOTEMPTY (error_code = 39)
    let error_code = FsTestHarness::extract_error_code(&rmdir_resp.header);
    assert_eq!(error_code, Some(39)); // FS_ERR_ENOTEMPTY = 39

    // Assert: Lookup(root, "dir") still succeeds
    let lookup_dir_again_req = LookupRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: root_inode_id.as_raw(),
        }),
        name: "dir".to_string(),
    };

    let lookup_dir_again_resp = harness
        .fs_service
        .lookup(Request::new(lookup_dir_again_req))
        .await
        .unwrap()
        .into_inner();

    assert!(lookup_dir_again_resp.inode.is_some());

    // Assert: Lookup(dir_inode, "x") still succeeds
    let lookup_x_req = LookupRequestProto {
        header: req_header.clone(),
        parent_inode_id: Some(proto::fs::InodeIdProto {
            value: dir_inode_id.as_raw(),
        }),
        name: "x".to_string(),
    };

    let lookup_x_resp = harness
        .fs_service
        .lookup(Request::new(lookup_x_req))
        .await
        .unwrap()
        .into_inner();

    assert!(lookup_x_resp.inode.is_some());
}
