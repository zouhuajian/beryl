// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Path service integration tests.

mod common;
use common::FsTestHarness;
use metadata::error::MetadataError;
use metadata::service::{MetadataFileSystemServiceImpl, MetadataInodeServiceImpl};
use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use proto::metadata::*;
use std::sync::Arc;
use tonic::Request;
use types::fs::{FileAttrs, InodeId, InodeKind};
use types::ids::ShardGroupId;
use types::layout::FileLayout;

/// Test harness for path service tests.
pub struct PathTestHarness {
    pub fs_harness: FsTestHarness,
    pub path_service: MetadataFileSystemServiceImpl,
}

impl PathTestHarness {
    /// Create a new test harness with mount and root inode.
    pub async fn new() -> Result<Self, MetadataError> {
        let fs_harness = FsTestHarness::new().await?;

        // Create mount and root inode
        let (_mount_id, _root_inode_id) = fs_harness
            .create_mount_with_root(
                "/mnt/test".to_string(),
                "file:///tmp/test".to_string(),
                ShardGroupId::new(1),
            )
            .await?;

        // Create path service
        use metadata::metrics::MetadataMetrics;
        let metrics = Arc::new(MetadataMetrics::new());
        let inode_service = MetadataInodeServiceImpl::new(
            fs_harness.state_store.clone() as Arc<dyn metadata::state::StateStore>,
            fs_harness.mount_table.clone(),
        )
        .with_storage(fs_harness.storage.clone())
        .with_raft_node(fs_harness.raft_node.clone())
        .with_metrics(metrics.clone());
        let fs_core = inode_service.fs_core();

        let path_service =
            MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
                .with_metrics(metrics);

        Ok(Self {
            fs_harness,
            path_service,
        })
    }
}

/// Test create + getattr + liststatus.
#[tokio::test]
#[ignore = "pending inode/data_handle alignment in path service tests"]
async fn test_create_getattr_liststatus() {
    let harness = PathTestHarness::new().await.unwrap();

    // Create file
    let req_header = FsTestHarness::create_test_request_header();
    let attrs = FileAttrs::new();
    let layout = FileLayout::new(1024, 512, 3);

    let create_req = CreatePathRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/file.txt".to_string(),
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

    let create_resp = FileSystemServiceProto::create(&harness.path_service, Request::new(create_req))
        .await
        .unwrap()
        .into_inner();

    assert!(create_resp.header.is_some());
    assert!(create_resp.inode_id.is_some());
    let inode_id = create_resp.inode_id.unwrap().value;

    // GetAttr
    let getattr_req = GetFileStatusRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/file.txt".to_string(),
    };

    let getattr_resp = FileSystemServiceProto::get_file_status(&harness.path_service, Request::new(getattr_req))
        .await
        .unwrap()
        .into_inner();

    assert!(getattr_resp.header.is_some());
    assert!(getattr_resp.attrs.is_some());
    assert_eq!(getattr_resp.inode_id.as_ref().unwrap().value, inode_id);

    // ListStatus
    let list_req = ListStatusPathRequestProto {
        header: req_header.clone(),
        path: "/mnt/test".to_string(),
        recursive: false,
        cursor: vec![],
        limit: 100,
    };

    let list_resp = FileSystemServiceProto::list_status(&harness.path_service, Request::new(list_req))
        .await
        .unwrap()
        .into_inner();

    assert!(list_resp.header.is_some());
    assert!(list_resp.entries.len() >= 1);
    assert!(list_resp.entries.iter().any(|e| e.name == "file.txt"));
}

/// Test mkdir + nested create + liststatus pagination.
#[tokio::test]
#[ignore = "pending inode/data_handle alignment in path service tests"]
async fn test_mkdir_nested_create_liststatus() {
    let harness = PathTestHarness::new().await.unwrap();

    let req_header = FsTestHarness::create_test_request_header();
    let attrs = FileAttrs::new();

    // Mkdir
    let mkdir_req = MkdirPathRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/dir1".to_string(),
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
        create_parents: false,
    };

    let mkdir_resp = FileSystemServiceProto::mkdir(&harness.path_service, Request::new(mkdir_req))
        .await
        .unwrap()
        .into_inner();

    assert!(mkdir_resp.header.is_some());
    assert!(mkdir_resp.inode.is_some());

    // Create nested file
    let layout = FileLayout::new(1024, 512, 3);
    let create_req = CreatePathRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/dir1/nested.txt".to_string(),
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

    let create_resp = FileSystemServiceProto::create(&harness.path_service, Request::new(create_req))
        .await
        .unwrap()
        .into_inner();

    assert!(create_resp.header.is_some());

    // ListStatus with pagination
    let list_req = ListStatusPathRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/dir1".to_string(),
        recursive: false,
        cursor: vec![],
        limit: 10,
    };

    let list_resp = FileSystemServiceProto::list_status(&harness.path_service, Request::new(list_req))
        .await
        .unwrap()
        .into_inner();

    assert!(list_resp.header.is_some());
    assert!(list_resp.entries.len() >= 1);
    assert!(list_resp.entries.iter().any(|e| e.name == "nested.txt"));
}

/// Test rename same mount (verify a disappears b appears and inode unchanged).
#[tokio::test]
#[ignore = "pending inode/data_handle alignment in path service tests"]
async fn test_rename_same_mount() {
    let harness = PathTestHarness::new().await.unwrap();

    let req_header = FsTestHarness::create_test_request_header();
    let attrs = FileAttrs::new();
    let layout = FileLayout::new(1024, 512, 3);

    // Create file "a"
    let create_req = CreatePathRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/a".to_string(),
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

    let create_resp = FileSystemServiceProto::create(&harness.path_service, Request::new(create_req))
        .await
        .unwrap()
        .into_inner();

    let inode_id_a = create_resp.inode_id.unwrap().value;

    // Rename a -> b
    let rename_req = RenamePathRequestProto {
        header: req_header.clone(),
        src_path: "/mnt/test/a".to_string(),
        dst_path: "/mnt/test/b".to_string(),
        flags: 0,
    };

    let rename_resp = FileSystemServiceProto::rename(&harness.path_service, Request::new(rename_req))
        .await
        .unwrap()
        .into_inner();

    assert!(rename_resp.header.is_some());

    // Verify "a" no longer exists
    let lookup_a_req = GetFileStatusRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/a".to_string(),
    };

    let lookup_a_resp = FileSystemServiceProto::get_file_status(&harness.path_service, Request::new(lookup_a_req))
        .await
        .unwrap()
        .into_inner();
    assert!(lookup_a_resp.header.as_ref().and_then(|h| h.error.as_ref()).is_some());

    // Verify "b" exists with same inode_id
    let lookup_b_req = GetFileStatusRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/b".to_string(),
    };

    let lookup_b_resp = FileSystemServiceProto::get_file_status(&harness.path_service, Request::new(lookup_b_req))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(lookup_b_resp.inode_id.as_ref().unwrap().value, inode_id_a);
}

/// Test rename cross mount -> EXDEV.
#[tokio::test]
async fn test_rename_cross_mount_exdev() {
    let harness = PathTestHarness::new().await.unwrap();

    // Create second mount
    let (_mount_id2, _root_inode_id2) = harness
        .fs_harness
        .create_mount_with_root(
            "/mnt/test2".to_string(),
            "file:///tmp/test2".to_string(),
            ShardGroupId::new(2),
        )
        .await
        .unwrap();

    let req_header = FsTestHarness::create_test_request_header();
    let attrs = FileAttrs::new();
    let layout = FileLayout::new(1024, 512, 3);

    // Create file in first mount
    let create_req = CreatePathRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/file.txt".to_string(),
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

    FileSystemServiceProto::create(&harness.path_service, Request::new(create_req))
        .await
        .unwrap();

    // Try to rename across mounts
    let rename_req = RenamePathRequestProto {
        header: req_header.clone(),
        src_path: "/mnt/test/file.txt".to_string(),
        dst_path: "/mnt/test2/file.txt".to_string(),
        flags: 0,
    };

    let rename_resp = FileSystemServiceProto::rename(&harness.path_service, Request::new(rename_req))
        .await
        .unwrap()
        .into_inner();

    // Should return EXDEV error
    assert!(rename_resp.header.is_some());
    assert!(rename_resp.header.as_ref().unwrap().error.is_some());
    let error = rename_resp.header.as_ref().unwrap().error.as_ref().unwrap();
    assert_eq!(
        error.code,
        Some(proto::common::error_detail_proto::Code::FsErrno(
            proto::common::FsErrnoProto::FsErrnoExdev as i32
        ))
    );
}

/// Test delete/unlink and rmdir non-empty ENOTEMPTY.
#[tokio::test]
#[ignore = "pending inode/data_handle alignment in path service tests"]
async fn test_delete_unlink_rmdir_notempty() {
    let harness = PathTestHarness::new().await.unwrap();

    let req_header = FsTestHarness::create_test_request_header();
    let attrs = FileAttrs::new();
    let layout = FileLayout::new(1024, 512, 3);

    // Create directory
    let mkdir_req = MkdirPathRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/dir".to_string(),
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
        create_parents: false,
    };

    FileSystemServiceProto::mkdir(&harness.path_service, Request::new(mkdir_req))
        .await
        .unwrap();

    // Create file in directory
    let create_req = CreatePathRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/dir/file.txt".to_string(),
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

    FileSystemServiceProto::create(&harness.path_service, Request::new(create_req))
        .await
        .unwrap();

    // Try to rmdir non-empty directory (should fail with ENOTEMPTY)
    let rmdir_req = RmdirPathRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/dir".to_string(),
    };

    let rmdir_resp = FileSystemServiceProto::rmdir(&harness.path_service, Request::new(rmdir_req))
        .await
        .unwrap()
        .into_inner();

    // Should return ENOTEMPTY error
    assert!(rmdir_resp.header.is_some());
    assert!(rmdir_resp.header.as_ref().unwrap().error.is_some());
    let error = rmdir_resp.header.as_ref().unwrap().error.as_ref().unwrap();
    assert_eq!(
        error.code,
        Some(proto::common::error_detail_proto::Code::FsErrno(
            proto::common::FsErrnoProto::FsErrnoEnotempty as i32
        ))
    );

    // Unlink file (should succeed)
    let unlink_req = UnlinkPathRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/dir/file.txt".to_string(),
    };

    let unlink_resp = FileSystemServiceProto::unlink(&harness.path_service, Request::new(unlink_req))
        .await
        .unwrap()
        .into_inner();

    assert!(unlink_resp.header.is_some());

    // Now rmdir should succeed
    let rmdir_req2 = RmdirPathRequestProto {
        header: req_header.clone(),
        path: "/mnt/test/dir".to_string(),
    };

    let rmdir_resp2 = FileSystemServiceProto::rmdir(&harness.path_service, Request::new(rmdir_req2))
        .await
        .unwrap()
        .into_inner();

    assert!(rmdir_resp2.header.is_some());
    assert!(rmdir_resp2.header.as_ref().unwrap().error.is_none());
}
