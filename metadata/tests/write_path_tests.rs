// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for the write path (OpenWrite/CloseWrite).

use metadata::write_session::{CreateSessionInput, WriteSessionManager};
use types::fs::{Extent, InodeId};
use types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, LeaseId, MountId};
use types::lease::FencingToken;
use types::CallId;

#[test]
fn test_extent_structure() {
    // Test that Extent structure works correctly
    let extent = Extent {
        file_offset: 0,
        block_id: BlockId::new(DataHandleId::new(1), BlockIndex::new(0)),
        block_offset: 0,
        len: 4096,
        file_version: None,
        block_stamp: None,
    };

    assert_eq!(extent.file_offset, 0);
    assert_eq!(extent.len, 4096);
}

#[test]
fn test_write_session_manager() {
    let manager = WriteSessionManager::default();

    // Test session creation
    let inode_id = InodeId::new(1);
    let mount_id = MountId::new(1);
    let lease_id = LeaseId::new(123);
    let fencing_token = FencingToken {
        block_id: BlockId::new(DataHandleId::new(1), BlockIndex::new(0)),
        owner: ClientId::new(1),
        epoch: 1,
    };
    let writer_identity = metadata::write_session::WriterIdentity {
        client_id: ClientId::new(1),
        call_id: CallId::new(),
    };

    let handle = manager.create_session(CreateSessionInput {
        inode_id,
        mount_id,
        data_handle_id: DataHandleId::new(1),
        lease_id,
        lease_epoch: 1,
        fencing_token,
        open_epoch: 1,
        base_size: 0,
        mode: metadata::inode_lease::WriteMode::Write,
        write_targets: Vec::new(),
        writer_identity,
    });

    // Verify session exists
    assert!(manager.get_session(handle).is_some());

    // Verify conflict detection
    assert!(manager.has_active_session(inode_id));

    // Cleanup
    manager.remove_session(handle);
    assert!(!manager.has_active_session(inode_id));
}
