// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Filesystem checkpoint and crash-image coverage through the local-store boundary.
use beryl_proto::worker::{BlockMetaPayloadProto, BlockStateProto};
use beryl_types::{BlockId, BlockIndex, ClientId, FencingToken, GroupName, InodeId, LeaseEpoch, Tier};
use beryl_worker::store::block::{
    CheckpointBlockRequest, FullBlockFileStore, LocalBlockStore, OpenBlockWriteRequest, ReclaimBlockRequest,
    ReclaimBlockResult,
};
use bytes::Bytes;
use prost::Message;
use std::fs::{self, OpenOptions};
use tempfile::TempDir;

fn fixture() -> (TempDir, FullBlockFileStore, OpenBlockWriteRequest) {
    let dir = tempfile::tempdir().unwrap();
    let store = FullBlockFileStore::new(dir.path().into());
    let block_id = BlockId::new(InodeId::new(7), BlockIndex::new(2));
    let request = OpenBlockWriteRequest {
        group_name: GroupName::parse("root").unwrap(),
        block_id,
        block_size: 16,
        tier: Tier::Ssd,
        fencing_token: FencingToken::new(ClientId::generate(), LeaseEpoch::new(1)),
        write_offset: 0,
        visible_len: 0,
    };
    (dir, store, request)
}

fn checkpoint(store: &FullBlockFileStore, req: &OpenBlockWriteRequest, data: &'static [u8]) {
    store.open_block_write(req.clone()).unwrap();
    store
        .write_at(
            &req.group_name,
            req.block_id,
            req.write_offset,
            Bytes::from_static(data),
        )
        .unwrap();
    let meta = store
        .checkpoint_block(CheckpointBlockRequest {
            group_name: req.group_name.clone(),
            block_id: req.block_id,
            effective_len: req.write_offset + data.len() as u64,
            fencing_token: req.fencing_token,
        })
        .unwrap();
    assert_eq!(meta.durable_len, req.write_offset + data.len() as u64);
}

#[test]
fn short_streams_resume_and_new_writer_discards_only_unpublished_suffix() {
    let (_dir, store, mut req) = fixture();
    checkpoint(&store, &req, b"abcd");
    req.write_offset = 4;
    req.visible_len = 4;
    checkpoint(&store, &req, b"lost");
    assert!(
        store.open_block_write(req.clone()).is_err(),
        "same epoch cannot rewind D"
    );
    let old = req.clone();
    req.fencing_token.epoch = LeaseEpoch::new(2);
    let meta = store.open_block_write(req.clone()).unwrap();
    assert_eq!(meta.durable_len, 4);
    assert_eq!(
        fs::metadata(store.paths(&req.group_name, req.block_id).data_path)
            .unwrap()
            .len(),
        4
    );
    assert!(store.open_block_write(old.clone()).is_err());
    assert!(store
        .checkpoint_block(CheckpointBlockRequest {
            group_name: req.group_name.clone(),
            block_id: req.block_id,
            effective_len: 4,
            fencing_token: old.fencing_token,
        })
        .is_err());
    assert!(store
        .write_at(&req.group_name, req.block_id, 0, Bytes::from_static(b"bad"))
        .is_err());
    checkpoint(&store, &req, b"efgh");
    assert_eq!(
        store.read_at(&req.group_name, req.block_id, 0, 8).unwrap(),
        b"abcdefgh"[..]
    );
    assert!(store.read_at(&req.group_name, req.block_id, 0, 9).is_err());
}

/// Installs a complete metadata image at a named crash boundary, without production fault hooks.
fn persist_meta_image(
    store: &FullBlockFileStore,
    req: &OpenBlockWriteRequest,
    edit: impl FnOnce(&mut BlockMetaPayloadProto),
) {
    let path = store.paths(&req.group_name, req.block_id).meta_path;
    let old = fs::read(&path).unwrap();
    let mut meta = BlockMetaPayloadProto::decode(&old[20..]).unwrap();
    edit(&mut meta);
    let payload = meta.encode_to_vec();
    let mut image = old[..20].to_vec();
    image[12..20].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    image.extend_from_slice(&payload);
    fs::write(path, image).unwrap();
}

#[test]
fn invalid_write_authority_is_rejected_before_io_and_during_recovery() {
    let (_dir, store, mut req) = fixture();
    req.fencing_token.owner = ClientId::new(0);
    assert!(store.open_block_write(req.clone()).is_err());
    assert!(!store.paths(&req.group_name, req.block_id).meta_path.exists());

    let corruptions: [fn(&mut BlockMetaPayloadProto); 5] = [
        |meta| meta.block_state = BlockStateProto::BlockStateUnspecified as i32,
        |meta| meta.fencing_token = None,
        |meta| meta.fencing_token.as_mut().unwrap().owner = Some(Default::default()),
        |meta| meta.block_id.as_mut().unwrap().block_index += 1,
        |meta| meta.group_name = "other".to_string(),
    ];
    for corrupt in corruptions {
        let (_dir, store, req) = fixture();
        checkpoint(&store, &req, b"abcd");
        persist_meta_image(&store, &req, corrupt);
        let paths = store.paths(&req.group_name, req.block_id);
        let before = fs::read(&paths.meta_path).unwrap();
        assert!(store.load_meta(&req.group_name, req.block_id).is_err());
        assert!(store.recover_blocks().is_err());
        assert_eq!(fs::read(&paths.meta_path).unwrap(), before);
        assert_eq!(fs::read(&paths.data_path).unwrap(), b"abcd");
    }
}

#[test]
fn recovery_uses_checkpoint_after_unsynced_io_and_interrupted_takeover() {
    for takeover in [false, true] {
        let (_dir, store, mut req) = fixture();
        checkpoint(&store, &req, b"abcd");
        req.write_offset = 4;
        checkpoint(&store, &req, b"suffix");
        if takeover {
            // Crash after E2/D4 metadata replacement and before truncation of P10.
            persist_meta_image(&store, &req, |meta| {
                meta.durable_len = 4;
                meta.fencing_token.as_mut().unwrap().epoch = 2;
            });
        } else {
            store
                .write_at(&req.group_name, req.block_id, 10, Bytes::from_static(b"extra"))
                .unwrap();
        }
        let expected = if takeover { 4 } else { 10 };
        assert_eq!(store.recover_blocks().unwrap(), (expected, 1));
        assert_eq!(
            fs::metadata(store.paths(&req.group_name, req.block_id).data_path)
                .unwrap()
                .len(),
            expected
        );
        assert_eq!(store.read_at(&req.group_name, req.block_id, 0, 4).unwrap(), b"abcd"[..]);
    }
}

#[test]
fn recovery_rejects_short_prefix_and_unknown_versions_without_changing_data() {
    for version in [None, Some(3u32), Some(5u32)] {
        let (_dir, store, req) = fixture();
        checkpoint(&store, &req, b"abcd");
        let paths = store.paths(&req.group_name, req.block_id);
        if let Some(version) = version {
            let mut bytes = fs::read(&paths.meta_path).unwrap();
            bytes[4..8].copy_from_slice(&version.to_le_bytes());
            fs::write(&paths.meta_path, bytes).unwrap();
        } else {
            OpenOptions::new()
                .write(true)
                .open(&paths.data_path)
                .unwrap()
                .set_len(3)
                .unwrap();
        }
        let data_before = fs::read(&paths.data_path).unwrap();
        let meta_before = fs::read(&paths.meta_path).unwrap();
        if version.is_some() {
            assert!(store.load_meta(&req.group_name, req.block_id).is_err());
        }
        assert!(store.recover_blocks().is_err());
        assert_eq!(fs::read(&paths.data_path).unwrap(), data_before);
        assert_eq!(fs::read(&paths.meta_path).unwrap(), meta_before);
    }
}

#[test]
fn deletion_recovers_each_unlink_boundary_and_is_idempotent() {
    for data_removed in [false, true] {
        let (_dir, store, req) = fixture();
        checkpoint(&store, &req, b"abcd");
        persist_meta_image(&store, &req, |meta| {
            meta.block_state = BlockStateProto::BlockStateDeleting as i32
        });
        let paths = store.paths(&req.group_name, req.block_id);
        if data_removed {
            fs::remove_file(&paths.data_path).unwrap();
        }
        assert!(store.read_at(&req.group_name, req.block_id, 0, 1).is_err());
        assert_eq!(store.recover_blocks().unwrap(), (0, 0));
        assert!(!paths.data_path.exists());
        assert!(!paths.meta_path.exists());
        assert_eq!(
            store
                .reclaim_block(&ReclaimBlockRequest {
                    group_name: req.group_name,
                    block_id: req.block_id
                })
                .unwrap(),
            ReclaimBlockResult::AlreadyAbsent
        );
    }
}
