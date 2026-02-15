// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for GetFileLayout slice-keyed block locations.

mod common;
use common::FsTestHarness;
use proto::metadata::metadata_inode_service_proto_server::MetadataInodeServiceProto;
use proto::metadata::GetFileLayoutRequestProto;
use std::collections::BTreeMap;
use tonic::Request;
use types::fs::{Extent, FileAttrs, Inode, InodeData, InodeId, InodeKind};
use types::ids::{BlockId, BlockIndex, DataHandleId};

#[tokio::test]
async fn get_file_layout_returns_slice_keyed_locations() {
    let harness = FsTestHarness::new().await.unwrap();

    // Mount + root
    let (mount_id, _root_inode_id) = harness
        .create_mount_with_root(
            "/mnt/layout".to_string(),
            "file:///tmp/layout".to_string(),
            types::ids::ShardGroupId::new(1),
        )
        .await
        .unwrap();

    // Seed a file inode with two extents.
    let file_inode_id = InodeId::new(42);
    let data_handle_id = DataHandleId::new(7);
    let extent1 = Extent {
        file_offset: 0,
        block_id: BlockId::new(data_handle_id, BlockIndex::new(0)),
        block_offset: 0,
        len: 4096,
        file_version: Some(1),
        block_stamp: Some(0),
    };
    let extent2 = Extent {
        file_offset: 4096,
        block_id: BlockId::new(data_handle_id, BlockIndex::new(1)),
        block_offset: 0,
        len: 2048,
        file_version: Some(1),
        block_stamp: Some(0),
    };
    let mut attrs = FileAttrs::new();
    attrs.size = extent1.len + extent2.len;
    let inode = Inode {
        inode_id: file_inode_id,
        kind: InodeKind::File,
        attrs,
        data: InodeData::File {
            extents: vec![extent1.clone(), extent2.clone()],
            lease_epoch: None,
        },
        mount_id,
        current_data_handle_id: data_handle_id,
        xattrs: BTreeMap::new(),
    };
    harness.storage.put_inode(&inode).unwrap();

    // Call GetFileLayout.
    let resp = MetadataInodeServiceProto::get_file_layout(
        &harness.inode_service,
        Request::new(GetFileLayoutRequestProto {
            header: FsTestHarness::create_test_request_header(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: file_inode_id.as_raw(),
            }),
            range: None,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(resp.extents.len(), 2);
    assert_eq!(resp.locations.len(), 2);

    for extent in resp.extents {
        let block_id = extent.block_id.expect("extent missing block_id");
        let loc = resp
            .locations
            .iter()
            .find(|loc| {
                loc.block_id
                    .as_ref()
                    .map(|b| b.data_handle_id == block_id.data_handle_id && b.block_index == block_id.block_index)
                    .unwrap_or(false)
            })
            .expect("matching location not found");
        assert_eq!(loc.file_offset, extent.file_offset);
        assert_eq!(loc.len, extent.len);
    }
}
