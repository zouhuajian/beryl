// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_client::{ClientResult, FileType, ListStatusOptions, MkdirOptions};
use beryl_common::error::rpc::{ErrorKind, ProtocolErrorKind};
use beryl_common::header::RequestHeader;
use beryl_e2e::data::deterministic_bytes;
use beryl_e2e::TestCluster;
use beryl_proto::common::BlockIdProto;
use beryl_proto::convert::rpc_error_from_proto;
use beryl_proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use beryl_proto::metadata::{
    AllocateBlockRequestProto, AuthorizeBlockWriteRequestProto, CreateFileRequestProto, MsyncRequestProto,
};
use beryl_types::{ClientId, GroupName, GroupStateWatermark};
use bytes::Bytes;
use futures::io::AsyncReadExt;
use std::fmt::Debug;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_client_crud_roundtrip() {
    let mut cluster = TestCluster::start(std::path::Path::new(env!("CARGO_BIN_EXE_metadata-e2e-server")))
        .await
        .expect("start hermetic local cluster");
    let client = cluster.client();
    let dir = "/e2e";
    let path = "/e2e/file";
    let renamed_path = "/e2e/file.renamed";

    let created_dir = client.mkdirs(dir).await.expect("mkdirs through metadata");
    assert_eq!(created_dir.path(), Some(dir));
    assert_eq!(created_dir.kind(), FileType::Dir);

    let first = Bytes::from(deterministic_bytes(1_337));
    let suffix = Bytes::from_static(b"-beryl-append-suffix");
    let expected = [first.as_ref(), suffix.as_ref()].concat();
    let mut writer = client.create(path).await.expect("create through metadata");
    writer.write_all(&first).await.expect("write through worker");
    writer.close().await.expect("close through metadata");

    let status = client.get_status(path).await.expect("status after close");
    assert_eq!(status.path(), Some(path));
    assert_eq!(status.kind(), FileType::File);
    assert_eq!(status.len(), first.len() as u64);

    let read = client
        .open(path)
        .await
        .expect("open after close")
        .read_range(..)
        .await
        .expect("read first bytes");
    assert_eq!(read, first);

    let mut appender = client.append(path).await.expect("append through metadata");
    appender.write_all(&suffix).await.expect("append write through worker");
    appender.close().await.expect("append close through metadata");

    let read = client
        .open(path)
        .await
        .expect("open after append")
        .read_range(..)
        .await
        .expect("read appended bytes");
    assert_eq!(read.as_ref(), expected.as_slice());

    let subdir = "/e2e/subdir";
    client
        .mkdirs_with_options(subdir, MkdirOptions { create_parent: false })
        .await
        .expect("create second listing entry");

    let mut statuses = client
        .list_status_with_options(dir, ListStatusOptions { page_size: Some(1) })
        .await
        .expect("start bounded directory listing");
    assert_eq!(statuses.path(), dir);
    let mut listed = Vec::new();
    while let Some(status) = statuses.next().await.expect("fetch next directory status") {
        listed.push(status);
    }
    listed.sort_by(|left, right| left.path().cmp(&right.path()));
    assert_eq!(
        listed.iter().map(|status| status.path()).collect::<Vec<_>>(),
        [Some(path), Some(subdir)]
    );
    assert_eq!(listed[0].kind(), FileType::File);
    assert_eq!(listed[0].len(), expected.len() as u64);
    assert_eq!(listed[1].kind(), FileType::Dir);

    client.delete(subdir).await.expect("delete empty listing subdirectory");

    let before_rename = client.get_status(path).await.unwrap();
    assert_eq!(before_rename.create_time(), status.create_time());
    assert!(before_rename.modify_time() >= status.modify_time());
    let reader_opened_before_rename = client.open(path).await.expect("open reader before rename");
    client
        .rename(path, renamed_path)
        .await
        .expect("rename through metadata");
    assert_not_found(client.get_status(path).await, "old path after rename");

    let renamed_status = client.get_status(renamed_path).await.expect("status after rename");
    assert_eq!(renamed_status.path(), Some(renamed_path));
    assert_eq!(reader_opened_before_rename.status().path(), Some(path));
    assert_eq!(reader_opened_before_rename.path(), path);
    assert_eq!(renamed_status.create_time(), before_rename.create_time());
    assert_eq!(renamed_status.modify_time(), before_rename.modify_time());
    assert_eq!(renamed_status.len(), expected.len() as u64);

    let renamed_read = client
        .open(renamed_path)
        .await
        .expect("open renamed file")
        .read_range(..)
        .await
        .expect("read renamed file");
    assert_eq!(renamed_read.as_ref(), expected.as_slice());
    let moved_reader_bytes = reader_opened_before_rename
        .read_range(..)
        .await
        .expect("reader opened before rename remains bound to the inode");
    assert_eq!(moved_reader_bytes.as_ref(), expected.as_slice());

    let uncached_reader = client.open(renamed_path).await.expect("open before delete");
    client
        .delete(renamed_path)
        .await
        .expect("namespace delete renamed file");
    assert_not_found(client.get_status(renamed_path).await, "deleted path status");
    assert_not_found(client.open(renamed_path).await, "deleted path open");
    assert_not_found(uncached_reader.read_range(0..1).await, "reader for deleted inode");

    let replacement = Bytes::from_static(b"replacement-file");
    let mut replacement_writer = client.create(renamed_path).await.expect("recreate deleted path");
    replacement_writer
        .write_all(&replacement)
        .await
        .expect("write replacement file");
    replacement_writer
        .close()
        .await
        .unwrap_or_else(|err| panic!("close replacement file: {err} ({err:?})"));
    assert_not_found(
        uncached_reader.read_range(0..1).await,
        "old reader must not bind to recreated path",
    );
    let replacement_read = client
        .open(renamed_path)
        .await
        .expect("open replacement file")
        .read_range(..)
        .await
        .expect("read replacement file");
    assert_eq!(replacement_read, replacement);
    client.delete(renamed_path).await.expect("delete replacement file");

    let mut statuses = client.list_status(dir).await.expect("list after delete");
    assert!(statuses.next().await.expect("read empty listing").is_none());

    cluster.shutdown().await.expect("local cluster shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn visibility_sync_then_continue_write_roundtrip() {
    let mut cluster = TestCluster::start(std::path::Path::new(env!("CARGO_BIN_EXE_metadata-e2e-server")))
        .await
        .expect("start hermetic local cluster");
    let client = cluster.client();
    let path = "/sync-continue";
    let first = Bytes::from(vec![b'a'; 317]);
    let second = Bytes::from(vec![b'b'; 1024]);

    let mut writer = client.create(path).await.expect("create through metadata");
    writer.write_all(&first).await.expect("write first block");
    writer
        .sync()
        .await
        .expect("publish first block while keeping session open");
    let visible_prefix = client
        .open(path)
        .await
        .expect("open immediately after visibility sync")
        .read_range(..)
        .await
        .expect("read published prefix while writer remains open");
    assert_eq!(visible_prefix, first);

    let boundary = 1024 - first.len();
    for chunk in [&second[..200], &second[200..boundary], &second[boundary..]] {
        writer.write_all(chunk).await.expect("write after visibility sync");
        writer.sync().await.expect("publish another tail checkpoint");
    }
    writer
        .sync()
        .await
        .expect("empty sync preserves the allocation predecessor");
    writer.close().await.expect("close after second block");

    let actual = client
        .open(path)
        .await
        .expect("open after close")
        .read_range(..)
        .await
        .expect("read both publication revisions");
    let expected = [first.as_ref(), second.as_ref()].concat();
    assert_eq!(actual.as_ref(), expected.as_slice());

    assert_eq!(cluster.physical_block_count().unwrap(), 2);
    cluster.shutdown().await.expect("local cluster shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_more_than_ten_blocks_roundtrip() {
    let mut cluster = TestCluster::start(std::path::Path::new(env!("CARGO_BIN_EXE_metadata-e2e-server")))
        .await
        .expect("start hermetic local cluster");
    let client = cluster.client();
    let path = "/many-blocks";
    let payload = Bytes::from(deterministic_bytes(12 * 1024 + 17));
    let mut writer = client.create(path).await.expect("create file");

    for offset in (0..payload.len()).step_by(127) {
        let end = (offset + 127).min(payload.len());
        writer
            .write_all(&payload[offset..end])
            .await
            .expect("write small frame across more than ten blocks");
    }
    writer.close().await.expect("close file");

    let mut reader = client.open(path).await.expect("open file");
    let mut actual = Vec::with_capacity(payload.len());
    let mut buffer = [0u8; 127];
    loop {
        let read = reader.read(&mut buffer).await.expect("read bounded step");
        if read == 0 {
            break;
        }
        actual.extend_from_slice(&buffer[..read]);
    }
    assert_eq!(actual.as_slice(), payload.as_ref());
    cluster.shutdown().await.expect("local cluster shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_headers_carry_write_authority_and_freshness() {
    let mut cluster = TestCluster::start(std::path::Path::new(env!("CARGO_BIN_EXE_metadata-e2e-server")))
        .await
        .expect("start cluster");
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint())
        .await
        .expect("connect Metadata");
    let header = RequestHeader::new(ClientId::new(701)).with_group_name(GroupName::parse("root").unwrap());
    let created = metadata
        .create_file(CreateFileRequestProto {
            header: Some((&header).into()),
            path: "/header-authority".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(created.header.unwrap().error.is_none());
    let allocated = metadata
        .allocate_block(AllocateBlockRequestProto {
            header: Some((&child_header(&header)).into()),
            write_handle: created.write_handle,
            previous_block_id: None,
        })
        .await
        .unwrap()
        .into_inner();
    let allocated_header = allocated.header.unwrap();
    assert!(allocated_header.error.is_none());
    let target = allocated.block.unwrap();
    let worker = &target.workers[0];
    let block_id = target.block_id.unwrap();

    let other = metadata
        .create_file(CreateFileRequestProto {
            header: Some((&child_header(&header)).into()),
            path: "/header-authority-other".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(other.header.unwrap().error.is_none());
    let other_handle = other.write_handle.unwrap();
    assert_eq!(other_handle.write_lease_epoch, target.fencing_token.unwrap().epoch);

    // Matching writer fields cannot authorize an unissued block in either inode.
    for (group, requested_block_id, allowed) in [
        (None, block_id, false),
        (Some("other"), block_id, false),
        (Some("root"), block_id, true),
        (
            Some("root"),
            BlockIdProto {
                block_index: block_id.block_index + 1,
                ..block_id
            },
            false,
        ),
        (
            Some("root"),
            BlockIdProto {
                inode_id: other_handle.inode_id,
                ..block_id
            },
            false,
        ),
    ] {
        let mut request_header = child_header(&header);
        request_header.group_name = group.map(|group| GroupName::parse(group).unwrap());
        let response = metadata
            .authorize_block_write(AuthorizeBlockWriteRequestProto {
                header: Some((&request_header).into()),
                block_id: Some(requested_block_id),
                worker_id: worker.worker_id,
                worker_run_id: worker.worker_run_id.clone(),
                fencing_token: target.fencing_token,
                write_offset: target.write_offset,
                block_size: target.block_size,
                tier: target.tier,
            })
            .await
            .unwrap()
            .into_inner();
        let response_header = response.header.unwrap();
        if allowed {
            assert!(response_header.error.is_none());
            assert_eq!(response_header.group_name, "root");
            assert_eq!(response.visible_len, 0);
        } else {
            let error = rpc_error_from_proto(&response_header.error.expect("matching scope and issued block required"));
            assert_eq!(error.kind, ErrorKind::Protocol(ProtocolErrorKind::PermissionDenied));
        }
    }

    let response = metadata
        .msync(MsyncRequestProto {
            header: Some((&child_header(&header)).into()),
        })
        .await
        .unwrap()
        .into_inner();
    let response_header = response.header.unwrap();
    assert!(response_header.error.is_none());
    assert_eq!(response_header.group_name, "root");
    let state = GroupStateWatermark::try_from(response_header.state.unwrap()).unwrap();
    assert_eq!(state.group_name.as_str(), "root");
    let allocated_state = GroupStateWatermark::try_from(allocated_header.state.unwrap()).unwrap();
    assert!(state.state_id >= allocated_state.state_id);
    cluster.shutdown().await.expect("shutdown cluster");
}

fn assert_not_found<T: Debug>(result: ClientResult<T>, context: &str) {
    let err = result.expect_err(context);
    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("not found") || message.contains("enoent"),
        "{context} should fail with not-found style error, got {err}"
    );
}

fn child_header(header: &RequestHeader) -> RequestHeader {
    let mut child = header.clone();
    child.client.call_id = beryl_types::CallId::new();
    child
}
