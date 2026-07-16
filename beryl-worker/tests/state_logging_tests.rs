// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::io;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use beryl_types::chunk::ByteRange;
use beryl_types::ids::{BlockId, BlockIndex, ClientId, DataHandleId};
use beryl_types::layout::BlockFormatId;
use beryl_types::lease::FencingToken;
use beryl_types::{GroupName, Tier, WorkerRunId};
use beryl_worker::store::block::{ChecksumKind, FullBlockFileStore, FullBlockFileStoreConfig};
use beryl_worker::{CommitWriteRequest, ReadOpenRequest, WorkerCore, WriteFrame, WriteOpenRequest};
use bytes::Bytes;
use tempfile::TempDir;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::{fmt, layer::SubscriberExt, Registry};

#[derive(Clone)]
struct LogCaptureWriter {
    output: Arc<Mutex<Vec<u8>>>,
}

impl LogCaptureWriter {
    fn new(output: Arc<Mutex<Vec<u8>>>) -> Self {
        Self { output }
    }
}

impl io::Write for LogCaptureWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.output
            .lock()
            .expect("log output must not be poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn captured_logs(output: &Arc<Mutex<Vec<u8>>>) -> Vec<serde_json::Value> {
    let bytes = output.lock().expect("log output must not be poisoned").clone();
    let text = String::from_utf8(bytes).expect("logs must be utf8");
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap_or_else(|err| panic!("invalid json log {line:?}: {err}")))
        .collect()
}

fn log_test_mutex() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn group_name() -> GroupName {
    GroupName::parse("root").expect("valid group")
}

fn worker_run_id() -> WorkerRunId {
    "550e8400-e29b-41d4-a716-000000000001"
        .parse()
        .expect("valid worker run id")
}

fn block_id() -> BlockId {
    BlockId::new(DataHandleId::new(7), BlockIndex::new(0))
}

fn token() -> FencingToken {
    FencingToken {
        block_id: block_id(),
        owner: ClientId::new(9),
        epoch: 1,
    }
}

fn open_request() -> WriteOpenRequest {
    WriteOpenRequest {
        group_name: group_name(),
        block_id: block_id(),
        worker_run_id: worker_run_id(),
        token: token(),
        block_stamp: 11,
        frame_size: 1024,
        block_size: 4096,
        block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
        chunk_size: 4096,
        effective_len: 1024,
        checksum_kind: ChecksumKind::None,
        tier: Tier::Hdd,
    }
}

fn core(temp_dir: &TempDir) -> WorkerCore {
    let store = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(
        temp_dir.path().to_path_buf(),
    )));
    WorkerCore::with_local_store(1024, 1024, 4096, Duration::from_secs(60), store)
}

#[tokio::test(flavor = "current_thread")]
async fn open_write_and_commit_emit_state_and_block_logs_without_info_frame_logs() {
    let _log_guard = log_test_mutex().lock().await;
    let temp_dir = TempDir::new().expect("temp dir");
    let core = core(&temp_dir);
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = LogCaptureWriter::new(Arc::clone(&output));
    let subscriber = Registry::default().with(
        fmt::layer()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .with_ansi(false)
            .with_target(true)
            .with_file(false)
            .with_line_number(false)
            .with_writer(move || writer.clone()),
    );

    let dispatch = tracing::Dispatch::new(subscriber);
    async {
        let open = core.open_write(open_request()).await.expect("open write");
        core.write_frame(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from(vec![1; 1024]),
            checksum32: 0,
        })
        .await
        .expect("write frame");
        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            group_name: group_name(),
            block_id: block_id(),
            worker_run_id: worker_run_id(),
            token: token(),
            commit_seq: 1,
            effective_len: 1024,
            block_stamp: 11,
            block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
            block_size: 4096,
            chunk_size: 4096,
            require_sync: true,
        })
        .await
        .expect("commit write");
    }
    .with_subscriber(dispatch.clone())
    .await;

    let logs = captured_logs(&output);
    assert!(
        logs.iter().any(|log| {
            log["target"] == "worker.state"
                && log["op"] == "OpenWrite"
                && log["result"] == "accepted"
                && log["block_id"] == block_id().to_string()
        }),
        "{logs:?}"
    );
    assert!(
        logs.iter().any(|log| {
            log["target"] == "worker.state"
                && log["op"] == "CommitWrite"
                && log["result"] == "completed"
                && log["committed_length"] == 1024
        }),
        "{logs:?}"
    );
    assert!(
        logs.iter().any(|log| {
            log["target"] == "worker.block"
                && log["op"] == "publish_ready"
                && log["result"] == "completed"
                && log["block_id"] == block_id().to_string()
        }),
        "{logs:?}"
    );
    assert!(
        logs.iter()
            .filter(|log| log["level"] == "INFO")
            .all(|log| log["op"] != "WriteStreamFrame"),
        "{logs:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn open_read_does_not_emit_state_change_log() {
    let _log_guard = log_test_mutex().lock().await;
    let temp_dir = TempDir::new().expect("temp dir");
    let core = core(&temp_dir);
    let open = core.open_write(open_request()).await.expect("open write");
    core.write_frame(WriteFrame {
        stream_id: open.stream_id,
        seq: 1,
        offset_in_block: 0,
        data: Bytes::from(vec![1; 1024]),
        checksum32: 0,
    })
    .await
    .expect("write frame");
    core.commit_write(CommitWriteRequest {
        stream_id: open.stream_id,
        group_name: group_name(),
        block_id: block_id(),
        worker_run_id: worker_run_id(),
        token: token(),
        commit_seq: 1,
        effective_len: 1024,
        block_stamp: 11,
        block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
        block_size: 4096,
        chunk_size: 4096,
        require_sync: true,
    })
    .await
    .expect("commit write");

    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = LogCaptureWriter::new(Arc::clone(&output));
    let subscriber = Registry::default().with(
        fmt::layer()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .with_ansi(false)
            .with_target(true)
            .with_file(false)
            .with_line_number(false)
            .with_writer(move || writer.clone()),
    );

    let dispatch = tracing::Dispatch::new(subscriber);
    async {
        core.open_read(ReadOpenRequest {
            group_name: group_name(),
            block_id: block_id(),
            worker_run_id: worker_run_id(),
            byte_range: ByteRange { offset: 0, len: 16 },
            block_stamp: 11,
            block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
            block_size: 4096,
            chunk_size: 4096,
            effective_len: 1024,
            frame_size: 1024,
        })
        .await
        .expect("open read");
    }
    .with_subscriber(dispatch.clone())
    .await;

    let logs = captured_logs(&output);
    assert!(logs.iter().all(|log| log["target"] != "worker.state"), "{logs:?}");
}
