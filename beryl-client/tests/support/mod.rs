// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use beryl_common::error::rpc::{ErrorKind, RefreshHint, RpcErrorDetail, WorkerErrorKind};
use beryl_common::header::{
    HEADER_WORKER_DATA_ERROR_DETAIL, HEADER_WORKER_DATA_REJECTION, WORKER_DATA_ERROR_DETAIL_V1,
    WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT,
};
use beryl_proto::common::{GroupStateWatermarkProto, RequestHeaderProto, ResponseHeaderProto};
use beryl_proto::convert::rpc_error_to_proto;
use beryl_proto::metadata::file_system_service_proto_server::{FileSystemServiceProto, FileSystemServiceProtoServer};
use beryl_proto::metadata::{
    AbortFileWriteRequestProto, AbortFileWriteResponseProto, AllocateBlockRequestProto, AllocateBlockResponseProto,
    CommitFileRequestProto, CommitFileResponseProto, CreateDirectoryRequestProto, CreateDirectoryResponseProto,
    CreateFileRequestProto, CreateFileResponseProto, DeleteRequestProto, DeleteResponseProto,
    GetBlockLocationsRequestProto, GetBlockLocationsResponseProto, GetStatusRequestProto, GetStatusResponseProto,
    ListStatusRequestProto, ListStatusResponseProto, MsyncRequestProto, MsyncResponseProto, OpenFileRequestProto,
    OpenFileResponseProto, OpenWriteRequestProto, OpenWriteResponseProto, RenameRequestProto, RenameResponseProto,
    RenewLeaseRequestProto, RenewLeaseResponseProto, SyncWriteRequestProto, SyncWriteResponseProto,
};
use beryl_proto::worker::worker_data_service_server::{WorkerDataService, WorkerDataServiceServer};
use beryl_proto::worker::{
    DataResponseHeaderProto, ReadBlockChunkProto, ReadBlockRequestProto, WriteBlockRequestProto,
    WriteBlockResponseProto,
};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use prost::Message;
use tokio::sync::oneshot;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub(crate) enum MetadataReply<T> {
    Success(T),
    SuccessWithAuthority(T, ResponseAuthority),
    Error(RpcErrorDetail),
    Status(Status),
    Blocked {
        body: T,
        started: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
}

impl<T> MetadataReply<T> {
    pub(crate) fn success(body: T) -> Self {
        Self::Success(body)
    }

    pub(crate) fn error(error: RpcErrorDetail) -> Self {
        Self::Error(error)
    }

    pub(crate) fn status(status: Status) -> Self {
        Self::Status(status)
    }
}

#[derive(Default)]
pub(crate) struct ResponseAuthority {
    pub(crate) state: Option<GroupStateWatermarkProto>,
}

#[derive(Default)]
pub(crate) struct MetadataScript {
    pub(crate) get_status: VecDeque<MetadataReply<GetStatusResponseProto>>,
    pub(crate) list_status: VecDeque<MetadataReply<ListStatusResponseProto>>,
    pub(crate) create_directory: VecDeque<MetadataReply<CreateDirectoryResponseProto>>,
    pub(crate) delete: VecDeque<MetadataReply<DeleteResponseProto>>,
    pub(crate) rename: VecDeque<MetadataReply<RenameResponseProto>>,
    pub(crate) open_file: VecDeque<MetadataReply<OpenFileResponseProto>>,
    pub(crate) get_block_locations: VecDeque<MetadataReply<GetBlockLocationsResponseProto>>,
    pub(crate) create_file: VecDeque<MetadataReply<CreateFileResponseProto>>,
    pub(crate) open_write: VecDeque<MetadataReply<OpenWriteResponseProto>>,
    pub(crate) allocate_block: VecDeque<MetadataReply<AllocateBlockResponseProto>>,
    pub(crate) commit_file: VecDeque<MetadataReply<CommitFileResponseProto>>,
    pub(crate) abort_file_write: VecDeque<MetadataReply<AbortFileWriteResponseProto>>,
    pub(crate) renew_lease: VecDeque<MetadataReply<RenewLeaseResponseProto>>,
    pub(crate) sync_write: VecDeque<MetadataReply<SyncWriteResponseProto>>,
    pub(crate) msync: VecDeque<MetadataReply<MsyncResponseProto>>,
}

#[derive(Clone, Debug)]
pub(crate) struct MetadataCall {
    pub(crate) method: &'static str,
    pub(crate) header: RequestHeaderProto,
}

#[derive(Clone)]
pub(crate) struct MockMetadata {
    state: Arc<MetadataState>,
}

struct MetadataState {
    script: Mutex<MetadataScript>,
    calls: Mutex<Vec<MetadataCall>>,
    allocations: Mutex<Vec<AllocateBlockRequestProto>>,
    layout_requests: Mutex<Vec<GetBlockLocationsRequestProto>>,
}

impl MockMetadata {
    pub(crate) fn new(script: MetadataScript) -> Self {
        Self {
            state: Arc::new(MetadataState {
                script: Mutex::new(script),
                calls: Mutex::new(Vec::new()),
                allocations: Mutex::new(Vec::new()),
                layout_requests: Mutex::new(Vec::new()),
            }),
        }
    }

    pub(crate) fn calls(&self) -> Vec<MetadataCall> {
        self.state.calls.lock().expect("metadata calls").clone()
    }

    pub(crate) fn allocations(&self) -> Vec<AllocateBlockRequestProto> {
        self.state.allocations.lock().expect("allocation requests").clone()
    }

    pub(crate) fn layout_requests(&self) -> Vec<GetBlockLocationsRequestProto> {
        self.state.layout_requests.lock().expect("layout requests").clone()
    }

    pub(crate) async fn start(&self) -> RunningServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock Metadata");
        let endpoint = listener.local_addr().expect("mock Metadata address").to_string();
        let incoming = TcpListenerStream::new(listener);
        let (shutdown, shutdown_signal) = oneshot::channel();
        let service = self.clone();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(FileSystemServiceProtoServer::new(service))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_signal.await;
                })
                .await
        });
        RunningServer {
            endpoint,
            shutdown,
            task,
        }
    }

    fn record(&self, method: &'static str, header: Option<&RequestHeaderProto>) -> Result<RequestHeaderProto, Status> {
        let header = header
            .cloned()
            .ok_or_else(|| Status::invalid_argument(format!("{method} request missing header")))?;
        self.state.calls.lock().expect("metadata calls").push(MetadataCall {
            method,
            header: header.clone(),
        });
        Ok(header)
    }
}

fn response_header(
    request: &RequestHeaderProto,
    error: Option<RpcErrorDetail>,
    authority: ResponseAuthority,
) -> ResponseHeaderProto {
    ResponseHeaderProto {
        client: request.client.clone(),
        error: error.as_ref().map(rpc_error_to_proto),
        state: authority.state,
        group_name: request.group_name.clone(),
    }
}

async fn metadata_response<T: Default>(
    request: &RequestHeaderProto,
    reply: Option<MetadataReply<T>>,
    method: &'static str,
    set_header: impl FnOnce(&mut T, ResponseHeaderProto),
) -> Result<Response<T>, Status> {
    match reply.unwrap_or_else(|| MetadataReply::Status(Status::failed_precondition(format!("unscripted {method}")))) {
        MetadataReply::Success(mut body) => {
            set_header(&mut body, response_header(request, None, ResponseAuthority::default()));
            Ok(Response::new(body))
        }
        MetadataReply::SuccessWithAuthority(mut body, authority) => {
            set_header(&mut body, response_header(request, None, authority));
            Ok(Response::new(body))
        }
        MetadataReply::Error(error) => {
            let mut body = T::default();
            set_header(
                &mut body,
                response_header(request, Some(error), ResponseAuthority::default()),
            );
            Ok(Response::new(body))
        }
        MetadataReply::Status(status) => Err(status),
        MetadataReply::Blocked {
            mut body,
            started,
            release,
        } => {
            let _ = started.send(());
            release.await.map_err(|_| Status::cancelled("metadata gate dropped"))?;
            set_header(&mut body, response_header(request, None, ResponseAuthority::default()));
            Ok(Response::new(body))
        }
    }
}

#[tonic::async_trait]
impl FileSystemServiceProto for MockMetadata {
    async fn authorize_block_write(
        &self,
        _request: Request<beryl_proto::metadata::AuthorizeBlockWriteRequestProto>,
    ) -> Result<Response<beryl_proto::metadata::AuthorizeBlockWriteResponseProto>, Status> {
        Err(Status::unimplemented(
            "client fixture does not serve Worker authorization",
        ))
    }

    async fn get_status(
        &self,
        request: Request<GetStatusRequestProto>,
    ) -> Result<Response<GetStatusResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("GetStatus", request.header.as_ref())?;
        let reply = self
            .state
            .script
            .lock()
            .expect("metadata script")
            .get_status
            .pop_front();
        metadata_response(&header, reply, "GetStatus", |body, header| body.header = Some(header)).await
    }

    async fn list_status(
        &self,
        request: Request<ListStatusRequestProto>,
    ) -> Result<Response<ListStatusResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("ListStatus", request.header.as_ref())?;
        let reply = self
            .state
            .script
            .lock()
            .expect("metadata script")
            .list_status
            .pop_front();
        metadata_response(&header, reply, "ListStatus", |body, header| body.header = Some(header)).await
    }

    async fn create_directory(
        &self,
        request: Request<CreateDirectoryRequestProto>,
    ) -> Result<Response<CreateDirectoryResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("CreateDirectory", request.header.as_ref())?;
        let reply = self
            .state
            .script
            .lock()
            .expect("metadata script")
            .create_directory
            .pop_front();
        metadata_response(&header, reply, "CreateDirectory", |body, header| {
            body.header = Some(header)
        })
        .await
    }

    async fn delete(&self, request: Request<DeleteRequestProto>) -> Result<Response<DeleteResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("Delete", request.header.as_ref())?;
        let reply = self.state.script.lock().expect("metadata script").delete.pop_front();
        metadata_response(&header, reply, "Delete", |body, header| body.header = Some(header)).await
    }

    async fn rename(&self, request: Request<RenameRequestProto>) -> Result<Response<RenameResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("Rename", request.header.as_ref())?;
        let reply = self.state.script.lock().expect("metadata script").rename.pop_front();
        metadata_response(&header, reply, "Rename", |body, header| body.header = Some(header)).await
    }

    async fn open_file(
        &self,
        request: Request<OpenFileRequestProto>,
    ) -> Result<Response<OpenFileResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("OpenFile", request.header.as_ref())?;
        let reply = self.state.script.lock().expect("metadata script").open_file.pop_front();
        metadata_response(&header, reply, "OpenFile", |body, header| body.header = Some(header)).await
    }

    async fn get_block_locations(
        &self,
        request: Request<GetBlockLocationsRequestProto>,
    ) -> Result<Response<GetBlockLocationsResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("GetBlockLocations", request.header.as_ref())?;
        self.state
            .layout_requests
            .lock()
            .expect("layout requests")
            .push(request);
        let reply = self
            .state
            .script
            .lock()
            .expect("metadata script")
            .get_block_locations
            .pop_front();
        metadata_response(&header, reply, "GetBlockLocations", |body, header| {
            body.header = Some(header)
        })
        .await
    }

    async fn create_file(
        &self,
        request: Request<CreateFileRequestProto>,
    ) -> Result<Response<CreateFileResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("CreateFile", request.header.as_ref())?;
        let reply = self
            .state
            .script
            .lock()
            .expect("metadata script")
            .create_file
            .pop_front();
        metadata_response(&header, reply, "CreateFile", |body, header| body.header = Some(header)).await
    }

    async fn open_write(
        &self,
        request: Request<OpenWriteRequestProto>,
    ) -> Result<Response<OpenWriteResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("OpenWrite", request.header.as_ref())?;
        let reply = self
            .state
            .script
            .lock()
            .expect("metadata script")
            .open_write
            .pop_front();
        metadata_response(&header, reply, "OpenWrite", |body, header| body.header = Some(header)).await
    }

    async fn allocate_block(
        &self,
        request: Request<AllocateBlockRequestProto>,
    ) -> Result<Response<AllocateBlockResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("AllocateBlock", request.header.as_ref())?;
        self.state
            .allocations
            .lock()
            .expect("allocation requests")
            .push(request);
        let reply = self
            .state
            .script
            .lock()
            .expect("metadata script")
            .allocate_block
            .pop_front();
        metadata_response(&header, reply, "AllocateBlock", |body, header| {
            body.header = Some(header)
        })
        .await
    }

    async fn commit_file(
        &self,
        request: Request<CommitFileRequestProto>,
    ) -> Result<Response<CommitFileResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("CommitFile", request.header.as_ref())?;
        let reply = self
            .state
            .script
            .lock()
            .expect("metadata script")
            .commit_file
            .pop_front();
        metadata_response(&header, reply, "CommitFile", |body, header| body.header = Some(header)).await
    }

    async fn abort_file_write(
        &self,
        request: Request<AbortFileWriteRequestProto>,
    ) -> Result<Response<AbortFileWriteResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("AbortFileWrite", request.header.as_ref())?;
        let reply = self
            .state
            .script
            .lock()
            .expect("metadata script")
            .abort_file_write
            .pop_front();
        metadata_response(&header, reply, "AbortFileWrite", |body, header| {
            body.header = Some(header)
        })
        .await
    }

    async fn renew_lease(
        &self,
        request: Request<RenewLeaseRequestProto>,
    ) -> Result<Response<RenewLeaseResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("RenewLease", request.header.as_ref())?;
        let reply = self
            .state
            .script
            .lock()
            .expect("metadata script")
            .renew_lease
            .pop_front();
        metadata_response(&header, reply, "RenewLease", |body, header| body.header = Some(header)).await
    }

    async fn sync_write(
        &self,
        request: Request<SyncWriteRequestProto>,
    ) -> Result<Response<SyncWriteResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("SyncWrite", request.header.as_ref())?;
        let reply = self
            .state
            .script
            .lock()
            .expect("metadata script")
            .sync_write
            .pop_front();
        metadata_response(&header, reply, "SyncWrite", |body, header| body.header = Some(header)).await
    }

    async fn msync(&self, request: Request<MsyncRequestProto>) -> Result<Response<MsyncResponseProto>, Status> {
        let request = request.into_inner();
        let header = self.record("Msync", request.header.as_ref())?;
        let reply = self.state.script.lock().expect("metadata script").msync.pop_front();
        metadata_response(&header, reply, "Msync", |body, header| body.header = Some(header)).await
    }
}

pub(crate) enum ReadReply {
    Data(Bytes),
    Chunks(Vec<Result<Bytes, Status>>),
    BlockedEnd {
        data: Bytes,
        started: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
    RefreshMetadata(WorkerErrorKind),
    BlockedRefresh {
        started: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
}

pub(crate) enum WriteReply {
    Success,
    CapacityRejected,
    AckThenUnavailable,
    BlockedOpen {
        started: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
    BlockedFinish {
        started: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
}

#[derive(Default)]
pub(crate) struct WorkerScript {
    pub(crate) reads: VecDeque<ReadReply>,
    pub(crate) writes: VecDeque<WriteReply>,
}

#[derive(Clone)]
pub(crate) struct MockWorker {
    state: Arc<WorkerState>,
}

struct WorkerState {
    script: Mutex<WorkerScript>,
    read_requests: Mutex<Vec<ReadBlockRequestProto>>,
    write_calls: AtomicUsize,
    write_data_frames: AtomicUsize,
    write_completions: AtomicUsize,
    write_cancellations: AtomicUsize,
    written_data: Mutex<Vec<Bytes>>,
}

impl MockWorker {
    pub(crate) fn new(script: WorkerScript) -> Self {
        Self {
            state: Arc::new(WorkerState {
                script: Mutex::new(script),
                read_requests: Mutex::new(Vec::new()),
                write_calls: AtomicUsize::new(0),
                write_data_frames: AtomicUsize::new(0),
                write_completions: AtomicUsize::new(0),
                write_cancellations: AtomicUsize::new(0),
                written_data: Mutex::new(Vec::new()),
            }),
        }
    }

    pub(crate) fn read_calls(&self) -> usize {
        self.state.read_requests.lock().expect("read requests").len()
    }

    pub(crate) fn read_requests(&self) -> Vec<ReadBlockRequestProto> {
        self.state.read_requests.lock().expect("read requests").clone()
    }

    pub(crate) fn write_calls(&self) -> usize {
        self.state.write_calls.load(Ordering::SeqCst)
    }

    pub(crate) fn write_data_frames(&self) -> usize {
        self.state.write_data_frames.load(Ordering::SeqCst)
    }

    pub(crate) fn write_completions(&self) -> usize {
        self.state.write_completions.load(Ordering::SeqCst)
    }

    pub(crate) fn written_data(&self) -> Vec<u8> {
        self.state
            .written_data
            .lock()
            .unwrap()
            .iter()
            .flat_map(|data| data.iter().copied())
            .collect()
    }

    pub(crate) fn write_cancellations(&self) -> usize {
        self.state.write_cancellations.load(Ordering::SeqCst)
    }

    pub(crate) async fn start(&self) -> RunningServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock Worker");
        let endpoint = listener.local_addr().expect("mock Worker address").to_string();
        let incoming = TcpListenerStream::new(listener);
        let (shutdown, shutdown_signal) = oneshot::channel();
        let service = self.clone();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(WorkerDataServiceServer::new(service))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_signal.await;
                })
                .await
        });
        RunningServer {
            endpoint,
            shutdown,
            task,
        }
    }
}

type WorkerStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl WorkerDataService for MockWorker {
    type ReadBlockStream = WorkerStream<ReadBlockChunkProto>;

    async fn read_block(
        &self,
        request: Request<ReadBlockRequestProto>,
    ) -> Result<Response<Self::ReadBlockStream>, Status> {
        let request = request.into_inner();
        self.state
            .read_requests
            .lock()
            .expect("read requests")
            .push(request.clone());
        let reply = self
            .state
            .script
            .lock()
            .expect("worker script")
            .reads
            .pop_front()
            .ok_or_else(|| Status::failed_precondition("unscripted ReadBlock"))?;
        match reply {
            ReadReply::Data(data) => {
                let range = request
                    .byte_range
                    .ok_or_else(|| Status::invalid_argument("ReadBlock missing byte range"))?;
                let start = usize::try_from(range.offset)
                    .map_err(|_| Status::out_of_range("ReadBlock offset exceeds usize"))?;
                let end = start
                    .checked_add(range.len as usize)
                    .ok_or_else(|| Status::out_of_range("ReadBlock range overflow"))?;
                let bytes = data
                    .get(start..end)
                    .ok_or_else(|| Status::out_of_range("ReadBlock range exceeds scripted data"))?;
                let bytes = Bytes::copy_from_slice(bytes);
                Ok(Response::new(Box::pin(futures::stream::once(async move {
                    Ok(ReadBlockChunkProto { data: bytes })
                }))))
            }
            ReadReply::Chunks(chunks) => Ok(Response::new(Box::pin(futures::stream::iter(
                chunks
                    .into_iter()
                    .map(|chunk| chunk.map(|data| ReadBlockChunkProto { data })),
            )))),
            ReadReply::BlockedEnd { data, started, release } => {
                let end = futures::stream::once(async move {
                    let _ = started.send(());
                    let _ = release.await;
                })
                .filter_map(|()| futures::future::ready(None));
                Ok(Response::new(Box::pin(
                    futures::stream::iter([Ok(ReadBlockChunkProto { data })]).chain(end),
                )))
            }
            ReadReply::RefreshMetadata(kind) => Err(worker_refresh_status(request.header.as_ref(), kind)),
            ReadReply::BlockedRefresh { started, release } => {
                let _ = started.send(());
                release.await.map_err(|_| Status::cancelled("read gate dropped"))?;
                Err(worker_refresh_status(
                    request.header.as_ref(),
                    WorkerErrorKind::RunMismatch,
                ))
            }
        }
    }

    type WriteBlockStream = WorkerStream<WriteBlockResponseProto>;

    async fn write_block(
        &self,
        request: Request<tonic::Streaming<WriteBlockRequestProto>>,
    ) -> Result<Response<Self::WriteBlockStream>, Status> {
        self.state.write_calls.fetch_add(1, Ordering::SeqCst);
        let reply = self
            .state
            .script
            .lock()
            .expect("worker script")
            .writes
            .pop_front()
            .ok_or_else(|| Status::failed_precondition("unscripted WriteBlock"))?;
        if matches!(reply, WriteReply::CapacityRejected) {
            let mut metadata = tonic::metadata::MetadataMap::new();
            metadata.insert(
                HEADER_WORKER_DATA_REJECTION,
                tonic::metadata::MetadataValue::from_static(WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT),
            );
            return Err(Status::with_metadata(
                tonic::Code::ResourceExhausted,
                "mock Worker capacity exhausted",
                metadata,
            ));
        }

        let mut requests = request.into_inner();
        let command = requests
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("WriteBlock missing command"))?;
        let Some(beryl_proto::worker::write_block_request_proto::Payload::Command(_)) = command.payload else {
            return Err(Status::invalid_argument("WriteBlock first payload must be command"));
        };
        let reply = match reply {
            WriteReply::BlockedOpen { started, release } => {
                let _ = started.send(());
                release
                    .await
                    .map_err(|_| Status::cancelled("write opening gate dropped"))?;
                WriteReply::Success
            }
            reply => reply,
        };

        let (responses, response_stream) = tokio::sync::mpsc::channel(2);
        let _ = responses.send(Ok(WriteBlockResponseProto {})).await;
        match reply {
            WriteReply::Success | WriteReply::BlockedFinish { .. } => {
                let finish_gate = match reply {
                    WriteReply::BlockedFinish { started, release } => Some((started, release)),
                    _ => None,
                };
                let state = Arc::clone(&self.state);
                tokio::spawn(async move {
                    loop {
                        match requests.message().await {
                            Ok(Some(request)) => match request.payload {
                                Some(beryl_proto::worker::write_block_request_proto::Payload::Data(data)) => {
                                    state.write_data_frames.fetch_add(1, Ordering::SeqCst);
                                    state.written_data.lock().unwrap().push(data);
                                }
                                _ => {
                                    state.write_cancellations.fetch_add(1, Ordering::SeqCst);
                                    let _ = responses
                                        .send(Err(Status::cancelled("mock Worker received write cancellation")))
                                        .await;
                                    break;
                                }
                            },
                            Ok(None) => {
                                if let Some((started, release)) = finish_gate {
                                    let _ = started.send(());
                                    tokio::select! {
                                        _ = release => {},
                                        _ = responses.closed() => return,
                                    }
                                }
                                state.write_completions.fetch_add(1, Ordering::SeqCst);
                                break;
                            }
                            Err(error) => {
                                let _ = responses
                                    .send(Err(Status::unavailable(format!(
                                        "mock Worker request stream failed: {error}"
                                    ))))
                                    .await;
                                break;
                            }
                        }
                    }
                    drop(responses);
                });
            }
            WriteReply::AckThenUnavailable => {
                let _ = responses
                    .send(Err(Status::unavailable(
                        "mock Worker unavailable after acknowledgement",
                    )))
                    .await;
            }
            WriteReply::CapacityRejected => unreachable!("capacity rejection returned before acknowledgement"),
            WriteReply::BlockedOpen { .. } => unreachable!("opening gate already resolved"),
        }
        Ok(Response::new(Box::pin(ReceiverStream::new(response_stream))))
    }
}

fn worker_refresh_status(
    header: Option<&beryl_proto::worker::DataRequestHeaderProto>,
    kind: WorkerErrorKind,
) -> Status {
    let error = RpcErrorDetail::refresh_metadata(
        ErrorKind::Worker(kind),
        RefreshHint::default(),
        "scripted stale Worker location",
    );
    let response = DataResponseHeaderProto {
        client: header.and_then(|header| header.client.clone()),
        error: Some(rpc_error_to_proto(&error)),
    };
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        HEADER_WORKER_DATA_ERROR_DETAIL,
        WORKER_DATA_ERROR_DETAIL_V1
            .parse()
            .expect("worker error detail version"),
    );
    Status::with_details_and_metadata(
        tonic::Code::FailedPrecondition,
        error.message,
        Bytes::from(response.encode_to_vec()),
        metadata,
    )
}

pub(crate) struct RunningServer {
    endpoint: String,
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl RunningServer {
    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) async fn shutdown(self) {
        let _ = self.shutdown.send(());
        self.task
            .await
            .expect("mock server task")
            .expect("mock server shutdown");
    }
}
