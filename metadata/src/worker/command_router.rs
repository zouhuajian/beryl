// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker command routing boundary between worker RPC and command sources.

use super::delete_executor::DeleteExecutor;
use super::repair::{ErrorClass, RepairQueue, RepairTask, RepairTaskId, RepairTaskRecord, TaskAckStatus};
use parking_lot::Mutex;
use proto::metadata::{
    worker_command_proto, DeleteBlockStatusProto, ErrorClassProto, EvictCommandProto, MoveCopyCommandProto,
    ReplicateCommandProto, TaskAckProto, TaskAckStatusProto, WorkerCommandProto,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};
use types::ids::WorkerId;

const DEFAULT_ROUTE_TTL_MS: u64 = 10 * 60 * 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum WorkerCommandSourceKind {
    Delete,
    Repair,
}

pub(crate) struct WorkerCommandEnvelope {
    pub(crate) source: WorkerCommandSourceKind,
    pub(crate) internal_task_id: u64,
    pub(crate) command: WorkerCommandProto,
}

#[async_trait::async_trait]
pub(crate) trait WorkerCommandSource: Send + Sync {
    fn kind(&self) -> WorkerCommandSourceKind;

    fn poll_commands(&self, worker_id: WorkerId, max: usize) -> Vec<WorkerCommandEnvelope>;

    async fn handle_ack(&self, worker_id: WorkerId, ack: TaskAckProto, internal_task_id: u64);
}

#[derive(Clone, Copy, Debug)]
struct WorkerCommandRoute {
    source: WorkerCommandSourceKind,
    internal_task_id: u64,
    created_at_ms: u64,
}

struct RegisteredCommandSource {
    source: Arc<dyn WorkerCommandSource>,
    max_per_poll: usize,
}

pub(crate) struct WorkerCommandRouter {
    sources: Vec<RegisteredCommandSource>,
    routes: Mutex<HashMap<u64, WorkerCommandRoute>>,
    next_external_task_id: AtomicU64,
    route_ttl_ms: u64,
}

impl WorkerCommandRouter {
    pub(crate) fn new() -> Self {
        Self {
            sources: Vec::new(),
            routes: Mutex::new(HashMap::new()),
            next_external_task_id: AtomicU64::new(1),
            route_ttl_ms: DEFAULT_ROUTE_TTL_MS,
        }
    }

    #[cfg(test)]
    fn with_mapping_ttl_ms(route_ttl_ms: u64) -> Self {
        Self {
            sources: Vec::new(),
            routes: Mutex::new(HashMap::new()),
            next_external_task_id: AtomicU64::new(1),
            route_ttl_ms,
        }
    }

    pub(crate) fn register_source(&mut self, source: Arc<dyn WorkerCommandSource>, max_per_poll: usize) {
        self.sources.push(RegisteredCommandSource { source, max_per_poll });
    }

    #[cfg(test)]
    pub(crate) fn source_count(&self) -> usize {
        self.sources.len()
    }

    pub(crate) fn poll_commands(&self, worker_id: WorkerId, budget: usize) -> Vec<WorkerCommandProto> {
        if budget == 0 {
            return Vec::new();
        }

        self.cleanup_expired_routes(now_ms());

        let mut commands = Vec::with_capacity(budget);
        for registered in &self.sources {
            let remaining = budget.saturating_sub(commands.len());
            if remaining == 0 {
                break;
            }

            let max = remaining.min(registered.max_per_poll);
            for envelope in registered.source.poll_commands(worker_id, max) {
                if commands.len() >= budget {
                    break;
                }
                let external_task_id = self.next_external_task_id.fetch_add(1, Ordering::Relaxed);
                let route = WorkerCommandRoute {
                    source: envelope.source,
                    internal_task_id: envelope.internal_task_id,
                    created_at_ms: now_ms(),
                };
                self.routes.lock().insert(external_task_id, route);

                let mut command = envelope.command;
                command.task_id = external_task_id;
                commands.push(command);
            }
        }

        commands
    }

    pub(crate) async fn handle_acks(&self, worker_id: WorkerId, acks: &[TaskAckProto]) {
        if acks.is_empty() {
            return;
        }

        self.cleanup_expired_routes(now_ms());

        for ack in acks {
            let route = {
                let mut routes = self.routes.lock();
                let Some(route) = routes.get(&ack.task_id).copied() else {
                    warn!(
                        worker_id = worker_id.as_raw(),
                        task_id = ack.task_id,
                        "Ignoring ack for unknown worker command task_id"
                    );
                    continue;
                };
                if should_remove_route(route.source, ack) {
                    routes.remove(&ack.task_id);
                }
                route
            };

            let Some(source) = self.source(route.source) else {
                warn!(
                    worker_id = worker_id.as_raw(),
                    source = ?route.source,
                    task_id = ack.task_id,
                    "Ignoring ack because command source is no longer registered"
                );
                continue;
            };

            source.handle_ack(worker_id, ack.clone(), route.internal_task_id).await;
        }
    }

    fn source(&self, kind: WorkerCommandSourceKind) -> Option<Arc<dyn WorkerCommandSource>> {
        self.sources
            .iter()
            .find(|registered| registered.source.kind() == kind)
            .map(|registered| Arc::clone(&registered.source))
    }

    fn cleanup_expired_routes(&self, now_ms: u64) {
        let mut routes = self.routes.lock();
        routes.retain(|_external_task_id, route| now_ms.saturating_sub(route.created_at_ms) <= self.route_ttl_ms);
    }
}

pub(crate) struct DeleteCommandSource {
    delete_executor: Arc<DeleteExecutor>,
}

impl DeleteCommandSource {
    pub(crate) fn new(delete_executor: Arc<DeleteExecutor>) -> Self {
        Self { delete_executor }
    }
}

#[async_trait::async_trait]
impl WorkerCommandSource for DeleteCommandSource {
    fn kind(&self) -> WorkerCommandSourceKind {
        WorkerCommandSourceKind::Delete
    }

    fn poll_commands(&self, worker_id: WorkerId, max: usize) -> Vec<WorkerCommandEnvelope> {
        self.delete_executor
            .get_pending_commands(worker_id, max)
            .into_iter()
            .map(|command| WorkerCommandEnvelope {
                source: self.kind(),
                internal_task_id: command.task_id,
                command,
            })
            .collect()
    }

    async fn handle_ack(&self, worker_id: WorkerId, mut ack: TaskAckProto, internal_task_id: u64) {
        ack.task_id = internal_task_id;
        self.delete_executor.process_task_acks(worker_id, &[ack]).await;
    }
}

pub(crate) struct RepairCommandSource {
    repair_queue: Arc<RepairQueue>,
}

impl RepairCommandSource {
    pub(crate) fn new(repair_queue: Arc<RepairQueue>) -> Self {
        Self { repair_queue }
    }

    fn record_to_command(record: RepairTaskRecord) -> Option<WorkerCommandProto> {
        let task_id = record.id.0;
        let command = match record.task {
            RepairTask::Replicate {
                block_id,
                src_workers: _,
                target_worker,
                ..
            } => {
                let proto_block_id = proto::common::BlockIdProto {
                    data_handle_id: block_id.data_handle_id.as_raw(),
                    block_index: block_id.index.as_raw(),
                };

                Some(worker_command_proto::Command::Replicate(ReplicateCommandProto {
                    block_id: Some(proto_block_id),
                    target_worker_ids: vec![target_worker.as_raw()],
                }))
            }
            RepairTask::Evict {
                block_id,
                reason,
                target_worker: _,
            } => {
                let proto_block_id = proto::common::BlockIdProto {
                    data_handle_id: block_id.data_handle_id.as_raw(),
                    block_index: block_id.index.as_raw(),
                };

                Some(worker_command_proto::Command::Evict(EvictCommandProto {
                    block_ids: vec![proto_block_id],
                    reason,
                    intent_id: 0,
                    op_kind: proto::metadata::DeleteOpKindProto::DeleteOpKindDelete as i32,
                    not_before_ms: 0,
                    expected_epoch: 0,
                }))
            }
            RepairTask::MoveCopy {
                block_id,
                from_worker,
                to_worker,
            } => {
                let proto_block_id = proto::common::BlockIdProto {
                    data_handle_id: block_id.data_handle_id.as_raw(),
                    block_index: block_id.index.as_raw(),
                };

                Some(worker_command_proto::Command::MoveCopy(MoveCopyCommandProto {
                    block_id: Some(proto_block_id),
                    from_worker_id: from_worker.as_raw(),
                    to_worker_id: to_worker.as_raw(),
                }))
            }
        };

        command.map(|command| WorkerCommandProto {
            task_id,
            command: Some(command),
        })
    }
}

#[async_trait::async_trait]
impl WorkerCommandSource for RepairCommandSource {
    fn kind(&self) -> WorkerCommandSourceKind {
        WorkerCommandSourceKind::Repair
    }

    fn poll_commands(&self, worker_id: WorkerId, max: usize) -> Vec<WorkerCommandEnvelope> {
        self.repair_queue
            .poll_for_worker(worker_id, max)
            .into_iter()
            .filter_map(Self::record_to_command)
            .map(|command| WorkerCommandEnvelope {
                source: self.kind(),
                internal_task_id: command.task_id,
                command,
            })
            .collect()
    }

    async fn handle_ack(&self, worker_id: WorkerId, ack: TaskAckProto, internal_task_id: u64) {
        let task_id = RepairTaskId(internal_task_id);
        let status = match ack.status() {
            TaskAckStatusProto::TaskAckStatusSuccess => TaskAckStatus::Success,
            TaskAckStatusProto::TaskAckStatusFailed => TaskAckStatus::Failed,
            TaskAckStatusProto::TaskAckStatusRetryableFailed => TaskAckStatus::RetryableFailed,
            _ => {
                warn!(task_id = task_id.0, "Unknown ack status, treating as failed");
                TaskAckStatus::Failed
            }
        };

        let message = if ack.error_message.is_empty() {
            None
        } else {
            Some(ack.error_message.clone())
        };

        let error_class = match ack.error_class() {
            ErrorClassProto::ErrorClassOk => Some(ErrorClass::Ok),
            ErrorClassProto::ErrorClassRetryable => Some(ErrorClass::Retryable),
            ErrorClassProto::ErrorClassFatal => Some(ErrorClass::Fatal),
            ErrorClassProto::ErrorClassNeedRefresh => Some(ErrorClass::NeedRefresh),
            _ => None,
        };

        match self.repair_queue.ack(task_id, worker_id, status, message, error_class) {
            Ok(Some(followup_task)) => {
                if let Err(e) = self.repair_queue.enqueue(followup_task) {
                    warn!(
                        task_id = task_id.0,
                        error = %e,
                        "Failed to enqueue followup Evict task after MoveCopy"
                    );
                } else {
                    info!(task_id = task_id.0, "MoveCopy completed, enqueued followup Evict task");
                }
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    task_id = task_id.0,
                    worker_id = worker_id.as_raw(),
                    error = %e,
                    "Failed to process task ack"
                );
            }
        }
    }
}

fn should_remove_route(source: WorkerCommandSourceKind, ack: &TaskAckProto) -> bool {
    match source {
        WorkerCommandSourceKind::Repair => true,
        WorkerCommandSourceKind::Delete => delete_ack_is_terminal(ack),
    }
}

fn delete_ack_is_terminal(ack: &TaskAckProto) -> bool {
    if ack.block_results.is_empty() {
        return !matches!(
            ack.status(),
            TaskAckStatusProto::TaskAckStatusRetryableFailed | TaskAckStatusProto::TaskAckStatusUnspecified
        );
    }

    ack.block_results.iter().all(|result| {
        matches!(
            result.status(),
            DeleteBlockStatusProto::DeleteBlockStatusDeleted
                | DeleteBlockStatusProto::DeleteBlockStatusTombstoned
                | DeleteBlockStatusProto::DeleteBlockStatusNotFound
                | DeleteBlockStatusProto::DeleteBlockStatusFailedFatal
        )
    })
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::metadata::{
        worker_command_proto, EvictCommandProto, TaskAckProto, TaskAckStatusProto, WorkerCommandProto,
    };
    use std::sync::Arc;
    use types::ids::WorkerId;

    #[derive(Clone)]
    struct FakeCommandSource {
        kind: WorkerCommandSourceKind,
        commands: Arc<parking_lot::Mutex<Vec<WorkerCommandProto>>>,
        handled: Arc<parking_lot::Mutex<Vec<(WorkerId, u64, TaskAckProto)>>>,
    }

    impl FakeCommandSource {
        fn new(kind: WorkerCommandSourceKind, commands: Vec<WorkerCommandProto>) -> Self {
            Self {
                kind,
                commands: Arc::new(parking_lot::Mutex::new(commands)),
                handled: Arc::new(parking_lot::Mutex::new(Vec::new())),
            }
        }

        fn handled(&self) -> Vec<(WorkerId, u64, TaskAckProto)> {
            self.handled.lock().clone()
        }
    }

    #[async_trait::async_trait]
    impl WorkerCommandSource for FakeCommandSource {
        fn kind(&self) -> WorkerCommandSourceKind {
            self.kind
        }

        fn poll_commands(&self, _worker_id: WorkerId, max: usize) -> Vec<WorkerCommandEnvelope> {
            let limit = max.min(self.commands.lock().len());
            self.commands
                .lock()
                .drain(..limit)
                .map(|command| WorkerCommandEnvelope {
                    source: self.kind,
                    internal_task_id: command.task_id,
                    command,
                })
                .collect()
        }

        async fn handle_ack(&self, worker_id: WorkerId, ack: TaskAckProto, internal_task_id: u64) {
            self.handled.lock().push((worker_id, internal_task_id, ack));
        }
    }

    fn command(task_id: u64) -> WorkerCommandProto {
        WorkerCommandProto {
            task_id,
            command: Some(worker_command_proto::Command::Evict(EvictCommandProto {
                block_ids: Vec::new(),
                reason: "test".to_string(),
                intent_id: 0,
                op_kind: 0,
                not_before_ms: 0,
                expected_epoch: 0,
            })),
        }
    }

    fn success_ack(task_id: u64) -> TaskAckProto {
        TaskAckProto {
            task_id,
            status: TaskAckStatusProto::TaskAckStatusSuccess as i32,
            error_message: String::new(),
            error_class: 0,
            error_code: String::new(),
            verify_ok: true,
            block_results: Vec::new(),
            intent_id: 0,
        }
    }

    #[tokio::test]
    async fn router_rewrites_task_id_and_routes_ack_to_source_internal_id() {
        let source = Arc::new(FakeCommandSource::new(
            WorkerCommandSourceKind::Repair,
            vec![command(41)],
        ));
        let mut router = WorkerCommandRouter::new();
        router.register_source(source.clone(), 8);

        let worker_id = WorkerId::new(7);
        let commands = router.poll_commands(worker_id, 8);
        assert_eq!(commands.len(), 1);
        assert_ne!(commands[0].task_id, 41);

        router.handle_acks(worker_id, &[success_ack(commands[0].task_id)]).await;

        let handled = source.handled();
        assert_eq!(handled.len(), 1);
        assert_eq!(handled[0].0, worker_id);
        assert_eq!(handled[0].1, 41);
        assert_eq!(handled[0].2.task_id, commands[0].task_id);
    }

    #[tokio::test]
    async fn delete_and_repair_internal_task_id_one_do_not_cross_route() {
        let delete = Arc::new(FakeCommandSource::new(
            WorkerCommandSourceKind::Delete,
            vec![command(1)],
        ));
        let repair = Arc::new(FakeCommandSource::new(
            WorkerCommandSourceKind::Repair,
            vec![command(1)],
        ));
        let mut router = WorkerCommandRouter::new();
        router.register_source(delete.clone(), 4);
        router.register_source(repair.clone(), 8);

        let worker_id = WorkerId::new(9);
        let commands = router.poll_commands(worker_id, 12);
        assert_eq!(commands.len(), 2);
        assert_ne!(commands[0].task_id, commands[1].task_id);

        router.handle_acks(worker_id, &[success_ack(commands[1].task_id)]).await;

        assert!(delete.handled().is_empty());
        let repair_handled = repair.handled();
        assert_eq!(repair_handled.len(), 1);
        assert_eq!(repair_handled[0].1, 1);
    }

    #[tokio::test]
    async fn unknown_ack_is_ignored_without_panic() {
        let source = Arc::new(FakeCommandSource::new(WorkerCommandSourceKind::Repair, Vec::new()));
        let mut router = WorkerCommandRouter::new();
        router.register_source(source.clone(), 8);

        router.handle_acks(WorkerId::new(3), &[success_ack(999_999)]).await;

        assert!(source.handled().is_empty());
    }

    #[tokio::test]
    async fn expired_route_is_cleaned_up_opportunistically() {
        let source = Arc::new(FakeCommandSource::new(
            WorkerCommandSourceKind::Repair,
            vec![command(11)],
        ));
        let mut router = WorkerCommandRouter::with_mapping_ttl_ms(1);
        router.register_source(source.clone(), 8);

        let worker_id = WorkerId::new(4);
        let commands = router.poll_commands(worker_id, 8);
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        router.handle_acks(worker_id, &[success_ack(commands[0].task_id)]).await;

        assert!(source.handled().is_empty());
    }
}
