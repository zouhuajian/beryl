// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker command routing boundary between worker RPC and command sources.

use crate::maintenance::delete::DeleteExecutor;
use crate::maintenance::repair::{RepairQueue, RepairTask, RepairTaskRecord};
use proto::metadata::{worker_command_proto, EvictCommandProto, ReplicateCommandProto, WorkerCommandProto};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use types::ids::WorkerId;

pub(crate) struct WorkerCommandEnvelope {
    pub(crate) command: WorkerCommandProto,
}

#[async_trait::async_trait]
pub(crate) trait WorkerCommandSource: Send + Sync {
    fn poll_commands(&self, worker_id: WorkerId, max: usize) -> Vec<WorkerCommandEnvelope>;
}

struct RegisteredCommandSource {
    source: Arc<dyn WorkerCommandSource>,
    max_per_poll: usize,
}

pub(crate) struct WorkerCommandRouter {
    sources: Vec<RegisteredCommandSource>,
    next_external_task_id: AtomicU64,
}

impl WorkerCommandRouter {
    pub(crate) fn new() -> Self {
        Self {
            sources: Vec::new(),
            next_external_task_id: AtomicU64::new(1),
        }
    }

    pub(crate) fn register_source(&mut self, source: Arc<dyn WorkerCommandSource>, max_per_poll: usize) {
        self.sources.push(RegisteredCommandSource { source, max_per_poll });
    }

    pub(crate) fn poll_commands(&self, worker_id: WorkerId, budget: usize) -> Vec<WorkerCommandProto> {
        if budget == 0 {
            return Vec::new();
        }

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
                let mut command = envelope.command;
                command.task_id = external_task_id;
                commands.push(command);
            }
        }

        commands
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
    fn poll_commands(&self, worker_id: WorkerId, max: usize) -> Vec<WorkerCommandEnvelope> {
        self.delete_executor
            .get_pending_commands(worker_id, max)
            .into_iter()
            .map(|command| WorkerCommandEnvelope { command })
            .collect()
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
            } => Some(worker_command_proto::Command::Replicate(ReplicateCommandProto {
                block_id: Some(block_id.into()),
                target_worker_ids: vec![target_worker.as_raw()],
            })),
            RepairTask::EvictReplica {
                block_id,
                reason,
                target_worker: _,
            } => Some(worker_command_proto::Command::Evict(EvictCommandProto {
                block_ids: vec![block_id.into()],
                reason,
                intent_id: 0,
                op_kind: proto::metadata::DeleteOpKindProto::DeleteOpKindReplicaEvict as i32,
                not_before_ms: 0,
                expected_epoch: 0,
            })),
        };

        command.map(|command| WorkerCommandProto {
            task_id,
            command: Some(command),
        })
    }
}

#[async_trait::async_trait]
impl WorkerCommandSource for RepairCommandSource {
    fn poll_commands(&self, worker_id: WorkerId, max: usize) -> Vec<WorkerCommandEnvelope> {
        self.repair_queue
            .poll_for_worker(worker_id, max)
            .into_iter()
            .filter_map(Self::record_to_command)
            .map(|command| WorkerCommandEnvelope { command })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::metadata::{worker_command_proto, EvictCommandProto, WorkerCommandProto};
    use std::sync::Arc;
    use types::ids::WorkerId;

    #[derive(Clone)]
    struct FakeCommandSource {
        commands: Arc<parking_lot::Mutex<Vec<WorkerCommandProto>>>,
    }

    impl FakeCommandSource {
        fn new(commands: Vec<WorkerCommandProto>) -> Self {
            Self {
                commands: Arc::new(parking_lot::Mutex::new(commands)),
            }
        }
    }

    #[async_trait::async_trait]
    impl WorkerCommandSource for FakeCommandSource {
        fn poll_commands(&self, _worker_id: WorkerId, max: usize) -> Vec<WorkerCommandEnvelope> {
            let limit = max.min(self.commands.lock().len());
            self.commands
                .lock()
                .drain(..limit)
                .map(|command| WorkerCommandEnvelope { command })
                .collect()
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

    #[test]
    fn router_rewrites_task_ids_and_honors_budget() {
        let source = Arc::new(FakeCommandSource::new(vec![command(41), command(42)]));
        let mut router = WorkerCommandRouter::new();
        router.register_source(source.clone(), 8);

        let worker_id = WorkerId::new(7);
        let commands = router.poll_commands(worker_id, 1);
        assert_eq!(commands.len(), 1);
        assert_ne!(commands[0].task_id, 41);
    }
}
