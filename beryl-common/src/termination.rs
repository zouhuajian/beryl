// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Process termination signal registration shared by Beryl servers.

use std::io;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Operating-system request that starts a graceful process shutdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminationKind {
    Interrupt,
    Terminate,
}

/// Owned signal task that publishes cancellation throughout process startup.
///
/// The monitor starts immediately after signal handlers are installed. Startup
/// code can inspect its token, while the process lifecycle owner retains and
/// awaits the task to identify the signal that requested shutdown.
pub struct TerminationMonitor {
    cancellation: CancellationToken,
    task: Option<JoinHandle<TerminationKind>>,
}

impl TerminationMonitor {
    /// Returns a token that is cancelled as soon as SIGINT/SIGTERM is received.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Returns whether a termination signal arrived during startup.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Consumes the installed signal task and returns the triggering signal.
    pub async fn recv(&mut self) -> Result<TerminationKind, tokio::task::JoinError> {
        let result = self.task.as_mut().expect("termination task is owned").await;
        self.task.take();
        result
    }

    /// Cancels and awaits the signal task when startup exits for another reason.
    pub async fn shutdown(&mut self) -> Result<(), tokio::task::JoinError> {
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        task.abort();
        match task.await {
            Ok(_) => Ok(()),
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(error),
        }
    }
}

impl Drop for TerminationMonitor {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Installed SIGINT/SIGTERM receivers for one server process.
///
/// On Unix, `install` registers both handlers synchronously. The process can
/// therefore receive either signal during startup without falling back to the
/// operating system's immediate default termination behavior.
#[cfg(unix)]
pub struct TerminationSignal {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl TerminationSignal {
    /// Installs SIGINT and SIGTERM handlers before long-lived services start.
    pub fn install() -> io::Result<Self> {
        Ok(Self {
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    /// Waits for the first termination signal delivered to this process.
    pub async fn recv(&mut self) -> TerminationKind {
        tokio::select! {
            _ = self.interrupt.recv() => TerminationKind::Interrupt,
            _ = self.terminate.recv() => TerminationKind::Terminate,
        }
    }

    /// Starts the process-owned task that drives startup cancellation.
    pub fn monitor(mut self) -> TerminationMonitor {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            let signal = self.recv().await;
            task_cancellation.cancel();
            signal
        });
        TerminationMonitor {
            cancellation,
            task: Some(task),
        }
    }
}

/// Installed Ctrl-C receiver for platforms without Unix signal streams.
#[cfg(not(unix))]
pub struct TerminationSignal;

#[cfg(not(unix))]
impl TerminationSignal {
    /// Installs the platform Ctrl-C receiver.
    pub fn install() -> io::Result<Self> {
        Ok(Self)
    }

    /// Waits for Ctrl-C on platforms without Unix signal streams.
    pub async fn recv(&mut self) -> TerminationKind {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "Failed to receive Ctrl-C");
        }
        TerminationKind::Interrupt
    }

    /// Starts the process-owned task that drives startup cancellation.
    pub fn monitor(mut self) -> TerminationMonitor {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            let signal = self.recv().await;
            task_cancellation.cancel();
            signal
        });
        TerminationMonitor {
            cancellation,
            task: Some(task),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct TaskDrop(Arc<AtomicBool>);

    impl Drop for TaskDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn cancelled_receive_keeps_signal_task_owned_until_shutdown() {
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = Arc::clone(&dropped);
        let task = tokio::spawn(async move {
            let _drop = TaskDrop(task_dropped);
            std::future::pending::<TerminationKind>().await
        });
        tokio::task::yield_now().await;
        let mut monitor = TerminationMonitor {
            cancellation: CancellationToken::new(),
            task: Some(task),
        };
        let receive_polled = Arc::new(AtomicBool::new(false));
        let branch_observed_poll = Arc::clone(&receive_polled);
        let receive_records_poll = Arc::clone(&receive_polled);

        tokio::select! {
            _ = async move {
                while !branch_observed_poll.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            } => {}
            _ = async {
                receive_records_poll.store(true, Ordering::Release);
                monitor.recv().await
            } => panic!("signal task must remain pending"),
        }
        assert!(receive_polled.load(Ordering::Acquire));
        monitor.shutdown().await.unwrap();

        assert!(dropped.load(Ordering::Acquire));
    }
}
