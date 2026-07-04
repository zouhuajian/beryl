// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Audit logging: independent file, daily rotation, async queue.

use anyhow::{Context, Result};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::time::{Instant, interval};
use tracing::{error, info, warn};

/// Audit record.
#[derive(Clone, Debug, Serialize)]
pub struct AuditRecord {
    /// Timestamp (ISO 8601).
    pub timestamp: String,
    /// Vecton application call correlation ID.
    pub call_id: String,
    /// Client ID.
    pub client_id: u128,
    /// Operation name.
    pub operation: String,
    /// File path (primary key for audit queries).
    pub path: Option<String>,
    /// Block ID (format: data_handle_id:block_index).
    pub block_id: Option<String>,
    /// Chunk reference (format: data_handle_id:block_index:chunk_idx).
    pub chunk_ref: Option<String>,
    /// Request source.
    pub source: String,
    /// Result.
    pub result: String,
    /// Bytes transferred.
    pub bytes: u64,
    /// Latency in milliseconds.
    pub latency_ms: f64,
}

/// Audit logger with async queue and daily rotation.
pub struct AuditLogger {
    /// Sender for audit records.
    sender: mpsc::UnboundedSender<AuditRecord>,
    /// Current log file path retained with the logger state.
    #[allow(dead_code)]
    current_log_path: PathBuf,
    /// Base directory retained with the logger state.
    #[allow(dead_code)]
    base_dir: PathBuf,
}

impl AuditLogger {
    /// Create a new AuditLogger.
    pub fn new<P: AsRef<Path>>(base_dir: P) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let (sender, receiver) = mpsc::unbounded_channel();

        // Ensure base directory exists
        std::fs::create_dir_all(&base_dir).context("Failed to create audit log directory")?;

        let current_log_path = Self::log_path_for_date(&base_dir, SystemTime::now())?;
        let base_dir_clone = base_dir.clone();

        // Create initial symlink: audit.log -> audit.log.YYYYMMDD
        let symlink_path = base_dir.join("audit.log");
        #[cfg(unix)]
        {
            if let Some(target) = current_log_path.file_name() {
                // Remove old symlink if exists
                let _ = std::fs::remove_file(&symlink_path);
                if let Err(e) = std::os::unix::fs::symlink(target, &symlink_path) {
                    warn!(error = %e, "Failed to create initial audit.log symlink");
                }
            }
        }

        let logger = Self {
            sender,
            current_log_path: current_log_path.clone(),
            base_dir,
        };

        // Spawn background task
        tokio::spawn(Self::background_writer(receiver, current_log_path));

        info!(base_dir = %base_dir_clone.display(), "Audit logger initialized");
        Ok(logger)
    }

    /// Get log file path for a given date.
    fn log_path_for_date(base_dir: &Path, time: SystemTime) -> Result<PathBuf> {
        // Format: audit.log.YYYYMMDD
        let datetime = chrono::DateTime::<chrono::Utc>::from(time);
        let date_str = datetime.format("audit.log.%Y%m%d").to_string();
        Ok(base_dir.join(date_str))
    }

    /// Background writer task: batches records and flushes periodically.
    async fn background_writer(mut receiver: mpsc::UnboundedReceiver<AuditRecord>, mut current_log_path: PathBuf) {
        let mut buffer = Vec::new();
        let mut last_flush = Instant::now();
        let flush_interval = Duration::from_millis(100); // 100ms
        let batch_size = 1000; // 1000 records
        let mut flush_timer = interval(flush_interval);

        loop {
            tokio::select! {
                // Receive record
                record = receiver.recv() => {
                    match record {
                        Some(record) => {
                            buffer.push(record);

                            // Flush if buffer is full
                            if buffer.len() >= batch_size {
                                if let Err(e) = Self::flush_buffer(&mut buffer, &mut current_log_path).await {
                                    error!(error = %e, "Failed to flush audit buffer");
                                }
                                last_flush = Instant::now();
                            }
                        }
                        None => {
                            // Channel closed, flush remaining and exit
                            if !buffer.is_empty() {
                                let _ = Self::flush_buffer(&mut buffer, &mut current_log_path).await;
                            }
                            break;
                        }
                    }
                }
                // Periodic flush
                _ = flush_timer.tick() => {
                    if !buffer.is_empty() && last_flush.elapsed() >= flush_interval {
                        if let Err(e) = Self::flush_buffer(&mut buffer, &mut current_log_path).await {
                            error!(error = %e, "Failed to flush audit buffer");
                        }
                        last_flush = Instant::now();
                    }
                }
            }
        }
    }

    /// Flush buffer to log file (with rotation check).
    async fn flush_buffer(buffer: &mut Vec<AuditRecord>, current_log_path: &mut PathBuf) -> Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }

        // Check if we need to rotate (new day)
        let now = SystemTime::now();
        let new_log_path = Self::log_path_for_date(current_log_path.parent().unwrap(), now)?;

        if new_log_path != *current_log_path {
            // Rotate: close old file, open new file
            *current_log_path = new_log_path.clone();

            // Update symlink: audit.log -> audit.log.YYYYMMDD
            if let Some(base_dir) = new_log_path.parent() {
                let symlink_path = base_dir.join("audit.log");
                // Remove old symlink if exists
                let _ = std::fs::remove_file(&symlink_path);
                // Create new symlink (Unix only, Windows would need different approach)
                #[cfg(unix)]
                {
                    if let Some(target) = new_log_path.file_name()
                        && let Err(e) = std::os::unix::fs::symlink(target, &symlink_path)
                    {
                        warn!(error = %e, "Failed to create audit.log symlink");
                    }
                }
            }
        }

        // Append records to file
        let mut file: tokio::fs::File = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current_log_path)
            .await
            .context("Failed to open audit log file")?;

        for record in buffer.drain(..) {
            let json = serde_json::to_string(&record).context("Failed to serialize audit record")?;
            use tokio::io::AsyncWriteExt;
            file.write_all(json.as_bytes())
                .await
                .context("Failed to write audit record")?;
            file.write_all(b"\n").await.context("Failed to write newline")?;
        }

        file.sync_all().await.context("Failed to sync audit log file")?;

        Ok(())
    }

    /// Log an audit record (non-blocking).
    pub fn log(&self, record: AuditRecord) {
        if let Err(e) = self.sender.send(record) {
            warn!(error = %e, "Failed to send audit record (queue full?)");
        }
    }

    /// Get queue size (approximate).
    /// Note: UnboundedSender doesn't have len(), so we return 0 as a placeholder.
    /// In a real implementation, we'd track the queue size separately.
    pub fn queue_size(&self) -> usize {
        0 // TODO: Track queue size separately if needed
    }
}

impl Drop for AuditLogger {
    fn drop(&mut self) {
        // Close sender to signal background task to exit
        // UnboundedSender will be dropped automatically when it goes out of scope
        // The background task will detect the channel closure and exit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_audit_logger() {
        let temp_dir = TempDir::new().unwrap();
        let logger = AuditLogger::new(temp_dir.path()).unwrap();

        let record = AuditRecord {
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            call_id: "test-call-id".to_string(),
            client_id: 12345,
            operation: "OpenReadStream".to_string(),
            path: Some("/test/path".to_string()),
            block_id: Some("1:0".to_string()),
            chunk_ref: None,
            source: "WorkerDataService".to_string(),
            result: "Success".to_string(),
            bytes: 1048576,
            latency_ms: 10.5,
        };

        logger.log(record);

        // Wait a bit for background writer to flush
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check that log file was created
        let log_files: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.file_name().unwrap().to_str().unwrap().starts_with("audit.log"))
            .collect();

        assert!(!log_files.is_empty());
    }
}
