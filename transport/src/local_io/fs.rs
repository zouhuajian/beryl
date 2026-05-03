// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! File system I/O engine implementation.

use crate::error::{IoError, IoResult};
use crate::local_io::LocalIoEngine;
use async_trait::async_trait;
use bytes::Bytes;
use std::path::Path;

/// File system I/O engine using standard file system operations.
///
/// This is the default implementation that uses tokio's file I/O
/// with blocking operations in a thread pool.
pub struct FsIoEngine;

impl FsIoEngine {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FsIoEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LocalIoEngine for FsIoEngine {
    async fn write_all(&self, path: &Path, data: Bytes) -> IoResult<()> {
        let path = path.to_path_buf();
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || {
            use std::fs::OpenOptions;
            use std::io::Write;

            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .map_err(IoError::Io)?;

            file.write_all(&data).map_err(IoError::Io)?;

            // Optional: sync data to disk
            #[cfg(target_family = "unix")]
            {
                file.sync_data().map_err(IoError::Io)?;
            }

            #[cfg(not(target_family = "unix"))]
            {
                file.sync_all().map_err(IoError::Io)?;
            }

            Ok(())
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(format!("spawn_blocking failed: {}", e))))?
    }

    async fn read_range(&self, path: &Path, offset: u64, len: usize) -> IoResult<Bytes> {
        let path = path.to_path_buf();

        tokio::task::spawn_blocking(move || {
            #[cfg(target_family = "unix")]
            {
                use std::fs::File;
                use std::os::unix::fs::FileExt;

                let file = File::open(&path).map_err(IoError::Io)?;

                let mut buf = vec![0u8; len];
                let mut total_read = 0;

                // Handle short reads: loop until we read len bytes or hit EOF
                while total_read < len {
                    match file.read_at(&mut buf[total_read..], offset + total_read as u64) {
                        Ok(0) => {
                            // EOF reached before reading len bytes
                            // If we haven't read anything, return UnexpectedEof
                            // If we've read some data but not enough, also return UnexpectedEof
                            // (per requirement: EOF should return UnexpectedEof)
                            return Err(IoError::UnexpectedEof);
                        }
                        Ok(n) => {
                            total_read += n;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                            // Retry on interrupt
                            continue;
                        }
                        Err(e) => {
                            return Err(IoError::Io(e));
                        }
                    }
                }

                Ok(Bytes::from(buf))
            }

            #[cfg(not(target_family = "unix"))]
            {
                Err(IoError::NotSupported(
                    "read_range is only supported on Unix platforms".to_string(),
                ))
            }
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(format!("spawn_blocking failed: {}", e))))?
    }

    async fn sync(&self, path: &Path) -> IoResult<()> {
        let path = path.to_path_buf();

        tokio::task::spawn_blocking(move || {
            use std::fs::File;

            let file = File::open(&path).map_err(IoError::Io)?;

            #[cfg(target_family = "unix")]
            {
                file.sync_data().map_err(IoError::Io)?;
            }

            #[cfg(not(target_family = "unix"))]
            {
                file.sync_all().map_err(IoError::Io)?;
            }

            Ok(())
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(format!("spawn_blocking failed: {}", e))))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn test_write_all_and_read_range() {
        let engine = FsIoEngine::new();
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Write test data
        let test_data = Bytes::from(b"Hello, World!".as_slice());
        engine.write_all(path, test_data.clone()).await.unwrap();

        // Read the entire range
        let read_data = engine.read_range(path, 0, test_data.len()).await.unwrap();
        assert_eq!(read_data, test_data);

        // Read a partial range
        let partial = engine.read_range(path, 0, 5).await.unwrap();
        assert_eq!(partial, Bytes::from(b"Hello".as_slice()));

        // Read from offset
        let offset_data = engine.read_range(path, 7, 5).await.unwrap();
        assert_eq!(offset_data, Bytes::from(b"World".as_slice()));
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn test_read_range_eof() {
        let engine = FsIoEngine::new();
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Write small data
        let test_data = Bytes::from(b"Hi".as_slice());
        engine.write_all(path, test_data).await.unwrap();

        // Try to read more than available - should return UnexpectedEof
        let result = engine.read_range(path, 0, 100).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            IoError::UnexpectedEof => {}
            _ => panic!("Expected UnexpectedEof"),
        }
    }
}
