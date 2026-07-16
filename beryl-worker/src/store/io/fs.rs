// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! File system I/O engine implementation.

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;

use super::{IoError, IoResult, LocalIoEngine};

/// File system I/O engine using blocking file APIs on Tokio's blocking pool.
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
        .map_err(|err| IoError::Io(std::io::Error::other(format!("spawn_blocking failed: {err}"))))?
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

                while total_read < len {
                    match file.read_at(&mut buf[total_read..], offset + total_read as u64) {
                        Ok(0) => return Err(IoError::UnexpectedEof),
                        Ok(n) => {
                            total_read += n;
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(err) => return Err(IoError::Io(err)),
                    }
                }

                Ok(Bytes::from(buf))
            }

            #[cfg(not(target_family = "unix"))]
            {
                let _ = (path, offset, len);
                Err(IoError::NotSupported(
                    "read_range is only supported on Unix platforms".to_string(),
                ))
            }
        })
        .await
        .map_err(|err| IoError::Io(std::io::Error::other(format!("spawn_blocking failed: {err}"))))?
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
        .map_err(|err| IoError::Io(std::io::Error::other(format!("spawn_blocking failed: {err}"))))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn write_all_and_read_range() {
        let engine = FsIoEngine::new();
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let data = Bytes::from_static(b"Hello, World!");
        engine.write_all(path, data.clone()).await.unwrap();

        assert_eq!(engine.read_range(path, 0, data.len()).await.unwrap(), data);
        assert_eq!(
            engine.read_range(path, 0, 5).await.unwrap(),
            Bytes::from_static(b"Hello")
        );
        assert_eq!(
            engine.read_range(path, 7, 5).await.unwrap(),
            Bytes::from_static(b"World")
        );
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn read_range_reports_unexpected_eof() {
        let engine = FsIoEngine::new();
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        engine.write_all(path, Bytes::from_static(b"Hi")).await.unwrap();

        let result = engine.read_range(path, 0, 100).await;
        assert!(matches!(result, Err(IoError::UnexpectedEof)));
    }
}
