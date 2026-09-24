// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Minimal Beryl Rust Client CRUD roundtrip.

use std::error::Error;
use std::io;
use std::path::PathBuf;

use beryl_client::{ClientConfig, FsClient};
use bytes::Bytes;
use futures::io::AsyncReadExt;

const DIRECTORY: &str = "/examples";
const FILE: &str = "/examples/rust-client-crud.bin";
const PAYLOAD_SIZE: usize = 2065;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("conf/client.yaml"));
    let client = FsClient::new(ClientConfig::load(config_path)?);

    client.mkdirs(DIRECTORY).await?;

    let payload = Bytes::from(
        (0..PAYLOAD_SIZE)
            .map(|index| ((index * 31 + 17) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let mut writer = client.create(FILE).await?;
    writer.write_all(&payload).await?;
    writer.close().await?;

    let status = client.get_status(FILE).await?;
    if status.len() != payload.len() as u64 {
        return Err(io::Error::other(format!(
            "stat size mismatch: expected {}, got {}",
            payload.len(),
            status.len()
        ))
        .into());
    }

    let mut reader = client.open(FILE).await?;
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).await?;
    if actual != payload {
        return Err(io::Error::other("read content mismatch").into());
    }

    client.delete(FILE).await?;
    println!("Rust Client CRUD roundtrip succeeded: {FILE}");
    Ok(())
}
