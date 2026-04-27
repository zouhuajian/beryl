// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service main entry point.

use metadata::runtime::{init_observability, load_config, DynError, MetadataServer};

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let config = load_config()?;
    let _observability = init_observability(config.as_ref())?;
    let server = MetadataServer::build(config).await?;

    server.serve().await
}
