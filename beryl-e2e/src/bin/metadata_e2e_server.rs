// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Standalone Metadata process used by crash-durability integration tests.

use std::sync::Arc;

use beryl_metadata::runtime::{init_observability, DynError, MetadataServer};
use beryl_metadata::MetadataConfig;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let config_path = std::env::args()
        .nth(1)
        .ok_or("metadata-e2e-server requires a config path")?;
    let config = Arc::new(MetadataConfig::load(config_path)?);
    let _observability = init_observability(config.as_ref())?;
    MetadataServer::build(config).await?.serve().await
}
