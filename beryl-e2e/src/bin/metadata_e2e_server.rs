// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Standalone Metadata process used by crash-durability integration tests.

use std::sync::Arc;

use beryl_common::termination::TerminationSignal;
use beryl_metadata::runtime::{init_observability, DynError, MetadataServer};
use beryl_metadata::MetadataConfig;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let config_path = std::env::args()
        .nth(1)
        .ok_or("metadata-e2e-server requires a config path")?;
    let mut termination = TerminationSignal::install()?.monitor();
    let config = match MetadataConfig::load(config_path) {
        Ok(config) => Arc::new(config),
        Err(error) => {
            termination.shutdown().await?;
            return Err(Box::new(error) as DynError);
        }
    };
    if termination.is_cancelled() {
        termination.recv().await?;
        return Ok(());
    }
    let observability = match init_observability(config.as_ref()) {
        Ok(observability) => observability,
        Err(error) => {
            termination.shutdown().await?;
            return Err(error);
        }
    };
    let server = match MetadataServer::build(config, termination.cancellation_token()).await {
        Ok(Some(server)) => server,
        Ok(None) => {
            termination.recv().await?;
            return Ok(());
        }
        Err(error) => {
            termination.shutdown().await?;
            return Err(error);
        }
    };
    let result = server.serve(observability, &mut termination).await;
    termination.shutdown().await?;
    result
}
