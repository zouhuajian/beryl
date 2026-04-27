// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service main entry point.

use metadata::runtime::{
    build_authority, build_filesystem_service, build_maintenance, build_readiness, build_worker_background,
    build_worker_manager, build_worker_service, compose_services, init_observability, load_config, serve, DynError,
};

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let config = load_config()?;
    let _observability = init_observability(config.as_ref())?;
    let authority = build_authority(config.as_ref()).await?;
    let worker_manager = build_worker_manager();
    let readiness = build_readiness(config.as_ref(), &authority).await;
    let filesystem = build_filesystem_service(config.as_ref(), &authority, worker_manager.clone(), &readiness).await?;
    let mut worker = build_worker_service(config.as_ref(), &authority, worker_manager.clone());
    let maintenance = build_maintenance(&authority, worker_manager, &worker).await;
    let worker_background = build_worker_background(&mut worker, &maintenance);
    let (services, handles) = compose_services(filesystem, worker, readiness, worker_background, maintenance);

    serve(config.as_ref(), services, handles).await
}
