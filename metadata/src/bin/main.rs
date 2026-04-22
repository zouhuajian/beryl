// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service main entry point.

#[path = "../runtime.rs"]
mod runtime;

use runtime::{
    bootstrap_authority, bootstrap_core, build_background_runtime, build_filesystem_runtime, build_worker_runtime,
    serve, DynError,
};

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let core = bootstrap_core()?;
    let authority = bootstrap_authority(core.config.as_ref()).await?;
    let mut worker = build_worker_runtime(core.config.as_ref(), &authority);
    let background = build_background_runtime(&authority, &mut worker).await;
    let filesystem = build_filesystem_runtime(core.config.as_ref(), &authority).await?;

    serve(core.config.as_ref(), filesystem, worker, background).await
}
