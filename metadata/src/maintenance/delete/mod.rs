// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Delete intent creation and execution owned by maintenance.

mod executor;
mod intent;

#[cfg(test)]
mod executor_tests;

pub(crate) use executor::{DeleteExecutor, DeleteExecutorHandle};
pub(crate) use intent::DeleteIntentBuilder;
