// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Write planner.

/// Splits sequential writes into block-local worker writes.
#[derive(Clone, Debug, Default)]
pub struct WritePlanner;
