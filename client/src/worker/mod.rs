// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker service client.

pub mod client;

#[cfg(test)]
mod tests;

pub use client::WorkerClient;
