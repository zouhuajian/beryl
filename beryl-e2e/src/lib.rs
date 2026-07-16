// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

pub mod cluster;
pub mod data;
pub mod ports;
pub mod readiness;
pub mod services;
pub mod temp_state;

pub use cluster::TestCluster;

pub type TestError = Box<dyn std::error::Error + Send + Sync>;
pub type TestResult<T> = Result<T, TestError>;
