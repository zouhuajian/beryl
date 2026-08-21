// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Configuration system with flat dotted-key support.

mod files;
mod flat;
mod host;
pub mod keys;

pub use files::load_from_yaml_file;
pub use flat::FlatConfig;
pub use host::{format_host_port, validate_public_host};

pub use keys::logging;
