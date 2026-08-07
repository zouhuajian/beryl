// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared configuration key constants.
//!
//! `common` owns generic config loading plus shared primitives such as
//! observability. Module-specific keys, defaults, and validation belong to the
//! owning module's typed config.

/// Process logging configuration keys.
pub mod logging {
    /// EnvFilter directive string.
    pub const LEVEL: &str = "beryl.logging.level";
    /// Log format: "compact" or "json".
    pub const FORMAT: &str = "beryl.logging.format";
    /// Log output stream: "stderr" or "stdout".
    pub const OUTPUT: &str = "beryl.logging.output";
}
