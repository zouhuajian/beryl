// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! ReadMode and WriteMode definitions.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Read mode hint for client operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReadMode {
    /// Direct read from worker (skip metadata if cache valid).
    Direct,

    /// Use cached metadata (prefer cache over fresh metadata).
    Cached,
}

impl fmt::Display for ReadMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadMode::Direct => write!(f, "direct"),
            ReadMode::Cached => write!(f, "cached"),
        }
    }
}

impl FromStr for ReadMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "direct" => Ok(ReadMode::Direct),
            "cached" => Ok(ReadMode::Cached),
            _ => Err(format!("Invalid read mode: {}", s)),
        }
    }
}

/// Convert to proto ReadModeProto.
impl From<ReadMode> for proto::common::ReadModeProto {
    fn from(mode: ReadMode) -> Self {
        match mode {
            ReadMode::Direct => proto::common::ReadModeProto::ReadModeDirect,
            ReadMode::Cached => proto::common::ReadModeProto::ReadModeCached,
        }
    }
}

/// Convert from proto ReadModeProto.
impl From<proto::common::ReadModeProto> for ReadMode {
    fn from(mode: proto::common::ReadModeProto) -> Self {
        match mode {
            proto::common::ReadModeProto::ReadModeDirect => ReadMode::Direct,
            proto::common::ReadModeProto::ReadModeCached => ReadMode::Cached,
            _ => ReadMode::Cached, // Default
        }
    }
}

/// Write mode for client operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WriteMode {
    /// Write-through to UFS (synchronous).
    Through,

    /// Write-back (cache, asynchronous flush).
    Back,

    /// Direct to UFS (bypass cache).
    Direct,
}

impl fmt::Display for WriteMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteMode::Through => write!(f, "through"),
            WriteMode::Back => write!(f, "back"),
            WriteMode::Direct => write!(f, "direct"),
        }
    }
}

impl FromStr for WriteMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "through" => Ok(WriteMode::Through),
            "back" => Ok(WriteMode::Back),
            "direct" => Ok(WriteMode::Direct),
            _ => Err(format!("Invalid write mode: {}", s)),
        }
    }
}

/// Convert to proto WriteModeProto.
impl From<WriteMode> for proto::common::WriteModeProto {
    fn from(mode: WriteMode) -> Self {
        match mode {
            WriteMode::Through => proto::common::WriteModeProto::WriteModeThrough,
            WriteMode::Back => proto::common::WriteModeProto::WriteModeBack,
            WriteMode::Direct => proto::common::WriteModeProto::WriteModeDirect,
        }
    }
}

/// Convert from proto WriteModeProto.
impl From<proto::common::WriteModeProto> for WriteMode {
    fn from(mode: proto::common::WriteModeProto) -> Self {
        match mode {
            proto::common::WriteModeProto::WriteModeThrough => WriteMode::Through,
            proto::common::WriteModeProto::WriteModeBack => WriteMode::Back,
            proto::common::WriteModeProto::WriteModeDirect => WriteMode::Direct,
            _ => WriteMode::Back, // Default
        }
    }
}
