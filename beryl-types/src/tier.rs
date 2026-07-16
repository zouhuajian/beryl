// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared local storage tier value.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Tier {
    Mem,
    Nvme,
    Ssd,
    Hdd,
}

/// Writable free bytes advertised for one worker-local storage tier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TierFree {
    pub tier: Tier,
    pub free_bytes: u64,
}

impl Tier {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mem => "MEM",
            Self::Nvme => "NVME",
            Self::Ssd => "SSD",
            Self::Hdd => "HDD",
        }
    }

    pub fn parse(value: impl AsRef<str>) -> Result<Self, TierError> {
        value.as_ref().parse()
    }
}

impl fmt::Display for Tier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Tier {
    type Err = TierError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "MEM" => Ok(Self::Mem),
            "NVME" => Ok(Self::Nvme),
            "SSD" => Ok(Self::Ssd),
            "HDD" => Ok(Self::Hdd),
            other => Err(TierError {
                value: other.to_string(),
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TierError {
    value: String,
}

impl fmt::Display for TierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unsupported tier {:?}; expected one of MEM, NVME, SSD, HDD",
            self.value
        )
    }
}

impl std::error::Error for TierError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_parse_accepts_supported_uppercase_values() {
        assert_eq!(Tier::parse("MEM").unwrap(), Tier::Mem);
        assert_eq!(Tier::parse("NVME").unwrap(), Tier::Nvme);
        assert_eq!(Tier::parse("SSD").unwrap(), Tier::Ssd);
        assert_eq!(Tier::parse("HDD").unwrap(), Tier::Hdd);
    }

    #[test]
    fn tier_parse_rejects_unknown_or_lowercase_values() {
        assert!(Tier::parse("TAPE").is_err());
        assert!(Tier::parse("hdd").is_err());
    }
}
