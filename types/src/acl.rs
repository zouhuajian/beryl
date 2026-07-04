// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! POSIX-like ACL domain model and xattr codec helpers.
//!
//! Canonical ACL xattr keys:
//! - `system.posix_acl_access`
//! - `system.posix_acl_default`
//!
//! Canonical binary encoding (`version = 1`):
//! - `version` (u32 LE)
//! - `entry_count` (u32 LE)
//! - repeated `entry_count` times:
//!   - `subject_tag` (u8): 1=user, 2=group, 3=other, 4=mask
//!   - `subject_id` (u32 LE): present only for user/group subjects
//!   - `perms` (u8 bitmask): bit0=read, bit1=write, bit2=execute
//!
//! Notes:
//! - POSIX ACL models subject-level `rwx` permissions (`User/Group/Other/Mask`).
//! - Mapping ACL `rwx` permissions to authz operation primitives is not active runtime behavior.
//! - This module defines storage/domain representation only; it does not validate or deny runtime operations.

use serde::{Deserialize, Serialize};
use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign};

/// Canonical xattr key for access ACL.
pub const POSIX_ACL_ACCESS_XATTR: &str = "system.posix_acl_access";
/// Canonical xattr key for default ACL.
pub const POSIX_ACL_DEFAULT_XATTR: &str = "system.posix_acl_default";

const ACL_ENCODING_VERSION: u32 = 1;
const SUBJECT_TAG_USER: u8 = 1;
const SUBJECT_TAG_GROUP: u8 = 2;
const SUBJECT_TAG_OTHER: u8 = 3;
const SUBJECT_TAG_MASK: u8 = 4;

/// Returns true when `key` is one of the canonical ACL xattr keys.
pub fn is_acl_xattr_key(key: &str) -> bool {
    matches!(key, POSIX_ACL_ACCESS_XATTR | POSIX_ACL_DEFAULT_XATTR)
}

/// ACL entry subject.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AclSubject {
    /// User by uid.
    User(u32),
    /// Group by gid.
    Group(u32),
    /// "Other" (world).
    Other,
    /// Effective permission mask.
    Mask,
}

/// ACL permission bitmask.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AclPerm(u8);

impl AclPerm {
    /// Read permission bit.
    pub const READ: Self = Self(0b001);
    /// Write permission bit.
    pub const WRITE: Self = Self(0b010);
    /// Execute permission bit.
    pub const EXECUTE: Self = Self(0b100);

    const VALID_BITS: u8 = Self::READ.0 | Self::WRITE.0 | Self::EXECUTE.0;

    /// Empty permission set.
    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Returns raw permission bits.
    #[inline]
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Returns true when `self` contains all bits in `other`.
    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Creates permissions from raw bits, rejecting unknown bits.
    #[inline]
    pub const fn from_bits(bits: u8) -> Option<Self> {
        if bits & !Self::VALID_BITS == 0 {
            Some(Self(bits))
        } else {
            None
        }
    }

    #[inline]
    const fn from_bits_truncate(bits: u8) -> Self {
        Self(bits & Self::VALID_BITS)
    }
}

impl Default for AclPerm {
    fn default() -> Self {
        Self::empty()
    }
}

impl BitOr for AclPerm {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self::from_bits_truncate(self.0 | rhs.0)
    }
}

impl BitOrAssign for AclPerm {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = *self | rhs;
    }
}

impl BitAnd for AclPerm {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self::from_bits_truncate(self.0 & rhs.0)
    }
}

impl BitAndAssign for AclPerm {
    fn bitand_assign(&mut self, rhs: Self) {
        *self = *self & rhs;
    }
}

/// Single ACL entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclEntry {
    /// Subject to which permissions apply.
    pub subject: AclSubject,
    /// Permission bitmask.
    pub perms: AclPerm,
}

/// POSIX-like ACL payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PosixAcl {
    /// Encoding version. Current fixed value is `1`.
    pub version: u32,
    /// ACL entries.
    pub entries: Vec<AclEntry>,
}

impl PosixAcl {
    /// Current canonical ACL encoding version.
    pub const VERSION: u32 = ACL_ENCODING_VERSION;

    /// Creates a new ACL payload at the canonical version.
    pub fn new(entries: Vec<AclEntry>) -> Self {
        Self {
            version: Self::VERSION,
            entries,
        }
    }
}

impl Default for PosixAcl {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

/// Directory default ACL has the same structure as access ACL.
pub type PosixDefaultAcl = PosixAcl;

/// ACL codec parse error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AclCodecError {
    #[error("acl blob truncated at offset {offset}")]
    Truncated { offset: usize },
    #[error("unsupported acl version {version}, expected {expected}")]
    UnsupportedVersion { version: u32, expected: u32 },
    #[error("acl entry {entry_index} has invalid subject tag {tag}")]
    InvalidSubjectTag { entry_index: u32, tag: u8 },
    #[error("acl entry {entry_index} has invalid perms bitmask {bits:#04x}")]
    InvalidPerms { entry_index: u32, bits: u8 },
    #[error("acl blob has trailing bytes: {remaining}")]
    TrailingBytes { remaining: usize },
}

/// Encodes a POSIX ACL payload into canonical versioned bytes.
pub fn encode_posix_acl(acl: &PosixAcl) -> Vec<u8> {
    let entry_count = u32::try_from(acl.entries.len()).expect("acl entry count exceeds u32::MAX");
    let mut out = Vec::with_capacity(8 + acl.entries.len() * 8);
    out.extend_from_slice(&acl.version.to_le_bytes());
    out.extend_from_slice(&entry_count.to_le_bytes());
    for entry in &acl.entries {
        match entry.subject {
            AclSubject::User(uid) => {
                out.push(SUBJECT_TAG_USER);
                out.extend_from_slice(&uid.to_le_bytes());
            }
            AclSubject::Group(gid) => {
                out.push(SUBJECT_TAG_GROUP);
                out.extend_from_slice(&gid.to_le_bytes());
            }
            AclSubject::Other => out.push(SUBJECT_TAG_OTHER),
            AclSubject::Mask => out.push(SUBJECT_TAG_MASK),
        }
        out.push(entry.perms.bits());
    }
    out
}

/// Decodes canonical versioned ACL bytes.
pub fn decode_posix_acl(bytes: &[u8]) -> Result<PosixAcl, AclCodecError> {
    let mut offset = 0usize;
    let version = read_u32_le(bytes, &mut offset)?;
    if version != ACL_ENCODING_VERSION {
        return Err(AclCodecError::UnsupportedVersion {
            version,
            expected: ACL_ENCODING_VERSION,
        });
    }

    let entry_count = read_u32_le(bytes, &mut offset)?;
    let mut entries = Vec::with_capacity(entry_count as usize);
    for entry_index in 0..entry_count {
        let subject_tag = read_u8(bytes, &mut offset)?;
        let subject = match subject_tag {
            SUBJECT_TAG_USER => AclSubject::User(read_u32_le(bytes, &mut offset)?),
            SUBJECT_TAG_GROUP => AclSubject::Group(read_u32_le(bytes, &mut offset)?),
            SUBJECT_TAG_OTHER => AclSubject::Other,
            SUBJECT_TAG_MASK => AclSubject::Mask,
            tag => return Err(AclCodecError::InvalidSubjectTag { entry_index, tag }),
        };
        let perms_bits = read_u8(bytes, &mut offset)?;
        let perms = AclPerm::from_bits(perms_bits).ok_or(AclCodecError::InvalidPerms {
            entry_index,
            bits: perms_bits,
        })?;
        entries.push(AclEntry { subject, perms });
    }

    if offset != bytes.len() {
        return Err(AclCodecError::TrailingBytes {
            remaining: bytes.len() - offset,
        });
    }

    Ok(PosixAcl { version, entries })
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> Result<u8, AclCodecError> {
    if *offset >= bytes.len() {
        return Err(AclCodecError::Truncated { offset: *offset });
    }
    let value = bytes[*offset];
    *offset += 1;
    Ok(value)
}

fn read_u32_le(bytes: &[u8], offset: &mut usize) -> Result<u32, AclCodecError> {
    if bytes.len().saturating_sub(*offset) < 4 {
        return Err(AclCodecError::Truncated { offset: *offset });
    }
    let mut raw = [0u8; 4];
    raw.copy_from_slice(&bytes[*offset..*offset + 4]);
    *offset += 4;
    Ok(u32::from_le_bytes(raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acl_key_helper_matches_canonical_keys() {
        assert!(is_acl_xattr_key(POSIX_ACL_ACCESS_XATTR));
        assert!(is_acl_xattr_key(POSIX_ACL_DEFAULT_XATTR));
        assert!(!is_acl_xattr_key("user.custom"));
    }

    #[test]
    fn encode_decode_roundtrip_preserves_acl() {
        let acl = PosixAcl::new(vec![
            AclEntry {
                subject: AclSubject::User(1000),
                perms: AclPerm::READ | AclPerm::WRITE,
            },
            AclEntry {
                subject: AclSubject::Group(2000),
                perms: AclPerm::READ,
            },
            AclEntry {
                subject: AclSubject::Other,
                perms: AclPerm::READ,
            },
            AclEntry {
                subject: AclSubject::Mask,
                perms: AclPerm::READ | AclPerm::EXECUTE,
            },
        ]);

        let encoded = encode_posix_acl(&acl);
        let decoded = decode_posix_acl(&encoded).expect("decode must succeed");
        assert_eq!(decoded, acl);
    }
}
