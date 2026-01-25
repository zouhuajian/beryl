// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Zero-copy buffer abstraction.
//!
//! This module provides a unified buffer interface that allows different transport
//! implementations to use zero-copy operations without being tied to specific
//! buffer types (like protobuf Bytes).

use bytes::Bytes;
use std::ops::Deref;

/// A zero-copy buffer that can be shared across threads and efficiently
/// passed between transport layers.
///
/// The goal is to avoid unnecessary copies when passing data through
/// the transport stack. Different implementations can use:
/// - `Bytes` (reference-counted, zero-copy slices)
/// - `io_uring` buffers (registered buffers)
/// - `spdk` buffers (DMA buffers)
/// - Custom zero-copy implementations
pub trait Buffer: Send + Sync + Clone {
    /// Create a buffer from a byte slice (may copy).
    fn from_slice(data: &[u8]) -> Self;

    /// Create an empty buffer.
    fn empty() -> Self;

    /// Get the length of the buffer.
    fn len(&self) -> usize;

    /// Check if the buffer is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Slice the buffer (zero-copy if possible).
    fn slice(&self, range: std::ops::Range<usize>) -> Self;

    /// Convert to a contiguous byte slice (may require copy).
    fn as_bytes(&self) -> &[u8];

    /// Take ownership of the buffer data (may copy).
    fn to_vec(&self) -> Vec<u8>;
}

/// Default implementation using `Bytes` (reference-counted zero-copy).
impl Buffer for Bytes {
    fn from_slice(data: &[u8]) -> Self {
        Bytes::copy_from_slice(data)
    }

    fn empty() -> Self {
        Bytes::new()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn slice(&self, range: std::ops::Range<usize>) -> Self {
        self.slice(range)
    }

    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }

    fn to_vec(&self) -> Vec<u8> {
        self.as_ref().to_vec()
    }
}

/// A buffer that can be used for zero-copy I/O operations.
///
/// This is a wrapper that can hold different buffer types while providing
/// a unified interface.
#[derive(Clone, Debug)]
pub struct ZeroCopyBuffer {
    inner: Bytes,
}

impl ZeroCopyBuffer {
    pub fn new(data: Bytes) -> Self {
        Self { inner: data }
    }

    pub fn from_slice(data: &[u8]) -> Self {
        Self {
            inner: Bytes::copy_from_slice(data),
        }
    }

    pub fn empty() -> Self {
        Self { inner: Bytes::new() }
    }

    pub fn into_inner(self) -> Bytes {
        self.inner
    }
}

impl Buffer for ZeroCopyBuffer {
    fn from_slice(data: &[u8]) -> Self {
        Self::from_slice(data)
    }

    fn empty() -> Self {
        Self { inner: Bytes::new() }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn slice(&self, range: std::ops::Range<usize>) -> Self {
        Self {
            inner: self.inner.slice(range),
        }
    }

    fn as_bytes(&self) -> &[u8] {
        self.inner.as_ref()
    }

    fn to_vec(&self) -> Vec<u8> {
        self.inner.to_vec()
    }
}

impl Deref for ZeroCopyBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl From<Bytes> for ZeroCopyBuffer {
    fn from(bytes: Bytes) -> Self {
        Self::new(bytes)
    }
}

impl From<ZeroCopyBuffer> for Bytes {
    fn from(buf: ZeroCopyBuffer) -> Self {
        buf.into_inner()
    }
}

impl From<Vec<u8>> for ZeroCopyBuffer {
    fn from(vec: Vec<u8>) -> Self {
        Self::new(Bytes::from(vec))
    }
}
