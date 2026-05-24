// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public filesystem operation options.

/// Options for opening an existing file for reads.
///
/// This type is intentionally empty until the client supports stable read-open
/// options. It exists to keep the public entrypoint explicit without implying
/// write, create, append, or truncate behavior.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpenOptions {}

/// Options for creating a file write session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CreateOptions {
    /// Creation behavior for the target path.
    pub disposition: CreateDisposition,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self::create()
    }
}

impl CreateOptions {
    /// Return options that create a new file and fail if it already exists.
    pub fn create() -> Self {
        Self {
            disposition: CreateDisposition::Create,
        }
    }

    /// Return options that replace the file contents or create it if absent.
    pub fn overwrite() -> Self {
        Self {
            disposition: CreateDisposition::Overwrite,
        }
    }
}

/// Options for opening an append write session.
///
/// This type is intentionally empty until append has stable public options.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AppendOptions {}

/// Options for listing a directory through [`FsClient::list`](crate::FsClient::list).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListOptions {
    /// Whether the listing should recursively include descendants.
    pub recursive: bool,

    /// Opaque cursor returned by a previous listing page.
    pub cursor: Option<Vec<u8>>,

    /// Maximum number of entries to return. `None` lets metadata choose.
    pub limit: Option<u32>,
}

/// Creation behavior for [`CreateOptions`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CreateDisposition {
    /// Create a new file and fail if the path already exists.
    #[default]
    Create,
    /// Replace the existing file contents or create the file if it does not exist.
    Overwrite,
}
