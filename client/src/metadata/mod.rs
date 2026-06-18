// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client-owned metadata control-plane boundary.
//!
//! [`MetadataGateway`] is the metadata RPC boundary used by the public
//! [`crate::FsClient`] facade. It builds request headers from runtime attempt
//! context and preserves structured refresh hints for executor replay
//! decisions. Worker data reads stay behind the internal data boundary.

pub(crate) mod gateway;
pub(crate) mod model;

pub(crate) use gateway::{MetadataGateway, TonicMetadataGateway};
pub(crate) use model::{AddBlockResult, ReadLayout};
