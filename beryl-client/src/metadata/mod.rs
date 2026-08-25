// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Client-owned metadata control-plane boundary.
//!
//! [`MetadataClient`] owns operation execution, retry, and authority state.
//! [`MetadataTransport`] owns one selected-endpoint RPC attempt, including
//! request-header construction and validated wire-response conversion.

pub(crate) mod client;
pub(crate) mod model;
pub(crate) mod transport;

pub(crate) use client::MetadataClient;
pub(crate) use model::{AddBlockResult, MetadataAuthorityUpdate, ReadLayout, ReadSnapshot, ValidatedMetadataResponse};
pub(crate) use transport::{GrpcMetadataTransport, MetadataTransport};
