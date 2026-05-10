// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Transport conversion utilities.
//!
//! Worker data-plane v2 removed the transport-facing chunk payload messages.
//! Future helpers here should convert stream frame payloads without reintroducing
//! storage chunk messages as network frames.
