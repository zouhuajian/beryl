// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker control-plane startup registration.

mod heartbeat;
mod identity;
mod registrar;
mod registration;

pub use heartbeat::{HeartbeatError, HeartbeatRound, HeartbeatSnapshot, MetadataHeartbeatLoop};
pub use identity::resolve_worker_id;
pub use registrar::{MetadataRegistrar, RegistrationDescriptor, RegistrationError};
pub use registration::{Registration, RegistrationSet};
