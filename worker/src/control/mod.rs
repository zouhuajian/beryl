// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker control-plane startup registration.

mod identity;
mod registrar;
mod registration;

pub use identity::resolve_worker_id;
pub use registrar::{MetadataRegistrar, RegistrationDescriptor, RegistrationError};
pub use registration::{Registration, RegistrationSet};
