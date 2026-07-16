// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

pub fn deterministic_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|idx| ((idx * 31 + 7) % 251) as u8).collect()
}
