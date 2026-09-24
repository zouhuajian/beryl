// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_common::{CallerContext, CallerContextFields};

#[test]
fn caller_context_fields_parse_locality_hints_and_ignore_invalid_entries() {
    let context = CallerContext {
        context: "ip=10.0.0.1,host=worker-a".to_string(),
    };
    let from_context = CallerContextFields::from_caller_context(&context);
    assert_eq!(from_context.ip(), Some("10.0.0.1"));
    assert_eq!(from_context.host(), Some("worker-a"));

    let cases = [
        ("", [None, None]),
        (
            "host=first,unknown=value,malformed,host=second, =empty-key,ip=",
            [None, Some("first")],
        ),
        (
            " ip = 10.0.0.2 , host = worker-b ",
            [Some("10.0.0.2"), Some("worker-b")],
        ),
    ];

    for (raw, [ip, host]) in cases {
        let fields = CallerContextFields::parse(raw);
        assert_eq!(fields.ip(), ip, "ip mismatch for {raw}");
        assert_eq!(fields.host(), host, "host mismatch for {raw}");
    }
}
