// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_common::{CallerContext, CallerContextFields};

#[test]
fn caller_context_fields_parse_locality_hints_and_ignore_invalid_entries() {
    let context = CallerContext {
        context: "ip=10.0.0.1,host=worker-a,az=az-a,rack=rack-1,region=us-west".to_string(),
    };
    let from_context = CallerContextFields::from_caller_context(&context);
    assert_eq!(from_context.ip(), Some("10.0.0.1"));
    assert_eq!(from_context.host(), Some("worker-a"));
    assert_eq!(from_context.az(), Some("az-a"));
    assert_eq!(from_context.rack(), Some("rack-1"));
    assert_eq!(from_context.region(), Some("us-west"));

    let cases = [
        ("", [None, None, None, None, None]),
        (
            "host=first,unknown=value,malformed,host=second,rack = rack-a, =empty-key,az",
            [None, Some("first"), None, Some("rack-a"), None],
        ),
        (
            " ip = 10.0.0.2 , az = az-b , region = us-east ",
            [Some("10.0.0.2"), None, Some("az-b"), None, Some("us-east")],
        ),
    ];

    for (raw, [ip, host, az, rack, region]) in cases {
        let fields = CallerContextFields::parse(raw);
        assert_eq!(fields.ip(), ip, "ip mismatch for {raw}");
        assert_eq!(fields.host(), host, "host mismatch for {raw}");
        assert_eq!(fields.az(), az, "az mismatch for {raw}");
        assert_eq!(fields.rack(), rack, "rack mismatch for {raw}");
        assert_eq!(fields.region(), region, "region mismatch for {raw}");
    }
}
