// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_proto::metadata::{DeleteOptionsProto, DeleteRequestProto};
use prost::Message;

#[test]
fn delete_options_round_trip_on_the_new_wire_field() {
    let request = DeleteRequestProto {
        header: None,
        path: "/data/file".to_string(),
        options: Some(DeleteOptionsProto { recursive: true }),
    };

    let decoded = DeleteRequestProto::decode(request.encode_to_vec().as_slice()).unwrap();

    assert_eq!(decoded.path, "/data/file");
    let options = decoded.options.expect("delete options");
    assert!(options.recursive);
}

#[test]
fn delete_options_are_encoded_on_field_three() {
    let request = DeleteRequestProto {
        header: None,
        path: String::new(),
        options: Some(DeleteOptionsProto { recursive: true }),
    };

    let encoded = request.encode_to_vec();

    assert_eq!(encoded, [0x1a, 0x02, 0x08, 0x01]);
}
