//! Untyped conformance checking: does this body match this schema?
//!
//! The serdes in the sibling modules decode a body into a Rust type. This
//! module answers the weaker question a broker asks, where there is no Rust
//! type for a user's record: are these bytes a well-formed instance of this
//! schema?
//!
//! Each function decodes the body into the format's own generic value and then
//! discards it. Success means the body conforms. Nothing is returned, because
//! the caller wants the verdict and not the data.

use crate::{error::SchemaSerdeError, subject::SchemaKind};

/// Check `body` against `schema` for the format `kind`.
///
/// `message_index` is Confluent's message-index path, and only Protobuf reads
/// it. The other two formats ignore it.
///
/// # Errors
///
/// Returns [`SchemaSerdeError::Schema`] when `schema` is not a valid schema of
/// that format, and [`SchemaSerdeError::Deserialize`] when `schema` is valid
/// but `body` is not an instance of it. Returns [`SchemaSerdeError::Schema`]
/// when the Cargo feature for `kind` is not enabled in this build, because a
/// caller that cannot check a format must not be told the body passed.
pub fn validate_body(
    kind: SchemaKind,
    schema: &str,
    message_index: &[i32],
    body: &[u8],
) -> Result<(), SchemaSerdeError> {
    match kind {
        #[cfg(feature = "avro")]
        SchemaKind::Avro => validate_avro(schema, body),
        #[cfg(feature = "protobuf")]
        SchemaKind::Protobuf => validate_protobuf(schema, message_index, body),
        #[cfg(feature = "json")]
        SchemaKind::Json => validate_json(schema, body),
        #[cfg(not(all(feature = "avro", feature = "protobuf", feature = "json")))]
        other => Err(SchemaSerdeError::Schema(format!(
            "{other:?} body validation is not compiled into this build"
        ))),
    }
}

/// Check that `body` is one Avro datum of `schema`, with nothing after it.
///
/// # Errors
///
/// Returns [`SchemaSerdeError::Schema`] when `schema` does not parse, and
/// [`SchemaSerdeError::Deserialize`] when the body does not decode against it
/// or leaves trailing bytes.
#[cfg(feature = "avro")]
pub fn validate_avro(schema: &str, body: &[u8]) -> Result<(), SchemaSerdeError> {
    let parsed = apache_avro::schema::Schema::parse_str(schema)
        .map_err(|e| SchemaSerdeError::Schema(e.to_string()))?;
    let mut cursor = body;
    // Reader schema `None`: decode against the writer schema alone. There is
    // no reader type here, so no resolution is wanted.
    apache_avro::from_avro_datum(&parsed, &mut cursor, None)
        .map_err(|e| SchemaSerdeError::Deserialize(format!("avro body: {e}")))?;
    // `from_avro_datum` reads one datum and stops. Bytes after it are not part
    // of any datum this schema describes, so the body is not an instance of it.
    if cursor.is_empty() {
        Ok(())
    } else {
        Err(SchemaSerdeError::Deserialize(format!(
            "avro body: {} trailing byte(s) after the datum",
            cursor.len()
        )))
    }
}

/// Check that `body` is an instance of the message `message_index` selects in
/// `schema`.
///
/// `message_index` is Confluent's path into the file's message list: `[0]` is
/// the first top-level message, and `[0, 1]` is the second message nested
/// inside it.
///
/// # How strict this is
///
/// Protobuf checks less than Avro and JSON Schema do, and the difference is a
/// property of the wire format rather than of this code. A protobuf message
/// keeps a field number it does not declare as an unknown field rather than
/// failing, so a body carrying extra fields decodes without an error.
///
/// It does reject a field the message *does* declare when the wire type is
/// not the one declared for it, and it rejects a body that is not a protobuf
/// message at all: an illegal field number, a truncated varint, a length
/// prefix that runs past the end. Those are the common failures — an unframed
/// payload, or bytes from a different serializer — and they are what the check
/// is for.
///
/// # Errors
///
/// Returns [`SchemaSerdeError::Schema`] when the `.proto` text does not parse
/// or `message_index` does not name a message in it, and
/// [`SchemaSerdeError::Deserialize`] when the body does not decode as that
/// message.
#[cfg(feature = "protobuf")]
pub fn validate_protobuf(
    schema: &str,
    message_index: &[i32],
    body: &[u8],
) -> Result<(), SchemaSerdeError> {
    use prost_reflect::{DescriptorPool, DynamicMessage, prost_types::FileDescriptorSet};

    let file = protox_parse::parse("schema.proto", schema)
        .map_err(|e| SchemaSerdeError::Schema(format!("protobuf schema: {e}")))?;
    let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
        .map_err(|e| SchemaSerdeError::Schema(format!("protobuf link: {e}")))?;

    let descriptor = message_at(&pool, message_index)?;
    DynamicMessage::decode(descriptor, body)
        .map_err(|e| SchemaSerdeError::Deserialize(format!("protobuf body: {e}")))?;
    Ok(())
}

/// Walk a Confluent message-index path to the message it names.
#[cfg(feature = "protobuf")]
fn message_at(
    pool: &prost_reflect::DescriptorPool,
    message_index: &[i32],
) -> Result<prost_reflect::MessageDescriptor, SchemaSerdeError> {
    let missing = |path: &[i32]| {
        SchemaSerdeError::Schema(format!("protobuf message-index {path:?} names no message"))
    };
    // An empty path is Confluent's implicit `[0]`.
    let path = if message_index.is_empty() {
        &[0][..]
    } else {
        message_index
    };
    let (&first, rest) = path.split_first().ok_or_else(|| missing(path))?;
    let index = usize::try_from(first).map_err(|_| missing(path))?;
    // The first index is into the file's TOP-LEVEL message list, and nesting
    // is what the later indices are for. `DescriptorPool::all_messages` walks
    // nested messages too, so it would put `Order.Line` where `Other` belongs.
    // One file went into the pool, so its messages are the ones to walk.
    let file = pool.files().next().ok_or_else(|| missing(path))?;
    let mut current = file.messages().nth(index).ok_or_else(|| missing(path))?;
    for &step in rest {
        let step = usize::try_from(step).map_err(|_| missing(path))?;
        // Bound separately: `child_messages` borrows `current`, so the new
        // value has to be built before the assignment ends that borrow.
        let child = current
            .child_messages()
            .nth(step)
            .ok_or_else(|| missing(path))?;
        current = child;
    }
    Ok(current)
}

/// Check that `body` is UTF-8 JSON valid against the JSON Schema `schema`.
///
/// # Errors
///
/// Returns [`SchemaSerdeError::Schema`] when `schema` is not a usable JSON
/// Schema, and [`SchemaSerdeError::Deserialize`] when the body is not JSON or
/// does not satisfy the schema.
#[cfg(feature = "json")]
pub fn validate_json(schema: &str, body: &[u8]) -> Result<(), SchemaSerdeError> {
    let schema_value: serde_json::Value = serde_json::from_str(schema)
        .map_err(|e| SchemaSerdeError::Schema(format!("json schema: {e}")))?;
    let validator = jsonschema::validator_for(&schema_value)
        .map_err(|e| SchemaSerdeError::Schema(format!("json schema: {e}")))?;
    let instance: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| SchemaSerdeError::Deserialize(format!("json body: {e}")))?;
    validator
        .validate(&instance)
        .map_err(|e| SchemaSerdeError::Deserialize(format!("json body: {e}")))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[cfg(feature = "avro")]
    const ORDER_AVRO: &str = r#"{"type":"record","name":"Order","fields":[
        {"name":"id","type":"string"},
        {"name":"total","type":"double"}
    ]}"#;

    /// A datum of `ORDER_AVRO`: `id = "a"`, `total = 1.0`.
    #[cfg(feature = "avro")]
    fn order_avro_datum() -> Vec<u8> {
        use apache_avro::types::Value;
        let schema = apache_avro::schema::Schema::parse_str(ORDER_AVRO).unwrap();
        let record = Value::Record(vec![
            ("id".to_owned(), Value::String("a".to_owned())),
            ("total".to_owned(), Value::Double(1.0)),
        ]);
        apache_avro::to_avro_datum(&schema, record).unwrap()
    }

    #[cfg(feature = "avro")]
    #[test]
    fn avro_accepts_a_datum_of_its_own_schema() {
        check!(validate_avro(ORDER_AVRO, &order_avro_datum()).is_ok());
    }

    #[cfg(feature = "avro")]
    #[test]
    fn avro_rejects_bodies_that_are_not_instances() {
        let full = order_avro_datum();
        let cases = [
            ("empty", Vec::new()),
            ("truncated", full[..full.len() - 1].to_vec()),
            ("trailing bytes", [full.clone(), vec![0xFF]].concat()),
            ("unrelated bytes", b"plain text, no serializer".to_vec()),
        ];
        for (name, body) in cases {
            assert!(let Err(error) = validate_avro(ORDER_AVRO, &body), "case {name}");
            check!(
                matches!(error, SchemaSerdeError::Deserialize(_)),
                "case {name}: {error}"
            );
        }
    }

    #[cfg(feature = "avro")]
    #[test]
    fn avro_reports_an_unparseable_schema_as_a_schema_error() {
        assert!(let Err(error) = validate_avro("{not avro}", &[]));
        check!(matches!(error, SchemaSerdeError::Schema(_)), "{error}");
    }

    #[cfg(feature = "json")]
    const ORDER_JSON: &str = r#"{
        "type":"object",
        "properties":{"id":{"type":"string"},"total":{"type":"number"}},
        "required":["id","total"],
        "additionalProperties":false
    }"#;

    #[cfg(feature = "json")]
    #[test]
    fn json_accepts_and_rejects_by_the_schema() {
        let cases = [
            (r#"{"id":"a","total":1}"#, true),
            // `total` is the wrong type.
            (r#"{"id":"a","total":"1"}"#, false),
            // `total` is absent, and it is required.
            (r#"{"id":"a"}"#, false),
            // `additionalProperties` is false.
            (r#"{"id":"a","total":1,"extra":true}"#, false),
            // Not JSON at all.
            ("plain text, no serializer", false),
        ];
        for (body, want_ok) in cases {
            check!(
                validate_json(ORDER_JSON, body.as_bytes()).is_ok() == want_ok,
                "body {body}"
            );
        }
    }

    #[cfg(feature = "protobuf")]
    const ORDER_PROTO: &str = r#"
        syntax = "proto3";
        message Order {
          string id = 1;
          double total = 2;
          message Line { string sku = 1; }
        }
        message Other { int32 n = 1; }
    "#;

    #[cfg(feature = "protobuf")]
    fn order_proto_pool() -> prost_reflect::DescriptorPool {
        use prost_reflect::{DescriptorPool, prost_types::FileDescriptorSet};
        let file = protox_parse::parse("schema.proto", ORDER_PROTO).unwrap();
        DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] }).unwrap()
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn protobuf_accepts_a_message_of_the_indexed_type() {
        // `Order { id: "a" }` — field 1, wire type 2, one byte of payload.
        let body = [0x0A, 0x01, b'a'];
        check!(validate_protobuf(ORDER_PROTO, &[0], &body).is_ok());
        // An empty index is Confluent's implicit `[0]`.
        check!(validate_protobuf(ORDER_PROTO, &[], &body).is_ok());
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn protobuf_message_index_walks_to_the_named_message() {
        let pool = order_proto_pool();
        let cases = [
            // An empty path is Confluent's implicit `[0]`.
            (&[][..], "Order"),
            (&[0][..], "Order"),
            (&[1][..], "Other"),
            (&[0, 0][..], "Order.Line"),
        ];
        for (path, want) in cases {
            assert!(let Ok(descriptor) = message_at(&pool, path), "path {path:?}");
            check!(descriptor.full_name() == want, "path {path:?}");
        }
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn protobuf_rejects_a_wire_type_the_named_message_does_not_declare() {
        // `[1]` is `Other`, whose field 1 is a varint. The body carries field
        // 1 length-delimited, which that message does not describe.
        assert!(let Err(error) = validate_protobuf(ORDER_PROTO, &[1], &[0x0A, 0x01, b'a']));
        check!(matches!(error, SchemaSerdeError::Deserialize(_)), "{error}");
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn protobuf_accepts_a_field_number_the_message_does_not_declare() {
        // The documented limit, asserted so it stays a known property rather
        // than becoming a surprise. Field 9 is in no message of ORDER_PROTO,
        // so `Order` keeps it as an unknown field instead of failing.
        check!(validate_protobuf(ORDER_PROTO, &[0], &[0x48, 0x01]).is_ok());
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn protobuf_rejects_an_index_that_names_no_message() {
        for path in [&[9][..], &[0, 9][..]] {
            assert!(let Err(error) = validate_protobuf(ORDER_PROTO, path, &[]), "{path:?}");
            check!(
                matches!(error, SchemaSerdeError::Schema(_)),
                "{path:?}: {error}"
            );
        }
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn protobuf_rejects_a_body_that_is_not_a_message() {
        // Field number 0 is illegal in every protobuf message.
        assert!(let Err(error) = validate_protobuf(ORDER_PROTO, &[0], &[0x00, 0x01]));
        check!(matches!(error, SchemaSerdeError::Deserialize(_)), "{error}");
    }

    #[cfg(all(feature = "avro", feature = "protobuf", feature = "json"))]
    #[test]
    fn validate_body_dispatches_on_the_kind() {
        check!(validate_body(SchemaKind::Avro, ORDER_AVRO, &[], &order_avro_datum()).is_ok());
        check!(
            validate_body(
                SchemaKind::Json,
                ORDER_JSON,
                &[],
                br#"{"id":"a","total":1}"#
            )
            .is_ok()
        );
        check!(validate_body(SchemaKind::Protobuf, ORDER_PROTO, &[0], &[0x0A, 0x01, b'a']).is_ok());
    }
}
