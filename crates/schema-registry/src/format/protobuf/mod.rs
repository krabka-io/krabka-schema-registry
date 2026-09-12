//! Protobuf support.
//!
//! This module parses a single `.proto` source into a `FileDescriptorProto`.
//! The dedup key is the deterministic prost encoding of the descriptor, with
//! source-info cleared so that formatting does not change the bytes. The
//! implementation does not attempt Confluent's full canonicalization rules.
//!
//! `normalized_form()` reproduces the pretty-printed text that
//! cp-schema-registry normalises to, verified against the golden fixtures:
//!
//!   `syntax = "proto3";\n\n<messages>\n`
//!
//! Each message is formatted with 2-space indentation. The store keeps this
//! text in `by_id` so that the REST echo-back matches what cp-schema-registry
//! would return.

mod compat;
mod diff;

pub use krabka_schema_serde::format::protobuf::normalize;
use prost_reflect::{
    DescriptorPool,
    prost::Message,
    prost_types::{FileDescriptorProto, FileDescriptorSet},
};

use super::ParsedSchema;
use crate::error::SrError;

pub struct ProtobufSchema {
    descriptor: FileDescriptorProto,
    /// Normalised `.proto` text (cp-schema-registry compatible pretty-print).
    normalised: String,
}

/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub fn parse(schema: &str, refs: &[super::ResolvedReference]) -> Result<ProtobufSchema, SrError> {
    let descriptor = protox_parse::parse("schema.proto", schema)
        .map_err(|e| SrError::InvalidSchema(format!("Protobuf: {e}")))?;
    // Link the candidate + its (protobuf) references so imports resolve and
    // cross-file types validate. The reference `name` IS the import path.
    // Trigger linking whenever the candidate declares imports (so an unresolved
    // import is caught) or references are supplied.
    if !descriptor.dependency.is_empty() || !refs.is_empty() {
        let mut files: Vec<FileDescriptorProto> = Vec::with_capacity(refs.len() + 1);
        for r in refs.iter().filter(|r| r.ty == super::SchemaType::Protobuf) {
            let dep = protox_parse::parse(&r.name, &r.schema).map_err(|e| {
                SrError::InvalidSchema(format!("Protobuf reference {}: {e}", r.name))
            })?;
            files.push(dep);
        }
        files.push(descriptor.clone());
        DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: files })
            .map_err(|e| SrError::InvalidSchema(format!("Protobuf link: {e}")))?;
    }
    let normalised = normalize(&descriptor);
    Ok(ProtobufSchema {
        descriptor,
        normalised,
    })
}

impl ProtobufSchema {
    /// Return the normalised `.proto` text (cp-schema-registry compatible).
    #[must_use]
    pub fn normalized_form(&self) -> &str {
        &self.normalised
    }

    pub(crate) fn descriptor(&self) -> &FileDescriptorProto {
        &self.descriptor
    }
}

/// Confluent Protobuf compatibility. It answers whether a reader that uses
/// `reader` can read data written with `writer`. It computes the structural
/// diff, with `writer` as the original and `reader` as the update, and rejects
/// the pair if any difference is backward-incompatible.
#[tracing::instrument(level = "debug", name = "protobuf.check", skip_all, fields(reader_refs = reader_refs.len(), writer_refs = writer_refs.len(), diffs = tracing::field::Empty))]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub fn check(
    reader: &str,
    writer: &str,
    reader_refs: &[super::ResolvedReference],
    writer_refs: &[super::ResolvedReference],
) -> Result<(), Vec<String>> {
    let reader_d = parse(reader, reader_refs).map_err(|e| vec![format!("reader: {e}")])?;
    let writer_d = parse(writer, writer_refs).map_err(|e| vec![format!("writer: {e}")])?;
    let mut diffs = diff::compare(writer_d.descriptor(), reader_d.descriptor());
    for writer_ref in writer_refs
        .iter()
        .filter(|reference| reference.ty == super::SchemaType::Protobuf)
    {
        let Some(reader_ref) = reader_refs.iter().find(|reference| {
            reference.ty == super::SchemaType::Protobuf && reference.name == writer_ref.name
        }) else {
            continue;
        };
        if writer_ref.schema == reader_ref.schema {
            continue;
        }
        let writer_ref = parse(&writer_ref.schema, writer_refs)
            .map_err(|error| vec![format!("writer reference: {error}")])?;
        let reader_ref = parse(&reader_ref.schema, reader_refs)
            .map_err(|error| vec![format!("reader reference: {error}")])?;
        diffs.extend(diff::compare(
            writer_ref.descriptor(),
            reader_ref.descriptor(),
        ));
    }
    tracing::Span::current().record("diffs", diffs.len());
    let incompatible: Vec<&diff::Difference> = diffs
        .iter()
        .filter(|d| !compat::is_backward_compatible(&d.kind))
        .collect();
    if incompatible.is_empty() {
        Ok(())
    } else {
        Err(compat::messages(&incompatible))
    }
}

impl ParsedSchema for ProtobufSchema {
    fn canonical_form(&self) -> String {
        // Clone the descriptor, clear source_code_info (formatting/comments)
        // and the file name so neither whitespace nor the synthetic filename
        // affects the dedup key. Then prost-encode to bytes and hex-encode.
        // NOTE: this is a descriptor-bytes key, not Confluent canonical form
        // which this implementation intentionally does not attempt.
        tracing::debug!(
            "protobuf canonical_form: using descriptor-bytes key (not Confluent canonical form)"
        );
        let mut d = self.descriptor.clone();
        d.source_code_info = None;
        d.name = None;
        // hex-encode for a printable, stable string (lowercase, like `{:02x}`)
        hex::encode(d.encode_to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::ParsedSchema;

    const P: &str = "syntax = \"proto3\"; message User { int32 id = 1; }";

    #[test]
    fn parses_and_is_stable() {
        let a = parse(P, &[]).unwrap();
        let b = parse(
            "syntax = \"proto3\";\nmessage User {\n  int32 id = 1;\n}\n",
            &[],
        )
        .unwrap();
        assert2::assert!(a.canonical_form() == b.canonical_form());
    }

    #[test]
    fn rejects_invalid_proto() {
        assert2::assert!(parse("this is not protobuf", &[]).is_err());
    }

    fn p(body: &str) -> String {
        format!("syntax = \"proto3\"; message U {{ {body} }}")
    }

    #[test]
    fn compatibility_basic_cases_are_named_and_table_driven() {
        let plain = "syntax = \"proto3\"; message U { int32 a = 1; int32 b = 2; }";
        let oneof = "syntax = \"proto3\"; message U { oneof x { int32 a = 1; int32 b = 2; } }";
        let small = "syntax = \"proto3\"; message U { int32 id = 1; }";
        for (_name, reader, writer, compatible) in [
            (
                "field-added",
                p("int32 id = 1; int32 x = 2;"),
                p("int32 id = 1;"),
                true,
            ),
            (
                "field-removed",
                p("int32 id = 1;"),
                p("int32 id = 1; int32 x = 2;"),
                true,
            ),
            (
                "scalar-same-wire-group",
                p("int64 id = 1;"),
                p("int32 id = 1;"),
                true,
            ),
            (
                "scalar-cross-wire-group",
                p("string id = 1;"),
                p("int32 id = 1;"),
                false,
            ),
            (
                "singular-to-repeated",
                p("repeated int32 id = 1;"),
                p("int32 id = 1;"),
                true,
            ),
            (
                "scalar-to-message",
                "syntax = \"proto3\"; message M {} message U { M id = 1; }".to_string(),
                small.to_string(),
                false,
            ),
            (
                "move-into-oneof",
                oneof.to_string(),
                plain.to_string(),
                false,
            ),
            (
                "move-out-of-oneof",
                plain.to_string(),
                oneof.to_string(),
                true,
            ),
            (
                "proto3-optional",
                "syntax = \"proto3\"; message U { optional int32 a = 1; }".to_string(),
                "syntax = \"proto3\"; message U { int32 a = 1; }".to_string(),
                true,
            ),
        ] {
            assert2::assert!(check(&reader, &writer, &[], &[]).is_ok() == compatible);
        }
    }

    #[test]
    fn compatibility_extended_cases_are_named_and_table_driven() {
        let small = "syntax = \"proto3\"; message U { int32 id = 1; }";
        let big = "syntax = \"proto3\"; message U { int32 id = 1; } message V { int32 a = 1; }";

        for (_name, reader, writer, compatible) in [
            (
                "reserve-number",
                "syntax = \"proto3\"; message U { reserved 2; int32 id = 1; }".to_string(),
                small.to_string(),
                true,
            ),
            (
                "map-cross-wire-group",
                "syntax = \"proto3\"; message U { map<string, string> m = 1; }".to_string(),
                "syntax = \"proto3\"; message U { map<string, int32> m = 1; }".to_string(),
                false,
            ),
            (
                "identical-map",
                "syntax = \"proto3\"; message U { map<string, int32> m = 1; }".to_string(),
                "syntax = \"proto3\"; message U { map<string, int32> m = 1; }".to_string(),
                true,
            ),
            (
                "enum-constant-added",
                "syntax = \"proto3\"; enum E { A = 0; B = 1; } message U { E e = 1; }".to_string(),
                "syntax = \"proto3\"; enum E { A = 0; } message U { E e = 1; }".to_string(),
                true,
            ),
            (
                "nested-field-cross-group",
                "syntax = \"proto3\"; message U { message N { string a = 1; } N n = 1; }"
                    .to_string(),
                "syntax = \"proto3\"; message U { message N { int32 a = 1; } N n = 1; }"
                    .to_string(),
                false,
            ),
            (
                "package-renamed",
                "syntax = \"proto3\"; package b; message U { int32 id = 1; }".to_string(),
                "syntax = \"proto3\"; package a; message U { int32 id = 1; }".to_string(),
                true,
            ),
            (
                "int-to-enum",
                "syntax = \"proto3\"; enum E { A = 0; } message U { E id = 1; }".to_string(),
                small.to_string(),
                true,
            ),
            (
                "reader-message-added",
                big.to_string(),
                small.to_string(),
                true,
            ),
            (
                "reader-message-removed",
                small.to_string(),
                big.to_string(),
                false,
            ),
        ] {
            assert2::assert!(check(&reader, &writer, &[], &[]).is_ok() == compatible);
        }
    }

    #[test]
    fn milestone_23_field_rules() {
        for (name, reader, writer, compatible) in [
            (
                "oneof-field-removed",
                "syntax = \"proto3\"; message U { oneof x { int32 a = 1; } }",
                "syntax = \"proto3\"; message U { oneof x { int32 a = 1; int32 b = 2; } }",
                false,
            ),
            (
                "existing-plus-new-moved-to-oneof",
                "syntax = \"proto3\"; message U { oneof x { int32 a = 1; int32 b = 2; } }",
                "syntax = \"proto3\"; message U { int32 a = 1; }",
                true,
            ),
            (
                "proto2-required-added",
                "syntax = \"proto2\"; message U { required int32 a = 1; }",
                "syntax = \"proto2\"; message U {}",
                false,
            ),
            (
                "proto2-required-removed",
                "syntax = \"proto2\"; message U {}",
                "syntax = \"proto2\"; message U { required int32 a = 1; }",
                false,
            ),
            (
                "proto2-numeric-label",
                "syntax = \"proto2\"; message U { repeated int32 a = 1; }",
                "syntax = \"proto2\"; message U { optional int32 a = 1; }",
                false,
            ),
            (
                "proto3-explicit-numeric-label",
                "syntax = \"proto3\"; message U { repeated int32 a = 1; }",
                "syntax = \"proto3\"; message U { optional int32 a = 1; }",
                false,
            ),
            (
                "string-label",
                "syntax = \"proto2\"; message U { repeated string a = 1; }",
                "syntax = \"proto2\"; message U { optional string a = 1; }",
                true,
            ),
        ] {
            assert2::assert!(
                check(reader, writer, &[], &[]).is_ok() == compatible,
                "{name}"
            );
        }
    }

    // ── reference resolution ─────────────────────────────────────────────────

    #[test]
    fn protobuf_resolves_import_reference() {
        use crate::format::{ResolvedReference, SchemaType};
        let dep = "syntax = \"proto3\"; package m; message Money { int64 cents = 1; }";
        let candidate =
            "syntax = \"proto3\"; import \"money.proto\"; message Order { m.Money price = 1; }";
        // With the import provided as a reference (name = import path), it links.
        for (_name, refs, expected) in [
            (
                "resolved_import",
                vec![ResolvedReference {
                    name: "money.proto".into(),
                    ty: SchemaType::Protobuf,
                    schema: dep.into(),
                }],
                true,
            ),
            ("unresolved_import", vec![], false),
        ] {
            assert2::assert!(parse(candidate, &refs).is_ok() == expected);
        }
    }

    #[test]
    fn protobuf_diffs_changed_imports() {
        use crate::format::{ResolvedReference, SchemaType};
        let candidate =
            "syntax = \"proto3\"; import \"money.proto\"; message Order { m.Money price = 1; }";
        let reference = |schema: &str| ResolvedReference {
            name: "money.proto".into(),
            ty: SchemaType::Protobuf,
            schema: schema.into(),
        };
        let old = reference("syntax = \"proto3\"; package m; message Money { int64 cents = 1; }");
        let changed =
            reference("syntax = \"proto3\"; package m; message Money { string cents = 1; }");

        assert2::assert!(
            check(
                candidate,
                candidate,
                std::slice::from_ref(&old),
                std::slice::from_ref(&old)
            )
            .is_ok()
        );
        assert2::assert!(check(candidate, candidate, &[changed], &[old]).is_err());
    }

    #[test]
    fn normalize_emits_package_and_imports_cp_exact() {
        use crate::format::{ResolvedReference, SchemaType};
        // Packaged schema: `package` follows `syntax` with no blank line, then a
        // blank line precedes the message (verified against cp-schema-registry 7.4.0).
        let money = "syntax = \"proto3\"; package m; message Money { int64 cents = 1; }";
        // Importing schema: blank line, `import`, blank line, message (cp-exact).
        let order =
            "syntax = \"proto3\"; import \"money.proto\"; message Order { m.Money price = 1; }";
        for (_name, schema, refs, expected) in [
            (
                "package",
                money,
                vec![],
                "syntax = \"proto3\";\npackage m;\n\nmessage Money {\n  int64 cents = 1;\n}\n",
            ),
            (
                "import",
                order,
                vec![ResolvedReference {
                    name: "money.proto".into(),
                    ty: SchemaType::Protobuf,
                    schema: money.into(),
                }],
                "syntax = \"proto3\";\n\nimport \"money.proto\";\n\nmessage Order {\n  m.Money price = 1;\n}\n",
            ),
        ] {
            assert2::assert!(parse(schema, &refs).unwrap().normalized_form() == expected);
        }
    }

    #[test]
    fn normalize_preserves_nested_enum_and_message_indentation() {
        use prost_reflect::prost_types::{
            DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
            field_descriptor_proto::{Label, Type as FieldType},
        };

        let fdp = FileDescriptorProto {
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Outer".into()),
                enum_type: vec![EnumDescriptorProto {
                    name: Some("Kind".into()),
                    value: vec![
                        EnumValueDescriptorProto {
                            name: Some("KIND_UNSPECIFIED".into()),
                            number: Some(0),
                            ..Default::default()
                        },
                        EnumValueDescriptorProto {
                            name: Some("KIND_READY".into()),
                            number: Some(1),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                nested_type: vec![DescriptorProto {
                    name: Some("Inner".into()),
                    field: vec![FieldDescriptorProto {
                        name: Some("id".into()),
                        number: Some(1),
                        label: Some(Label::Optional as i32),
                        r#type: Some(FieldType::String as i32),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        assert2::assert!(
            normalize(&fdp)
                == "syntax = \"proto3\";\n\nmessage Outer {\n  enum Kind {\n    KIND_UNSPECIFIED = 0;\n    KIND_READY = 1;\n  }\n  message Inner {\n    string id = 1;\n  }\n}\n"
        );
    }
}
