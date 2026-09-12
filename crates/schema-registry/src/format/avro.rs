//! Avro support: parse and Parsing Canonical Form through `apache-avro`. This
//! module also runs the directional compatibility check through
//! [`apache_avro::schema_compatibility::SchemaCompatibility`].

use apache_avro::schema_compatibility::SchemaCompatibility;

use super::ParsedSchema;
use crate::error::SrError;

pub struct AvroSchema {
    parsed: apache_avro::Schema,
    identity: String,
}

/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub fn parse(schema: &str, refs: &[super::ResolvedReference]) -> Result<AvroSchema, SrError> {
    let identity = serde_json::from_str::<serde_json::Value>(schema)
        .and_then(|value| serde_json::to_string(&value))
        .map_err(|e| SrError::InvalidSchema(format!("Avro: {e}")))?;
    if refs.is_empty() {
        return apache_avro::Schema::parse_str(schema)
            .map(|parsed| AvroSchema { parsed, identity })
            .map_err(|e| SrError::InvalidSchema(format!("Avro: {e}")));
    }
    // Dependencies first (so their named types are in scope), candidate last.
    let mut sources: Vec<&str> = refs.iter().map(|r| r.schema.as_str()).collect();
    sources.push(schema);
    let parsed = apache_avro::Schema::parse_list(&sources)
        .map_err(|e| SrError::InvalidSchema(format!("Avro: {e}")))?;
    // `parse_list` preserves input order; the candidate is the last entry.
    parsed
        .into_iter()
        .next_back()
        .map(|parsed| AvroSchema { parsed, identity })
        .ok_or_else(|| SrError::InvalidSchema("Avro: empty parse_list".into()))
}

impl ParsedSchema for AvroSchema {
    fn canonical_form(&self) -> String {
        self.identity.clone()
    }
}

/// Directional Avro check. It answers whether a reader that uses `reader` can
/// read data written with `writer`. It returns `Ok(())` when the pair is
/// compatible, and `Err(messages)` otherwise.
#[tracing::instrument(level = "debug", name = "avro.check", skip_all, fields(reader_refs = reader_refs.len(), writer_refs = writer_refs.len()))]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub fn check(
    reader: &str,
    writer: &str,
    reader_refs: &[super::ResolvedReference],
    writer_refs: &[super::ResolvedReference],
) -> Result<(), Vec<String>> {
    let mut reader_value: serde_json::Value =
        serde_json::from_str(reader).map_err(|e| vec![format!("reader: Avro: {e}")])?;
    let mut writer_value: serde_json::Value =
        serde_json::from_str(writer).map_err(|e| vec![format!("writer: Avro: {e}")])?;
    erase_logical_types(&mut reader_value);
    erase_logical_types(&mut writer_value);
    apply_reader_rules(&mut reader_value, &mut writer_value);
    let reader =
        serde_json::to_string(&reader_value).map_err(|e| vec![format!("reader: Avro: {e}")])?;
    let writer =
        serde_json::to_string(&writer_value).map_err(|e| vec![format!("writer: Avro: {e}")])?;
    let reader_schema = parse(&reader, reader_refs)
        .map_err(|e| vec![format!("reader: {e}")])?
        .parsed;
    let writer_schema = parse(&writer, writer_refs)
        .map_err(|e| vec![format!("writer: {e}")])?
        .parsed;
    SchemaCompatibility::can_read(&writer_schema, &reader_schema).map_err(|e| vec![e.to_string()])
}

fn erase_logical_types(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            if object.remove("logicalType").is_some() {
                object.remove("precision");
                object.remove("scale");
            }
            object.values_mut().for_each(erase_logical_types);
        }
        serde_json::Value::Array(array) => array.iter_mut().for_each(erase_logical_types),
        _ => {}
    }
}

fn apply_reader_rules(reader: &mut serde_json::Value, writer: &mut serde_json::Value) {
    let (Some(reader_object), Some(writer_object)) =
        (reader.as_object_mut(), writer.as_object_mut())
    else {
        if let (Some(reader), Some(writer)) = (reader.as_array_mut(), writer.as_array_mut()) {
            for (reader, writer) in reader.iter_mut().zip(writer) {
                apply_reader_rules(reader, writer);
            }
        }
        return;
    };

    let reader_name = reader_object
        .get("name")
        .and_then(serde_json::Value::as_str);
    let writer_name = writer_object
        .get("name")
        .and_then(serde_json::Value::as_str);
    if let (Some(reader_name), Some(writer_name)) = (reader_name, writer_name)
        && reader_name != writer_name
        && reader_object
            .get("aliases")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|aliases| {
                aliases
                    .iter()
                    .any(|alias| alias.as_str() == Some(writer_name))
            })
    {
        writer_object.insert("name".into(), reader_name.into());
    }

    if reader_object
        .get("type")
        .and_then(serde_json::Value::as_str)
        == Some("enum")
        && writer_object
            .get("type")
            .and_then(serde_json::Value::as_str)
            == Some("enum")
    {
        let default = reader_object
            .get("default")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        let writer_symbols = writer_object
            .get("symbols")
            .and_then(serde_json::Value::as_array)
            .cloned();
        if let (Some(default), Some(writer_symbols), Some(reader_symbols)) = (
            default,
            writer_symbols,
            reader_object
                .get_mut("symbols")
                .and_then(serde_json::Value::as_array_mut),
        ) && reader_symbols
            .iter()
            .any(|symbol| symbol.as_str() == Some(&default))
        {
            for symbol in writer_symbols {
                if !reader_symbols.contains(&symbol) {
                    reader_symbols.push(symbol);
                }
            }
        }
    }

    if let (Some(reader_fields), Some(writer_fields)) = (
        reader_object
            .get_mut("fields")
            .and_then(serde_json::Value::as_array_mut),
        writer_object
            .get_mut("fields")
            .and_then(serde_json::Value::as_array_mut),
    ) {
        for reader_field in reader_fields {
            let Some(name) = reader_field.get("name").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let Some(writer_field) = writer_fields
                .iter_mut()
                .find(|field| field.get("name").and_then(serde_json::Value::as_str) == Some(name))
            else {
                continue;
            };
            if let (Some(reader_type), Some(writer_type)) =
                (reader_field.get_mut("type"), writer_field.get_mut("type"))
            {
                apply_reader_rules(reader_type, writer_type);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avro_check_directions() {
        let old = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"}]}"#;
        let new = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int","default":0}]}"#;
        let new_nodef = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int"}]}"#;
        for (_name, reader, writer, compatible) in [
            ("new_reads_old", new, old, true),
            ("old_reads_new", old, new, true),
            ("missing_default", new_nodef, old, false),
        ] {
            assert2::assert!(check(reader, writer, &[], &[]).is_ok() == compatible);
        }
    }

    #[test]
    fn avro_resolves_named_reference() {
        use crate::format::ResolvedReference;
        let money = r#"{"type":"record","name":"Money","fields":[{"name":"cents","type":"long"}]}"#;
        let candidate =
            r#"{"type":"record","name":"Order","fields":[{"name":"price","type":"Money"}]}"#;
        let refs = vec![ResolvedReference {
            name: "Money".into(),
            ty: crate::format::SchemaType::Avro,
            schema: money.into(),
        }];
        for (_name, refs, valid) in [
            ("unresolved", &[][..], false),
            ("resolved", refs.as_slice(), true),
        ] {
            assert2::assert!(parse(candidate, refs).is_ok() == valid);
        }
    }

    #[test]
    fn avro_matches_java_logical_enum_and_alias_rules() {
        for (name, reader, writer) in [
            (
                "logical-types",
                r#"{"type":"long","logicalType":"timestamp-micros"}"#,
                r#"{"type":"long","logicalType":"timestamp-millis"}"#,
            ),
            (
                "enum-default",
                r#"{"type":"enum","name":"E","symbols":["A"],"default":"A"}"#,
                r#"{"type":"enum","name":"E","symbols":["A","B"]}"#,
            ),
            (
                "record-alias",
                r#"{"type":"record","name":"New","aliases":["Old"],"fields":[]}"#,
                r#"{"type":"record","name":"Old","fields":[]}"#,
            ),
        ] {
            assert2::assert!(check(reader, writer, &[], &[]).is_ok(), "{name}");
        }
    }
}
