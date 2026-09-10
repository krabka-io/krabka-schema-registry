//! Protobuf serde over `prost` and `prost-reflect`.
//!
//! The local message gives its descriptor with `ReflectMessage`. The registered
//! schema is the normalized `.proto` text of its file descriptor.

use std::{fmt::Write as _, marker::PhantomData, sync::Arc};

use bytes::Bytes;
use prost::Message;
use prost_reflect::{
    MessageDescriptor, ReflectMessage,
    prost_types::{
        DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorProto,
        ServiceDescriptorProto,
        field_descriptor_proto::{Label, Type as FieldType},
    },
};

use crate::{
    cache::SchemaCache,
    error::SchemaSerdeError,
    format::{Binding, SchemaDeserializer, SchemaSerializer, SchemaSubject},
    registry::model::SchemaReference,
    subject::{Role, SchemaKind},
    wire,
};

/// Protobuf serializer and deserializer for a `prost` message
/// `T: ReflectMessage`, bound to a key/value role.
///
/// The serde takes the subject from the topic at call time.
pub struct ProtobufSerde<T> {
    binding: Binding,
    message_index: Vec<i32>,
    _marker: PhantomData<fn() -> T>,
}

// Manual `Clone` (not derived) to avoid a spurious `T: Clone` bound;
// `add_source`/`add_sink` require `Serde<_> + Clone`.
impl<T> Clone for ProtobufSerde<T> {
    fn clone(&self) -> Self {
        Self {
            binding: self.binding.clone(),
            message_index: self.message_index.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: ReflectMessage + Default> ProtobufSerde<T> {
    fn make(cache: &Arc<SchemaCache>, role: Role) -> Self {
        let descriptor = T::default().descriptor();
        let proto_text = proto_source(&descriptor);
        let message_index = message_index(&descriptor)
            .expect("a message descriptor is reachable from its parent file");
        Self {
            binding: Binding {
                cache: Arc::clone(cache),
                role,
                kind: SchemaKind::Protobuf,
                schema: proto_text,
                references: Vec::new(),
                message_type: Some(descriptor.full_name().to_string()),
            },
            message_index,
            _marker: PhantomData,
        }
    }

    /// A Protobuf serde for record **values**: `<topic>-value`.
    pub fn value(cache: &Arc<SchemaCache>) -> Self {
        Self::make(cache, Role::Value)
    }

    /// A Protobuf serde for record **keys**: `<topic>-key`.
    pub fn key(cache: &Arc<SchemaCache>) -> Self {
        Self::make(cache, Role::Key)
    }

    /// Attach the references sent with register and lookup requests.
    #[must_use]
    pub fn with_references(mut self, references: Vec<SchemaReference>) -> Self {
        self.binding.references = references;
        self
    }
}

/// A value serde over the process [`default_registry`](crate::default_registry).
impl<T: ReflectMessage + Default> Default for ProtobufSerde<T> {
    fn default() -> Self {
        let cache = crate::default_registry().expect(
            "schema-serde: call set_default_registry(cache) before a default ProtobufSerde",
        );
        Self::value(&cache)
    }
}

impl<T: Send + Sync + 'static> SchemaSubject for ProtobufSerde<T> {
    fn register_subject(&self, topic: &str) {
        self.binding.register(topic);
    }
}

impl<T> SchemaSerializer<T> for ProtobufSerde<T>
where
    T: Message + ReflectMessage + Send + Sync + 'static,
{
    fn serialize(&self, topic: &str, value: &T) -> Result<Bytes, SchemaSerdeError> {
        let id = self.binding.id(topic)?;
        let body = value.encode_to_vec();
        Ok(wire::encode_protobuf(id, &self.message_index, &body))
    }
}

impl<T> SchemaDeserializer<T> for ProtobufSerde<T>
where
    T: Message + ReflectMessage + Default + Send + Sync + 'static,
{
    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<T, SchemaSerdeError> {
        // prost decodes structurally; id/index validated by framing and, when
        // registry metadata is available, by the declared protobuf message type.
        let (id, _idx, body) = wire::decode_protobuf(bytes)?;
        if let Some(writer_message_type) = self.binding.cache.writer_message_type(id) {
            let local_message_type = T::default().descriptor().full_name().to_string();
            if writer_message_type != local_message_type {
                return Err(SchemaSerdeError::Deserialize(format!(
                    "protobuf messageType mismatch: writer {writer_message_type}, local {local_message_type}"
                )));
            }
        }
        T::decode(body).map_err(|e| SchemaSerdeError::Deserialize(e.to_string()))
    }
}

/// Render the file descriptor of `descriptor`'s parent file to `.proto` text.
fn proto_source(descriptor: &prost_reflect::MessageDescriptor) -> String {
    let file = descriptor.parent_file();
    normalize(file.file_descriptor_proto())
}

/// Compute the Confluent message-index path of `descriptor` within its file.
///
/// # Errors
///
/// Returns a schema error if the descriptor cannot be reached from its parent
/// file's top-level message list.
pub fn message_index(descriptor: &MessageDescriptor) -> Result<Vec<i32>, SchemaSerdeError> {
    let file = descriptor.parent_file();
    message_index_in(file.messages(), descriptor.full_name()).ok_or_else(|| {
        SchemaSerdeError::Schema(format!(
            "protobuf message {} is not reachable from its parent file",
            descriptor.full_name()
        ))
    })
}

fn message_index_in(
    messages: impl Iterator<Item = MessageDescriptor>,
    target: &str,
) -> Option<Vec<i32>> {
    for (index, message) in messages.enumerate() {
        let index = i32::try_from(index).ok()?;
        if message.full_name() == target {
            return Some(vec![index]);
        }
        if let Some(mut child) = message_index_in(message.child_messages(), target) {
            child.insert(0, index);
            return Some(child);
        }
    }
    None
}

/// Return Confluent-compatible normalized `.proto` text.
#[must_use]
pub fn normalize(file: &FileDescriptorProto) -> String {
    let mut out = String::new();
    let syntax = file.syntax.as_deref().unwrap_or("proto3");
    let _ = writeln!(out, "syntax = \"{syntax}\";");
    if let Some(package) = file
        .package
        .as_deref()
        .filter(|package| !package.is_empty())
    {
        let _ = writeln!(out, "package {package};");
    }
    for dependency in &file.dependency {
        out.push('\n');
        let _ = writeln!(out, "import \"{dependency}\";");
    }
    let package = file.package.as_deref().unwrap_or("");
    for enumeration in &file.enum_type {
        out.push('\n');
        write_enum(&mut out, enumeration, 0);
    }
    for message in &file.message_type {
        out.push('\n');
        write_message(&mut out, message, 0, package, syntax);
    }
    for service in &file.service {
        out.push('\n');
        write_service(&mut out, service, package);
    }
    out
}

fn write_message(
    out: &mut String,
    message: &DescriptorProto,
    depth: usize,
    package: &str,
    syntax: &str,
) {
    let indent = "  ".repeat(depth);
    let _ = writeln!(
        out,
        "{indent}message {} {{",
        message.name.as_deref().unwrap_or("Unknown")
    );
    write_reserved(out, message, depth + 1);
    for enumeration in &message.enum_type {
        write_enum(out, enumeration, depth + 1);
    }
    for field in message
        .field
        .iter()
        .filter(|field| field.oneof_index.is_none())
    {
        write_field(out, field, message, depth + 1, package, syntax);
    }
    for (index, oneof) in message.oneof_decl.iter().enumerate() {
        let fields: Vec<_> = message
            .field
            .iter()
            .filter(|field| field.oneof_index == i32::try_from(index).ok())
            .collect();
        if fields.len() == 1 && fields[0].proto3_optional.unwrap_or(false) {
            write_field(out, fields[0], message, depth + 1, package, syntax);
            continue;
        }
        let child_indent = "  ".repeat(depth + 1);
        let _ = writeln!(
            out,
            "{child_indent}oneof {} {{",
            oneof.name.as_deref().unwrap_or("unknown")
        );
        for field in fields {
            write_field(out, field, message, depth + 2, package, syntax);
        }
        let _ = writeln!(out, "{child_indent}}}");
    }
    for nested in message.nested_type.iter().filter(|nested| {
        !nested
            .options
            .as_ref()
            .is_some_and(prost_reflect::prost_types::MessageOptions::map_entry)
    }) {
        write_message(out, nested, depth + 1, package, syntax);
    }
    let _ = writeln!(out, "{indent}}}");
}

fn write_reserved(out: &mut String, message: &DescriptorProto, depth: usize) {
    let indent = "  ".repeat(depth);
    for range in &message.reserved_range {
        let start = range.start.unwrap_or_default();
        let end = range.end.unwrap_or(start + 1) - 1;
        if start == end {
            let _ = writeln!(out, "{indent}reserved {start};");
        } else {
            let _ = writeln!(out, "{indent}reserved {start} to {end};");
        }
    }
    if !message.reserved_name.is_empty() {
        let names = message
            .reserved_name
            .iter()
            .map(|name| format!("\"{name}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(out, "{indent}reserved {names};");
    }
}

fn write_enum(out: &mut String, enumeration: &EnumDescriptorProto, depth: usize) {
    let indent = "  ".repeat(depth);
    let _ = writeln!(
        out,
        "{indent}enum {} {{",
        enumeration.name.as_deref().unwrap_or("Unknown")
    );
    for value in &enumeration.value {
        let _ = writeln!(
            out,
            "{indent}  {} = {};",
            value.name.as_deref().unwrap_or("UNKNOWN"),
            value.number.unwrap_or_default()
        );
    }
    let _ = writeln!(out, "{indent}}}");
}

fn write_field(
    out: &mut String,
    field: &FieldDescriptorProto,
    parent: &DescriptorProto,
    depth: usize,
    package: &str,
    syntax: &str,
) {
    let indent = "  ".repeat(depth);
    let label = if field.proto3_optional.unwrap_or(false) {
        "optional "
    } else if map_entry(parent, field).is_some() {
        ""
    } else {
        match (syntax, field.label()) {
            (_, Label::Repeated) => "repeated ",
            ("proto2", Label::Required) => "required ",
            ("proto2", Label::Optional) => "optional ",
            _ => "",
        }
    };
    let ty = map_entry(parent, field).map_or_else(
        || proto_type_name(field, package),
        |entry| {
            let key = entry.field.first().map_or_else(
                || "unknown".to_string(),
                |field| proto_type_name(field, package),
            );
            let value = entry.field.get(1).map_or_else(
                || "unknown".to_string(),
                |field| proto_type_name(field, package),
            );
            format!("map<{key}, {value}>")
        },
    );
    let _ = writeln!(
        out,
        "{indent}{label}{ty} {} = {};",
        field.name.as_deref().unwrap_or("unknown"),
        field.number.unwrap_or_default()
    );
}

fn map_entry<'a>(
    parent: &'a DescriptorProto,
    field: &FieldDescriptorProto,
) -> Option<&'a DescriptorProto> {
    let name = field.type_name.as_deref()?.rsplit('.').next()?;
    parent.nested_type.iter().find(|nested| {
        nested.name.as_deref() == Some(name)
            && nested
                .options
                .as_ref()
                .is_some_and(prost_reflect::prost_types::MessageOptions::map_entry)
    })
}

fn proto_type_name(field: &FieldDescriptorProto, package: &str) -> String {
    if let Some(name) = field.type_name.as_deref().filter(|name| !name.is_empty()) {
        return proto_ref_name(name, package);
    }
    match field.r#type() {
        FieldType::Double => "double",
        FieldType::Float => "float",
        FieldType::Int64 => "int64",
        FieldType::Uint64 => "uint64",
        FieldType::Int32 => "int32",
        FieldType::Fixed64 => "fixed64",
        FieldType::Fixed32 => "fixed32",
        FieldType::Bool => "bool",
        FieldType::String => "string",
        FieldType::Bytes => "bytes",
        FieldType::Uint32 => "uint32",
        FieldType::Sfixed32 => "sfixed32",
        FieldType::Sfixed64 => "sfixed64",
        FieldType::Sint32 => "sint32",
        FieldType::Sint64 => "sint64",
        FieldType::Group | FieldType::Message | FieldType::Enum => "unknown",
    }
    .to_string()
}

fn proto_ref_name(name: &str, package: &str) -> String {
    if !package.is_empty()
        && let Some(local) = name.strip_prefix(&format!(".{package}."))
    {
        return local.to_string();
    }
    name.trim_start_matches('.').to_string()
}

fn write_service(out: &mut String, service: &ServiceDescriptorProto, package: &str) {
    let _ = writeln!(
        out,
        "service {} {{",
        service.name.as_deref().unwrap_or("Unknown")
    );
    for method in &service.method {
        let input_prefix = if method.client_streaming.unwrap_or(false) {
            "stream "
        } else {
            ""
        };
        let output_prefix = if method.server_streaming.unwrap_or(false) {
            "stream "
        } else {
            ""
        };
        let input = proto_ref_name(method.input_type.as_deref().unwrap_or("Unknown"), package);
        let output = proto_ref_name(method.output_type.as_deref().unwrap_or("Unknown"), package);
        let _ = writeln!(
            out,
            "  rpc {} ({input_prefix}{input}) returns ({output_prefix}{output});",
            method.name.as_deref().unwrap_or("Unknown")
        );
    }
    let _ = writeln!(out, "}}");
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use prost_reflect::prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto};

    use super::{ProtobufSerde, message_index, message_index_in, normalize};
    use crate::format::SchemaDeserializer;

    #[test]
    fn renders_minimal_proto_text() {
        let file = FileDescriptorProto {
            package: Some("demo".into()),
            message_type: vec![DescriptorProto {
                name: Some("Order".into()),
                field: vec![FieldDescriptorProto {
                    name: Some("id".into()),
                    number: Some(1),
                    type_name: Some(".string".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let text = normalize(&file);
        check!(
            (
                text.contains("package demo;"),
                text.contains("message Order {"),
                text.contains("id = 1;"),
            ) == (true, true, true)
        );
    }

    #[test]
    fn complex_descriptor_round_trips_and_nested_index_is_exact() {
        let source = r#"
            syntax = "proto3";
            package demo;
            message Outer {
              enum Kind { KIND_UNSPECIFIED = 0; KIND_READY = 1; }
              message Inner { string name = 1; }
              repeated Inner items = 1;
              Kind kind = 2;
              oneof choice { string text = 3; int64 number = 4; }
              map<string, int32> counts = 5;
            }
        "#;
        let file = protox_parse::parse("complex.proto", source).unwrap();
        let rendered = normalize(&file);
        check!(
            protox_parse::parse("complex.proto", &rendered).is_ok(),
            "{rendered}"
        );
        check!(rendered.contains("repeated Inner items = 1;"));
        check!(rendered.contains("oneof choice"));
        check!(rendered.contains("map<string, int32> counts = 5;"));

        let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(
            prost_reflect::prost_types::FileDescriptorSet { file: vec![file] },
        )
        .unwrap();
        let inner = pool.get_message_by_name("demo.Outer.Inner").unwrap();
        check!(message_index(&inner).unwrap() == vec![0, 0]);
        check!(message_index_in(inner.parent_file().messages(), "demo.Missing").is_none());
    }

    #[test]
    fn renders_scalar_field_types_as_proto3_keywords() {
        // Real prost descriptors set `type` (not `type_name`) for scalars; the
        // rendered `.proto` must name the type or the registry can't parse it.
        // Exercise every proto3 scalar keyword (one field per `field_type` arm),
        // plus the message-typed (`type_name`) branch.
        use prost_reflect::prost_types::field_descriptor_proto::Type;
        let scalars = [
            (Type::Double, "double"),
            (Type::Float, "float"),
            (Type::Int64, "int64"),
            (Type::Uint64, "uint64"),
            (Type::Int32, "int32"),
            (Type::Fixed64, "fixed64"),
            (Type::Fixed32, "fixed32"),
            (Type::Bool, "bool"),
            (Type::String, "string"),
            (Type::Bytes, "bytes"),
            (Type::Uint32, "uint32"),
            (Type::Sfixed32, "sfixed32"),
            (Type::Sfixed64, "sfixed64"),
            (Type::Sint32, "sint32"),
            (Type::Sint64, "sint64"),
        ];
        let mut field = Vec::new();
        for (i, (ty, kw)) in scalars.iter().enumerate() {
            field.push(FieldDescriptorProto {
                name: Some(format!("f_{kw}")),
                number: Some(i32::try_from(i).unwrap() + 1),
                r#type: Some(*ty as i32),
                ..Default::default()
            });
        }
        // Message-typed field: the renderer takes the `type_name` branch and
        // strips the leading dot.
        field.push(FieldDescriptorProto {
            name: Some("nested".into()),
            number: Some(100),
            type_name: Some(".demo.Other".into()),
            ..Default::default()
        });
        let file = FileDescriptorProto {
            package: Some("demo".into()),
            message_type: vec![DescriptorProto {
                name: Some("AllScalars".into()),
                field,
                ..Default::default()
            }],
            ..Default::default()
        };
        let text = normalize(&file);
        for (i, (_, kw)) in scalars.iter().enumerate() {
            check!(text.contains(&format!("{kw} f_{kw} = {};", i + 1)));
        }
        check!(text.contains("Other nested = 100;"));
    }

    #[test]
    fn message_type_metadata_mismatch_rejects_typed_decode() {
        use bytes::{Buf, BufMut};
        use prost::{
            DecodeError, Message,
            encoding::{DecodeContext, WireType},
        };
        use prost_reflect::{
            DescriptorPool, MessageDescriptor, ReflectMessage,
            prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet},
        };

        use crate::{
            cache::{CacheConfig, SchemaCache},
            registry::RegistryClient,
            wire,
        };

        #[derive(Clone, Debug, Default, PartialEq, Eq)]
        struct TestOrder;

        impl Message for TestOrder {
            fn encode_raw(&self, _buf: &mut impl BufMut) {}

            fn merge_field(
                &mut self,
                _tag: u32,
                _wire_type: WireType,
                _buf: &mut impl Buf,
                _ctx: DecodeContext,
            ) -> Result<(), DecodeError> {
                Ok(())
            }

            fn encoded_len(&self) -> usize {
                0
            }

            fn clear(&mut self) {}
        }

        impl ReflectMessage for TestOrder {
            fn descriptor(&self) -> MessageDescriptor {
                static POOL: std::sync::OnceLock<DescriptorPool> = std::sync::OnceLock::new();
                POOL.get_or_init(|| {
                    DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
                        file: vec![FileDescriptorProto {
                            name: Some("demo.proto".into()),
                            package: Some("demo".into()),
                            message_type: vec![DescriptorProto {
                                name: Some("Order".into()),
                                ..Default::default()
                            }],
                            syntax: Some("proto3".into()),
                            ..Default::default()
                        }],
                    })
                    .unwrap()
                })
                .get_message_by_name("demo.Order")
                .unwrap()
            }
        }

        let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
        cache.seed_subject_id("orders-value", 11);
        cache.seed_writer_message_type(11, "demo.Other");
        let serde = ProtobufSerde::<TestOrder>::value(&cache);
        let frame = wire::encode_protobuf(11, &[0], &[]);

        let err = serde.deserialize("orders", &frame).unwrap_err();
        assert2::assert!(err.to_string().contains("messageType"));
    }
}
