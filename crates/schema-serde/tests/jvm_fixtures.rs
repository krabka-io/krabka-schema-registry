use std::{collections::HashMap, sync::OnceLock};

use apache_avro::AvroSchema;
use krabka_schema_serde::{
    AvroSerde, CacheConfig, JsonSerde, ProtobufSerde, RegistryClient, SchemaCache,
    format::SchemaSerializer, wire,
};
use prost::{Enumeration, Message};
use prost_reflect::{
    DescriptorPool, MessageDescriptor, ReflectMessage,
    prost_types::{
        DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
        FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto::{Label, Type},
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, AvroSchema, JsonSchema)]
struct Order {
    id: String,
}

#[derive(Clone, PartialEq, Message)]
struct Flat {
    #[prost(string, tag = "1")]
    id: String,
}

#[derive(Clone, PartialEq, Message)]
struct Inner {
    #[prost(string, tag = "1")]
    id: String,
}

#[derive(Clone, PartialEq, Message)]
struct Event {
    #[prost(string, repeated, tag = "1")]
    tags: Vec<String>,
    #[prost(enumeration = "Kind", tag = "2")]
    kind: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
enum Kind {
    Unknown = 0,
    Ready = 1,
}

fn field(name: &str, number: i32, r#type: Type, label: Label) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(r#type as i32),
        ..Default::default()
    }
}

fn descriptors() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        let mut kind = field("kind", 2, Type::Enum, Label::Optional);
        kind.type_name = Some(".fixture.Event.Kind".into());
        DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("fixture.proto".into()),
                package: Some("fixture".into()),
                syntax: Some("proto3".into()),
                message_type: vec![
                    DescriptorProto {
                        name: Some("Flat".into()),
                        field: vec![field("id", 1, Type::String, Label::Optional)],
                        ..Default::default()
                    },
                    DescriptorProto {
                        name: Some("Outer".into()),
                        nested_type: vec![DescriptorProto {
                            name: Some("Inner".into()),
                            field: vec![field("id", 1, Type::String, Label::Optional)],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    DescriptorProto {
                        name: Some("Event".into()),
                        field: vec![field("tags", 1, Type::String, Label::Repeated), kind],
                        enum_type: vec![EnumDescriptorProto {
                            name: Some("Kind".into()),
                            value: vec![
                                EnumValueDescriptorProto {
                                    name: Some("UNKNOWN".into()),
                                    number: Some(0),
                                    ..Default::default()
                                },
                                EnumValueDescriptorProto {
                                    name: Some("READY".into()),
                                    number: Some(1),
                                    ..Default::default()
                                },
                            ],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
        })
        .unwrap()
    })
}

macro_rules! reflect_message {
    ($type:ty, $name:literal) => {
        impl ReflectMessage for $type {
            fn descriptor(&self) -> MessageDescriptor {
                descriptors().get_message_by_name($name).unwrap()
            }
        }
    };
}

reflect_message!(Flat, "fixture.Flat");
reflect_message!(Inner, "fixture.Outer.Inner");
reflect_message!(Event, "fixture.Event");

fn frames() -> HashMap<&'static str, Vec<u8>> {
    include_str!("fixtures/jvm-serde/frames.txt")
        .lines()
        .map(|line| {
            let (name, hex) = line.split_once('=').unwrap();
            let bytes = hex
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            (name, bytes)
        })
        .collect()
}

#[test]
fn serializers_match_cp_schema_registry_7_4_0() {
    let frames = frames();
    assert2::check!(frames.len() == 5);
    let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
    for (subject, id) in [
        ("avro-orders-value", 1),
        ("json-orders-value", 2),
        ("protobuf-flat-value", 3),
        ("protobuf-nested-value", 3),
        ("protobuf-event-value", 3),
    ] {
        cache.seed_subject_id(subject, id);
    }

    let order = Order { id: "o-1".into() };
    assert2::check!(
        AvroSerde::<Order>::value(&cache)
            .serialize("avro-orders", &order)
            .unwrap()
            .as_ref()
            == frames["avro"]
    );
    assert2::check!(
        JsonSerde::<Order>::value(&cache, false)
            .serialize("json-orders", &order)
            .unwrap()
            .as_ref()
            == frames["json"]
    );
    assert2::check!(
        ProtobufSerde::<Flat>::value(&cache)
            .serialize("protobuf-flat", &Flat { id: "o-1".into() })
            .unwrap()
            .as_ref()
            == frames["protobuf_flat"]
    );
    assert2::check!(
        ProtobufSerde::<Inner>::value(&cache)
            .serialize("protobuf-nested", &Inner { id: "o-1".into() })
            .unwrap()
            .as_ref()
            == frames["protobuf_nested"]
    );
    assert2::check!(
        ProtobufSerde::<Event>::value(&cache)
            .serialize(
                "protobuf-event",
                &Event {
                    tags: vec!["one".into(), "two".into()],
                    kind: Kind::Ready as i32,
                },
            )
            .unwrap()
            .as_ref()
            == frames["protobuf_repeated_enum"]
    );
}

#[test]
fn captured_protobuf_indices_cover_optimized_and_nested_forms() {
    let frames = frames();
    let (avro_id, avro_body) = wire::decode(&frames["avro"]).unwrap();
    assert2::check!(wire::encode(avro_id, avro_body).as_ref() == frames["avro"]);
    let (flat_id, flat, flat_body) = wire::decode_protobuf(&frames["protobuf_flat"]).unwrap();
    let (nested_id, nested, nested_body) =
        wire::decode_protobuf(&frames["protobuf_nested"]).unwrap();
    assert2::check!(flat == vec![0]);
    assert2::check!(nested == vec![1, 0]);
    assert2::check!(
        wire::encode_protobuf(flat_id, &flat, flat_body).as_ref() == frames["protobuf_flat"]
    );
    assert2::check!(
        wire::encode_protobuf(nested_id, &nested, nested_body).as_ref()
            == frames["protobuf_nested"]
    );
}
