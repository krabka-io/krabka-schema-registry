//! The record types the suite serializes, and the schemas it registers for
//! them through the typed Confluent serdes and the REST API.

use std::{sync::OnceLock, time::Duration};

use apache_avro::{AvroSchema, Schema, to_value, writer::datum::GenericDatumWriter};
use assert2::{assert, check};
use bytes::{Buf, BufMut, Bytes};
use krabka_schema_serde::{
    AvroSerde, CacheConfig, JsonSerde, ProtobufSerde, RegistryClient, SchemaCache,
    format::{SchemaSerializer, SchemaSubject},
};
use prost::{
    DecodeError, Message,
    encoding::{DecodeContext, WireType},
};
use prost_reflect::{
    DescriptorPool, MessageDescriptor, ReflectMessage,
    prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    registry::{get_json, register},
    wire::{framed, schema_id},
};

/// The `Base` record that `Envelope` references from another subject.
const BASE_AVRO: &str =
    r#"{"type":"record","name":"Base","fields":[{"name":"id","type":"string"}]}"#;
/// A record whose `base` field is the referenced `Base` type.
const ENVELOPE_AVRO: &str =
    r#"{"type":"record","name":"Envelope","fields":[{"name":"base","type":"Base"}]}"#;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AvroSchema)]
struct Order {
    id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
struct JsonOrder {
    id: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct Base {
    id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct Envelope {
    base: Base,
}

/// An empty proto3 `live.Order` message, with a descriptor built by hand so
/// the suite needs no `.proto` compilation step.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ProtoOrder;

impl Message for ProtoOrder {
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

impl ReflectMessage for ProtoOrder {
    fn descriptor(&self) -> MessageDescriptor {
        static POOL: OnceLock<DescriptorPool> = OnceLock::new();
        POOL.get_or_init(|| {
            DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
                file: vec![FileDescriptorProto {
                    name: Some("order.proto".into()),
                    package: Some("live".into()),
                    syntax: Some("proto3".into()),
                    message_type: vec![DescriptorProto {
                        name: Some("Order".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            })
            .unwrap()
        })
        .get_message_by_name("live.Order")
        .unwrap()
    }
}

/// The framed values and schema ids the suite produces with.
pub struct Payloads {
    /// An `Order` on the `avro-value` subject.
    pub avro: Bytes,
    pub json: Bytes,
    pub protobuf: Bytes,
    /// An `Envelope` whose schema references `Base` in `order-base`.
    pub referenced: Bytes,
    /// The schema id in [`Payloads::avro`].
    pub avro_id: u32,
    /// A schema bound only to `somewhere-else-value`.
    pub wrong_subject_id: u32,
    /// The schema id in [`Payloads::referenced`].
    pub referenced_id: u32,
}

/// Register every schema the suite uses through the node at `url`, and frame
/// one value of each.
///
/// The Avro, JSON Schema and Protobuf schemas are registered by the typed
/// serdes' prewarm, the way a producer application would register them.
pub async fn register_all(http: &reqwest::Client, url: &str) -> Payloads {
    let cache = SchemaCache::new(
        RegistryClient::with_http_client(url.to_owned(), http.clone()),
        CacheConfig::default(),
    );
    let avro = AvroSerde::<Order>::value(&cache);
    let json = JsonSerde::<JsonOrder>::value(&cache, true);
    let protobuf = ProtobufSerde::<ProtoOrder>::value(&cache);
    avro.register_subject("avro");
    json.register_subject("json");
    protobuf.register_subject("protobuf");
    let prewarm_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while let Err(error) = cache.prewarm().await {
        assert!(tokio::time::Instant::now() < prewarm_deadline, "{error}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let avro = avro.serialize("avro", &Order { id: "a".into() }).unwrap();
    let wrong_subject_id = register(
        http,
        url,
        "somewhere-else-value",
        serde_json::json!({
            "schema": r#"{"type":"record","name":"Elsewhere","fields":[{"name":"id","type":"string"}]}"#
        }),
    )
    .await;
    let (referenced_id, referenced) = register_referenced(http, url).await;
    Payloads {
        avro_id: schema_id(&avro),
        avro,
        json: json.serialize("json", &JsonOrder { id: 1 }).unwrap(),
        protobuf: protobuf.serialize("protobuf", &ProtoOrder).unwrap(),
        referenced,
        wrong_subject_id,
        referenced_id,
    }
}

/// Register `Base` and the `Envelope` that references it, and answer the
/// envelope's id and one framed envelope value.
async fn register_referenced(http: &reqwest::Client, url: &str) -> (u32, Bytes) {
    register(
        http,
        url,
        "order-base",
        serde_json::json!({ "schema": BASE_AVRO }),
    )
    .await;
    let id = register(
        http,
        url,
        "referenced-value",
        serde_json::json!({
            "schema": ENVELOPE_AVRO,
            "references": [{"name":"Base","subject":"order-base","version":1}]
        }),
    )
    .await;
    let registered = get_json(http, &format!("{url}/schemas/ids/{id}")).await;
    check!(registered["references"][0]["subject"] == "order-base");
    let schemas = Schema::parse_list([BASE_AVRO, ENVELOPE_AVRO]).unwrap();
    let body = GenericDatumWriter::builder(&schemas[1])
        .schemata(schemas.iter().collect())
        .unwrap()
        .build()
        .unwrap()
        .write_value_to_vec(
            to_value(Envelope {
                base: Base {
                    id: "referenced".into(),
                },
            })
            .unwrap(),
        )
        .unwrap();
    (id, framed(id, &body))
}
