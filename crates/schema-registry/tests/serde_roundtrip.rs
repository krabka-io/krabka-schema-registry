use std::sync::OnceLock;

use apache_avro::AvroSchema;
use krabka_broker::{Broker, BrokerConfig};
use krabka_schema_registry::{
    config::{RegistryConfig, RegistryRuntimeConfig, SecurityConfig},
    kafkastore::KafkaStore,
    rest::{self, AppState},
};
use krabka_schema_serde::{
    AvroSerde, CacheConfig, JsonSerde, ProtobufSerde, RegistryClient, SchemaCache,
    format::{SchemaDeserializer, SchemaSerializer, SchemaSubject},
};
use prost::Message;
use prost_reflect::{
    DescriptorPool, MessageDescriptor, ReflectMessage,
    prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto::{Label, Type},
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AvroSchema, JsonSchema)]
struct Order {
    id: String,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoOrder {
    #[prost(string, tag = "1")]
    id: String,
}

impl ReflectMessage for ProtoOrder {
    fn descriptor(&self) -> MessageDescriptor {
        static POOL: OnceLock<DescriptorPool> = OnceLock::new();
        POOL.get_or_init(|| {
            DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
                file: vec![FileDescriptorProto {
                    name: Some("order.proto".into()),
                    package: Some("fixture".into()),
                    syntax: Some("proto3".into()),
                    message_type: vec![DescriptorProto {
                        name: Some("Order".into()),
                        field: vec![FieldDescriptorProto {
                            name: Some("id".into()),
                            number: Some(1),
                            label: Some(Label::Optional as i32),
                            r#type: Some(Type::String as i32),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            })
            .unwrap()
        })
        .get_message_by_name("fixture.Order")
        .unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_formats_round_trip_through_in_process_registry() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let config = RegistryConfig {
        bootstrap: broker.listen_addr().to_string(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: "serde-roundtrip".into(),
        advertised_url: "http://127.0.0.1:0".into(),
        group_id: "serde-roundtrip".into(),
        leader_eligibility: true,
        runtime: RegistryRuntimeConfig::default(),
        security: SecurityConfig::default(),
    };
    let store = KafkaStore::start(&config, cancel.clone()).await.unwrap();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let serve_cancel = cancel.clone();
    tokio::spawn(async move {
        axum::serve(listener, rest::router(AppState { store }))
            .with_graceful_shutdown(async move { serve_cancel.cancelled().await })
            .await
            .unwrap();
    });

    let cache = SchemaCache::new(RegistryClient::new(base), CacheConfig::default());
    let avro = AvroSerde::<Order>::value(&cache);
    let json = JsonSerde::<Order>::value(&cache, true);
    let protobuf = ProtobufSerde::<ProtoOrder>::value(&cache);
    avro.register_subject("avro-orders");
    json.register_subject("json-orders");
    protobuf.register_subject("protobuf-orders");
    cache.prewarm().await.unwrap();

    let order = Order { id: "o-1".into() };
    let avro_bytes = avro.serialize("avro-orders", &order).unwrap();
    let json_bytes = json.serialize("json-orders", &order).unwrap();
    let proto_order = ProtoOrder { id: "o-1".into() };
    let protobuf_bytes = protobuf.serialize("protobuf-orders", &proto_order).unwrap();
    assert2::check!(avro.deserialize("avro-orders", &avro_bytes).unwrap() == order);
    assert2::check!(json.deserialize("json-orders", &json_bytes).unwrap() == order);
    assert2::check!(
        protobuf
            .deserialize("protobuf-orders", &protobuf_bytes)
            .unwrap()
            == proto_order
    );

    cancel.cancel();
    broker.shutdown().await;
}
