//! The Kafka wire drivers: `CreateTopics`, `Produce` and `Fetch` against the
//! cluster, and the Confluent framing of a record value.

use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use krabka_broker::BrokerHandle;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{
            CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig, CreateTopicsRequest,
        },
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::PartitionProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};

/// Kafka error 87. KIP-467 added it for "one or more records in the batch were
/// invalid", which is what a schema rejection is.
pub const INVALID_RECORD: i16 = 87;

/// Frame a body the way every Confluent serializer does:
/// `0x00 | schema_id(4 BE) | body`.
pub fn framed(id: u32, body: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(0x00);
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(body);
    Bytes::from(out)
}

/// The schema id in a Confluent-framed payload.
pub fn schema_id(payload: &[u8]) -> u32 {
    u32::from_be_bytes(payload[1..5].try_into().unwrap())
}

/// One Avro datum of the `Order` record, `{"id": "a"}`, hand-encoded: a
/// `string` is a zig-zag varint length then the bytes, and `1` zig-zag encodes
/// to `0x02`.
pub fn order_avro_body() -> Vec<u8> {
    vec![0x02, b'a']
}

/// Connect a client that bootstraps from `bootstrap`.
pub async fn client(bootstrap: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap)
        .client_id("live-registry")
        .build()
        .await
        .unwrap()
}

/// A connected client for the broker `node`.
pub async fn client_for(node: &BrokerHandle) -> Client {
    client(&node.listen_addr().to_string()).await
}

/// Create `name` with one partition and `replication_factor` replicas.
pub async fn create_topic_rf(
    broker: &BrokerHandle,
    client: &Client,
    name: &str,
    configs: &[(&str, &str)],
    replication_factor: i16,
) -> WireUuid {
    let topic = CreatableTopic {
        name: name.into(),
        num_partitions: 1,
        replication_factor,
        ..Default::default()
    };
    create_topic_from(broker, client, topic, configs).await
}

/// Create `name` with one partition on `replicas`, in that order, so the first
/// one leads. An automatic placement starts at a random broker, and a test
/// that stops the leader needs to know which broker that is.
pub async fn create_topic_on(
    broker: &BrokerHandle,
    client: &Client,
    name: &str,
    configs: &[(&str, &str)],
    replicas: &[i32],
) -> WireUuid {
    let topic = CreatableTopic {
        name: name.into(),
        num_partitions: -1,
        replication_factor: -1,
        assignments: vec![CreatableReplicaAssignment {
            partition_index: 0,
            broker_ids: replicas.to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    };
    create_topic_from(broker, client, topic, configs).await
}

/// Send `CreateTopics`, wait until `broker` has partition 0, and answer the
/// topic id.
///
/// The id comes from the response, which the broker sends only after the topic
/// record commits. A `Metadata` read is not safe here: `client` can bootstrap
/// to a broker that is still one fetch behind the commit, which answers the
/// topic as unknown with a zero id.
async fn create_topic_from(
    broker: &BrokerHandle,
    client: &Client,
    topic: CreatableTopic,
    configs: &[(&str, &str)],
) -> WireUuid {
    let name = topic.name.clone();
    let configs = configs
        .iter()
        .map(|&(name, value)| CreatableTopicConfig {
            name: name.into(),
            value: Some(value.into()),
            ..Default::default()
        })
        .collect();
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic { configs, ..topic }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    let created = &response.topics[0];
    assert!(
        created.error_code == 0,
        "create {name}: {:?}",
        created.error_message
    );
    assert!(
        created.topic_id != WireUuid::ZERO,
        "create {name} answered no topic id"
    );
    broker.wait_until_partition_present(&name, 0).await;
    created.topic_id
}

/// Produce one record with `value` to partition 0 of `topic`, and answer the
/// whole partition response.
pub async fn produce_value(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    value: Option<Bytes>,
) -> PartitionProduceResponse {
    let batch = RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 12_345,
        producer_id: -1,
        records: vec![Record {
            offset_delta: 0,
            value,
            ..Default::default()
        }],
        ..RecordBatch::default()
    };
    let response = client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(RecordsPayload::V2(vec![batch])),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    response.responses[0].partition_responses[0].clone()
}

/// Like [`produce_value`], but retry while the partition is not yet
/// produceable: `UNKNOWN_TOPIC_OR_PARTITION` (3), `NOT_LEADER_OR_FOLLOWER`
/// (6) and `UNKNOWN_TOPIC_ID` (100), for up to 30 seconds.
pub async fn produce_when_ready(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    value: Option<Bytes>,
) -> PartitionProduceResponse {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let response = produce_value(client, topic, topic_id, value.clone()).await;
        if !matches!(response.error_code, 3 | 6 | 100) {
            return response;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{topic} did not become produceable: {response:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait until partition 0 of `topic` has a high watermark of `count`, then
/// fetch it from offset 0 through `broker` and answer every record value.
pub async fn fetch_values(
    broker: &BrokerHandle,
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    count: i64,
) -> Vec<Option<Bytes>> {
    broker.wait_until_high_watermark(topic, 0, count).await;
    let response = client
        .send(FetchRequest {
            replica_id: -1,
            max_wait_ms: 1_000,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: topic.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let partition = &response.responses[0].partitions[0];
    assert!(partition.error_code == 0, "fetch failed: {partition:?}");
    partition
        .records
        .as_ref()
        .unwrap()
        .as_v2()
        .unwrap()
        .iter()
        .flat_map(|batch| batch.records.iter().map(|record| record.value.clone()))
        .collect()
}
