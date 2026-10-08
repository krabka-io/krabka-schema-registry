//! The broker's schema gate against this registry, end to end.
//!
//! Boots a three-broker in-process Krabka cluster whose KFC-7 schema validator
//! points, fail-closed and over HTTP Basic auth, at two in-process registry
//! nodes. The registries keep `_schemas` on that same cluster with replication
//! factor 3. It then drives the real Kafka wire path and asserts:
//!
//! - schemas registered through a SECONDARY are forwarded to the primary and
//!   replicated, for Avro, JSON Schema, Protobuf and an Avro schema with a
//!   reference to another subject,
//! - the broker accepts a value framed with each of those ids on a topic with
//!   `full` validation, and a tombstone, and an unframed value on a topic
//!   without validation, and every accepted value fetches back intact,
//! - the broker rejects bad framing, an unknown id, an id bound to another
//!   subject, and a body that does not match its schema, with
//!   `INVALID_RECORD`, and none of those moves the leader's log end offset,
//! - after the registry primary loses its election session, a schema
//!   registered through the successor is served through the URL the brokers
//!   kept, and the earlier registrations survive,
//! - after the partition leader crashes, the elected successor validates
//!   against the evolved schema,
//! - with every registry stopped, a fresh id is rejected and the log end
//!   offset does not move.
//!
//! The broker repository's own suite asserts the gate's decisions against a
//! mock registry. This one proves the gate and this registry agree, through
//! a registry failover and a broker failover.

mod broker_support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `live_registry/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "live_registry/cluster.rs"]
mod cluster;
#[path = "live_registry/payloads.rs"]
mod payloads;
#[path = "live_registry/registry.rs"]
mod registry;
#[path = "live_registry/wire.rs"]
mod wire;

use assert2::check;
use bytes::Bytes;
use krabka_broker::NodeId;
use krabka_protocol::primitives::uuid::Uuid as WireUuid;
use tokio::net::TcpListener;

use crate::{
    cluster::Node,
    payloads::Payloads,
    registry::RegistryNode,
    wire::{INVALID_RECORD, client, client_for, framed, order_avro_body},
};

/// The ids of the topics the suite creates.
struct Topics {
    /// Led by node 1, the broker the suite crashes.
    avro: WireUuid,
    json: WireUuid,
    protobuf: WireUuid,
    referenced: WireUuid,
    /// No schema validation.
    control: WireUuid,
}

/// A `RUST_LOG`-driven subscriber, so a failing run can be traced.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Create the validated topics and the control topic, all with replication
/// factor 3.
async fn create_topics(nodes: &[Node], bootstrap: &str) -> Topics {
    let admin = client(bootstrap).await;
    let broker = &nodes[0].broker;
    let validation = &[
        ("schema.validation.value", "true"),
        ("schema.validation.mode", "full"),
    ];
    // Node 1 leads `avro`, and the test stops the leader later on: the
    // registries' seed broker, node 2, must stay up to follow the failover. An
    // automatic placement would pick a random leader.
    Topics {
        avro: wire::create_topic_on(broker, &admin, "avro", validation, &[1, 2, 3]).await,
        json: wire::create_topic_rf(broker, &admin, "json", validation, 3).await,
        protobuf: wire::create_topic_rf(broker, &admin, "protobuf", validation, 3).await,
        referenced: wire::create_topic_rf(broker, &admin, "referenced", validation, 3).await,
        control: wire::create_topic_rf(broker, &admin, "control", &[], 3).await,
    }
}

/// Produce one valid value to each validated topic, then a warm-cache repeat
/// and a tombstone to `avro` and an unframed value to `control`, and check
/// that every accepted value fetches back intact.
async fn accept_valid_records(nodes: &[Node], topics: &Topics, payloads: &Payloads) {
    let observer = &nodes[0].broker;
    for (topic, topic_id, value) in [
        ("avro", topics.avro, &payloads.avro),
        ("json", topics.json, &payloads.json),
        ("protobuf", topics.protobuf, &payloads.protobuf),
        ("referenced", topics.referenced, &payloads.referenced),
    ] {
        let leader = client_for(&cluster::leader_of(nodes, observer, topic).broker).await;
        let response =
            wire::produce_when_ready(&leader, topic, topic_id, Some(value.clone())).await;
        check!(response.error_code == 0, "{topic}: {response:?}");
    }

    // Warm-cache acceptance and every required rejection keep the leader LEO
    // exact; no error response is allowed to hide an append.
    let avro_leader = &cluster::leader_of(nodes, observer, "avro").broker;
    let avro_client = client_for(avro_leader).await;
    let warm = wire::produce_when_ready(
        &avro_client,
        "avro",
        topics.avro,
        Some(payloads.avro.clone()),
    )
    .await;
    check!(warm.error_code == 0, "{warm:?}");
    let control_leader = &cluster::leader_of(nodes, observer, "control").broker;
    let control_client = client_for(control_leader).await;
    let unframed = Bytes::from_static(b"unframed-control");
    let control = wire::produce_when_ready(
        &control_client,
        "control",
        topics.control,
        Some(unframed.clone()),
    )
    .await;
    check!(control.error_code == 0, "{control:?}");
    let tombstone = wire::produce_when_ready(&avro_client, "avro", topics.avro, None).await;
    check!(tombstone.error_code == 0, "{tombstone:?}");

    let referenced_leader = &cluster::leader_of(nodes, observer, "referenced").broker;
    let referenced_client = client_for(referenced_leader).await;
    check!(
        wire::fetch_values(
            referenced_leader,
            &referenced_client,
            "referenced",
            topics.referenced,
            1
        )
        .await
            == vec![Some(payloads.referenced.clone())]
    );
    check!(
        wire::fetch_values(avro_leader, &avro_client, "avro", topics.avro, 3).await
            == vec![
                Some(payloads.avro.clone()),
                Some(payloads.avro.clone()),
                None
            ]
    );
    check!(
        wire::fetch_values(
            control_leader,
            &control_client,
            "control",
            topics.control,
            1
        )
        .await
            == vec![Some(unframed)]
    );
}

/// Produce each kind of invalid value to `avro` through its leader, and check
/// that each is rejected with `INVALID_RECORD` and leaves the log end where it
/// was.
async fn reject_invalid_records(leader: &Node, topics: &Topics, payloads: &Payloads) {
    let leader_client = client_for(&leader.broker).await;
    for (case, invalid) in [
        ("framing", Bytes::from_static(b"not-confluent-framing")),
        ("unknown id", framed(u32::MAX, &order_avro_body())),
        (
            "wrong subject",
            framed(payloads.wrong_subject_id, &order_avro_body()),
        ),
        (
            "body mismatch",
            framed(payloads.avro_id, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        ),
    ] {
        let before = leader.broker.local_log_end_offset("avro", 0).unwrap();
        let response =
            wire::produce_value(&leader_client, "avro", topics.avro, Some(invalid)).await;
        check!(
            response.error_code == INVALID_RECORD,
            "{case}: {response:?}"
        );
        check!(
            leader.broker.local_log_end_offset("avro", 0) == Some(before),
            "{case} moved the log end"
        );
    }
}

/// Remove the registry primary from its election session, register an
/// evolved `avro-value` schema through the successor, and check that node 0 —
/// the URL the brokers have kept throughout — serves it and still serves the
/// earlier referenced registration. Answers the evolved schema's id.
async fn fail_over_registry(
    registries: &mut [RegistryNode],
    primary: usize,
    http: &reqwest::Client,
    referenced_id: u32,
) -> u32 {
    registries[primary].election_cancel.cancel();
    let successor = 1 - primary;
    registry::wait_until_primary(&mut registries[successor]).await;
    let evolved_id = registry::register(
        http,
        &registries[successor].url,
        "avro-value",
        serde_json::json!({
            "schema": r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"note","type":["null","string"],"default":null}]}"#
        }),
    )
    .await;
    registry::wait_for_schema(http, &registries[0].url, evolved_id).await;
    let preserved = registry::get_json(
        http,
        &format!("{}/subjects/referenced-value/versions/1", registries[0].url),
    )
    .await;
    check!(preserved["id"] == u64::from(referenced_id));
    check!(preserved["version"] == 1);
    check!(preserved["references"][0]["subject"] == "order-base");
    evolved_id
}

/// Crash the `avro` leader, produce a value framed with the evolved schema's
/// id through the elected successor, then stop every registry and check that
/// a fresh id is rejected without moving the successor's log end. Answers the
/// crashed node's log directory, which the caller keeps until the end.
async fn fail_over_broker(
    nodes: &mut Vec<Node>,
    leader_index: usize,
    avro: WireUuid,
    evolved_id: u32,
    registries: Vec<RegistryNode>,
) -> tempfile::TempDir {
    let victim = nodes.remove(leader_index);
    let victim_id = victim.broker.node_id();
    victim.broker.crash_for_test().await;
    let observer = &nodes[0].broker;
    observer
        .wait_until_partition_leader_changed("avro", 0, NodeId(victim_id))
        .await;
    let new_leader = &cluster::leader_of(nodes, observer, "avro").broker;
    let failover_client = client_for(new_leader).await;
    let response = wire::produce_when_ready(
        &failover_client,
        "avro",
        avro,
        Some(framed(evolved_id, &[0x02, b'b', 0x00])),
    )
    .await;
    check!(response.error_code == 0, "{response:?}");
    new_leader
        .wait_until_local_log_end_offset("avro", 0, 4)
        .await;

    // With every registry endpoint gone, fail-closed rejects a fresh id and
    // still leaves the post-failover leader's LEO unchanged.
    for registry in registries {
        registry.stop().await;
    }
    let before = new_leader.local_log_end_offset("avro", 0).unwrap();
    let unavailable = wire::produce_value(
        &failover_client,
        "avro",
        avro,
        Some(framed(evolved_id + 10_000, &order_avro_body())),
    )
    .await;
    check!(unavailable.error_code == INVALID_RECORD, "{unavailable:?}");
    check!(new_leader.local_log_end_offset("avro", 0) == Some(before));
    victim.dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn rf_three_validation_survives_registry_and_broker_failover() {
    init_tracing();

    let registry_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let registry_url = format!("http://{}", registry_listener.local_addr().unwrap());
    let mut nodes = cluster::start(&registry_url).await;
    // The second broker is neither the bootstrap controller nor the first
    // replica selected for the registry log, so registry clients must follow
    // metadata rather than assuming their seed is the leader.
    let bootstrap = nodes[1].broker.listen_addr().to_string();

    let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut registries = vec![
        registry::start(&bootstrap, 1, registry_listener).await,
        registry::start(&bootstrap, 2, second_listener).await,
    ];
    check!(registries[0].url == registry_url);
    let primary = registry::wait_for_primary(&registries).await;

    // Register through the secondary so the first write also proves forwarding
    // to the elected primary and replication through the RF=3 `_schemas` log.
    let http = registry::authenticated_http();
    let payloads = payloads::register_all(&http, &registries[1 - primary].url).await;

    let topics = create_topics(&nodes, &bootstrap).await;
    accept_valid_records(&nodes, &topics, &payloads).await;
    let leader_id = nodes[0]
        .broker
        .partition_leader_for_test("avro", 0)
        .expect("avro leader");
    let leader_index = nodes
        .iter()
        .position(|node| node.broker.node_id() == leader_id)
        .unwrap();
    reject_invalid_records(&nodes[leader_index], &topics, &payloads).await;
    for node in &nodes {
        node.broker
            .wait_until_local_log_end_offset("avro", 0, 3)
            .await;
    }

    let evolved_id =
        fail_over_registry(&mut registries, primary, &http, payloads.referenced_id).await;
    let victim_dir = fail_over_broker(
        &mut nodes,
        leader_index,
        topics.avro,
        evolved_id,
        registries,
    )
    .await;

    cluster::shutdown(nodes).await;
    drop(victim_dir);
}
