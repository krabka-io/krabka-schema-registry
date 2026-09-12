use std::{sync::Arc, time::Duration};

use axum::Router;
use bytes::Bytes;
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_producer::{Acks, Producer, ProducerRecord};
use krabka_schema_registry::{
    config::{RegistryConfig, SecurityConfig},
    election::{Election, PrimaryState},
    format::SchemaType,
    ids::{SchemaId, SchemaVersion},
    kafkastore::{KafkaStore, RegisterSchema, record::SchemaReference},
    rest::{self, AppState, forward::ForwardState},
};
use tokio_util::sync::CancellationToken;

fn cfg(bootstrap: &str, port: i32) -> RegistryConfig {
    RegistryConfig {
        bootstrap: bootstrap.into(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: format!("sr-{port}"),
        advertised_url: format!("http://127.0.0.1:{port}"),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
        runtime: krabka_schema_registry::config::RegistryRuntimeConfig::default(),
        security: SecurityConfig::default(),
    }
}

/// Wait until `pred(state)` holds, or until `secs` elapses. Returns the
/// matching state.
async fn await_state(
    rx: &mut tokio::sync::watch::Receiver<PrimaryState>,
    secs: u64,
    pred: impl Fn(&PrimaryState) -> bool,
) -> PrimaryState {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if pred(&rx.borrow()) {
            return rx.borrow().clone();
        }
        tokio::select! {
            () = tokio::time::sleep_until(deadline) => panic!("state never matched: {:?}", *rx.borrow()),
            r = rx.changed() => { r.expect("election task alive"); }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_becomes_primary() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let c = cfg(&broker.listen_addr().to_string(), 8081);
    let mut rx = Election::start(&c, cancel.clone()).await.unwrap();
    let st = await_state(&mut rx, 20, |s| s.is_primary).await;
    assert2::assert!(st.is_primary);
    assert2::assert!(st.primary_url.as_deref() == Some("http://127.0.0.1:8081"));
    cancel.cancel();
    broker.shutdown().await;
}

struct Node {
    port: i32,
    store: Arc<KafkaStore>,
    primary: tokio::sync::watch::Receiver<PrimaryState>,
    election_cancel: CancellationToken,
    store_cancel: CancellationToken,
    cancel: CancellationToken,
}

/// Boot a full registry node with its store, forwarding router, and election on
/// an ephemeral `127.0.0.1` port. It binds the listener FIRST, so
/// `advertised_url` carries the real port and no fixed port can collide.
async fn start_node(bootstrap: &str) -> Node {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let c = cfg(bootstrap, port);
    let cancel = CancellationToken::new();
    let election_cancel = cancel.child_token();
    let store_cancel = cancel.child_token();
    let store = KafkaStore::start(&c, store_cancel.clone()).await.unwrap();
    let primary = Election::start(&c, election_cancel.clone()).await.unwrap();
    store.install_primary(primary.clone());
    let fwd = ForwardState {
        primary: primary.clone(),
        http: reqwest::Client::new(),
        node_id: c.advertised_url.clone(),
        forward_max_body: c.runtime.forward_max_body,
        forward_secret: None,
    };
    let app: Router = rest::router_with_forwarding(
        AppState {
            store: store.clone(),
        },
        fwd,
    );
    let serve_cancel = cancel.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { serve_cancel.cancelled().await })
            .await
            .ok();
    });
    Node {
        port,
        store,
        primary,
        election_cancel,
        store_cancel,
        cancel,
    }
}

/// Poll `GET url` until the body equals `expected`, or until `secs` elapses.
/// The non-writing node reflects a forwarded write only after it consumes the
/// `_schemas` record, so the view is eventually consistent and a single assert
/// would flake.
async fn await_get_body(http: &reqwest::Client, url: &str, expected: &str, secs: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if let Ok(r) = http.get(url).send().await
            && r.status() == 200
            && let Ok(b) = r.text().await
            && b == expected
        {
            return;
        }
        assert2::assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_node_elects_one_primary_forwards_writes_and_fails_over() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let mut a = start_node(&bootstrap).await;
    let mut b = start_node(&bootstrap).await;

    // Both nodes observe an elected primary, and it's exactly one of them.
    await_state(&mut a.primary, 25, |s| s.primary_url.is_some()).await;
    await_state(&mut b.primary, 25, |s| s.primary_url.is_some()).await;
    let a_is_primary = a.primary.borrow().is_primary;
    assert2::assert!(a_is_primary != b.primary.borrow().is_primary);
    let secondary_port = if a_is_primary { b.port } else { a.port };

    let http = reqwest::Client::new();
    let body = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[]}"}"#;

    // POST to the SECONDARY → middleware forwards it to the primary → write lands.
    let r = http
        .post(format!(
            "http://127.0.0.1:{secondary_port}/subjects/s/versions"
        ))
        .header("content-type", "application/vnd.schemaregistry.v1+json")
        .body(body)
        .send()
        .await
        .unwrap();
    assert2::assert!(r.status() == 200);

    // The write is readable on BOTH nodes (poll the secondary for consume lag).
    for port in [a.port, b.port] {
        await_get_body(
            &http,
            &format!("http://127.0.0.1:{port}/subjects/s/versions"),
            "[1]",
            15,
        )
        .await;
    }

    // FAILOVER: stop only the primary's election session. Keep its store and
    // reader alive so the test can prove its stale generation is fenced.
    let abandoned_producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .client_id("abandoned-old-primary")
        .transactional_id("krabka-schema-registry-schema-registry")
        .enable_idempotence(true)
        .acks(Acks::All)
        .build()
        .await
        .unwrap();
    abandoned_producer.init_transactions().await.unwrap();
    let _abandoned_transaction = abandoned_producer.begin_transaction().await.unwrap();
    abandoned_producer
        .send(ProducerRecord {
            topic: "_schemas".into(),
            key: Some(Bytes::from_static(br#"{"keytype":"NOOP","magic":0}"#)),
            value: Some(Bytes::from_static(b"{}")),
            ..Default::default()
        })
        .await
        .await
        .unwrap()
        .unwrap();
    let stale_store = if a_is_primary {
        a.election_cancel.cancel();
        a.store.clone()
    } else {
        b.election_cancel.cancel();
        b.store.clone()
    };
    let survivor = if a_is_primary { &mut b } else { &mut a };
    await_state(&mut survivor.primary, 30, |s| s.is_primary).await;
    let r2 = tokio::time::timeout(
        Duration::from_secs(5),
        http.post(format!(
            "http://127.0.0.1:{}/subjects/s2/versions",
            survivor.port
        ))
        .header("content-type", "application/vnd.schemaregistry.v1+json")
        .body(body)
        .send(),
    )
    .await
    .expect("new primary write is not delayed by abandoned transaction")
    .unwrap();
    assert2::assert!(r2.status() == 200);

    // The old primary still holds its stale local PrimaryState. Its barrier can
    // catch up, but TxnOffsetCommit carries the old group generation/member and
    // the coordinator must reject the transaction. The schema record remains
    // aborted and invisible to READ_COMMITTED readers.
    let stale_write = stale_store
        .register(RegisterSchema {
            subject: "stale-primary",
            ty: SchemaType::Avro,
            schema: r#"{"type":"string"}"#,
            references: &[],
            message_type: None,
            import_id: None,
            import_version: None,
        })
        .await;
    assert2::assert!(stale_write.is_err());
    assert2::assert!(
        stale_store
            .store
            .read()
            .versions("stale-primary", true)
            .is_none()
    );

    a.cancel.cancel();
    b.cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_store_reader_returns_confluent_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let mut node = start_node(&broker.listen_addr().to_string()).await;
    await_state(&mut node.primary, 20, |state| state.is_primary).await;
    node.store_cancel.cancel();

    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/subjects/cancelled/versions",
            node.port
        ))
        .header("content-type", "application/vnd.schemaregistry.v1+json")
        .body(r#"{"schema":"\"string\""}"#)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body: serde_json::Value = response.json().await.unwrap();
    assert2::check!(status == reqwest::StatusCode::INTERNAL_SERVER_ERROR);
    assert2::check!(
        body == serde_json::json!({
            "error_code": 50002,
            "message": "Operation timed out"
        })
    );

    node.cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarting_store_replays_the_complete_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let config = cfg(&broker.listen_addr().to_string(), 8084);
    let first_cancel = CancellationToken::new();
    let first = KafkaStore::start(&config, first_cancel.clone())
        .await
        .unwrap();
    let schema = |name| format!(r#"{{"type":"record","name":"{name}","fields":[]}}"#);

    register(&first, "orders", &schema("Order"), &[], None, None).await;
    register(&first, "deleted", &schema("Deleted"), &[], None, None).await;
    register(&first, "retired", &schema("Retired"), &[], None, None).await;
    register(&first, "base", &schema("Base"), &[], None, None).await;
    let references = [SchemaReference {
        name: "base.avsc".into(),
        subject: "base".into(),
        version: SchemaVersion(1),
    }];
    register(
        &first,
        "referencing",
        &schema("UsesBase"),
        &references,
        None,
        None,
    )
    .await;
    first
        .set_subject_mode("imported", "IMPORT".into())
        .await
        .unwrap();
    register(
        &first,
        "imported",
        &schema("Imported"),
        &[],
        Some(SchemaId(50)),
        Some(SchemaVersion(7)),
    )
    .await;
    first
        .set_subject_mode("imported", "READWRITE".into())
        .await
        .unwrap();
    first.set_global_compat("FULL".into()).await.unwrap();
    first
        .set_subject_compat("orders", "BACKWARD_TRANSITIVE".into())
        .await
        .unwrap();
    first
        .soft_delete_version("deleted", krabka_schema_registry::ids::SchemaVersion(1))
        .await
        .unwrap();
    first
        .soft_delete_version("retired", SchemaVersion(1))
        .await
        .unwrap();
    first
        .permanent_delete_version("retired", SchemaVersion(1))
        .await
        .unwrap();
    register(&first, "retired", &schema("Replacement"), &[], None, None).await;
    first.set_global_mode("READWRITE".into()).await.unwrap();
    first
        .set_subject_mode("orders", "READONLY".into())
        .await
        .unwrap();
    let expected = first.store.read().clone();

    first_cancel.cancel();
    drop(first);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second_cancel = CancellationToken::new();
    let second = KafkaStore::start(&config, second_cancel.clone())
        .await
        .unwrap();

    assert2::assert!(*second.store.read() == expected);
    let next_schema = schema("Next");
    let mut expected_after = expected.clone();
    let expected_registration = expected_after
        .register("next", SchemaType::Avro, &next_schema, &[], None)
        .unwrap();
    let actual = register(&second, "next", &next_schema, &[], None, None).await;
    assert2::assert!(actual == expected_registration);
    second_cancel.cancel();
    broker.shutdown().await;
}

async fn register(
    store: &KafkaStore,
    subject: &str,
    schema: &str,
    references: &[SchemaReference],
    import_id: Option<SchemaId>,
    import_version: Option<SchemaVersion>,
) -> krabka_schema_registry::store::Registered {
    store
        .register(RegisterSchema {
            subject,
            ty: SchemaType::Avro,
            schema,
            references,
            message_type: None,
            import_id,
            import_version,
        })
        .await
        .unwrap()
}
