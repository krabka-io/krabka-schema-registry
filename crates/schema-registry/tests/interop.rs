//! Docker interop test: prove our `KafkaStore` + `StoreReader` can decode
//! `_schemas` records that a REAL `the pinned cp-schema-registry image`
//! wrote, and that our REST router returns the same schema through `GET`.
//!
//! Mirrors the setup in `capture_fixtures.rs`:
//! - A Krabka broker binds `0.0.0.0:9092`, advertises `host.docker.internal:9092`.
//! - cp-schema-registry connects to it via Docker's `--add-host` gateway.
//! - We register an Avro schema through cp's REST endpoint.
//! - Then we start OUR `KafkaStore` (which replays the `_schemas` topic that cp wrote)
//!   and assert `GET /schemas/ids/1` returns the schema, and `GET /subjects` lists it.
//!
//! Gated `#[ignore]` so `cargo test --workspace` never needs Docker. Run with:
//!
//! ```text
//! cargo test -p krabka-schema-registry --test interop -- --ignored --nocapture
//! ```
//!
//! The test tears down the container on both success and failure, through
//! `ContainerGuard`.

use std::time::{Duration, Instant};

use axum::{body::Body, http::Request};
use krabka_schema_registry::{
    config::{RegistryConfig, SecurityConfig},
    kafkastore::KafkaStore,
    rest::{self, AppState},
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

mod docker_support;
mod interop_support;
use interop_support::{
    ContainerGuard, DIRECT_ADDR, SR_CONTENT_TYPE, docker_mapped_port, docker_pull,
    docker_run_schema_registry, start_host_broker, wait_for_registry,
};

/// POST a schema registration to cp-schema-registry's REST endpoint.
async fn register_via_cp(http: &reqwest::Client, base: &str, subject: &str, schema: &str) -> i64 {
    let body = serde_json::json!({ "schema": schema });
    let url = format!("{base}/subjects/{subject}/versions");
    let resp = http
        .post(&url)
        .header("Content-Type", SR_CONTENT_TYPE)
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {url}: {e}"));
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert2::assert!(status.is_success());
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    v["id"]
        .as_i64()
        .unwrap_or_else(|| panic!("no id in {text}"))
}

// ── our router helpers ─────────────────────────────────────────────────────────

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let b = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    serde_json::from_slice(&b).unwrap()
}

async fn get_json(app: &axum::Router, uri: &str) -> serde_json::Value {
    let resp = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    body_json(resp).await
}

// ── the test ───────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn our_store_decodes_cp_schema_registry_records() {
    let avro_schema = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}"#;

    docker_pull();

    let (broker, _dir) = start_host_broker().await;

    let container_id = docker_run_schema_registry();
    let _guard = ContainerGuard(container_id.clone());

    let port = docker_mapped_port(&container_id);
    let base = format!("http://127.0.0.1:{port}");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build reqwest client");

    wait_for_registry(&http, &base, &container_id).await;

    // Register one Avro schema via the REAL cp-schema-registry.
    let id = register_via_cp(&http, &base, "av-value", avro_schema).await;
    eprintln!("INTEROP cp registered id={id}");
    assert2::assert!(id == 1);

    // Brief pause so the `_schemas` topic record is durable before our
    // KafkaStore starts its reader.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Now start OUR KafkaStore against the SAME broker (direct 127.0.0.1).
    let cfg = RegistryConfig {
        bootstrap: DIRECT_ADDR.to_string(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: "sr-interop".into(),
        advertised_url: "http://127.0.0.1:0".into(),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
        runtime: krabka_schema_registry::config::RegistryRuntimeConfig::default(),
        security: SecurityConfig::default(),
    };
    let cancel = CancellationToken::new();
    let store = KafkaStore::start(&cfg, cancel.clone())
        .await
        .expect("start KafkaStore");

    // Give the reader a moment to replay the existing records.
    // The reader is live and will have caught up by the time we poll.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let subjects = store.store.read().subjects(false);
        if subjects.contains(&"av-value".to_string()) {
            eprintln!("INTEROP store has av-value after replay");
            break;
        }
        assert2::assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Build our REST router on top of the replayed store.
    let app = rest::router(AppState { store });

    // Assert GET /schemas/ids/1 returns the schema cp registered.
    let got = get_json(&app, "/schemas/ids/1").await;
    eprintln!("INTEROP GET /schemas/ids/1 = {got}");
    let schema_type_omitted = got.get("schemaType").is_none();
    let schema_str = got["schema"].as_str().expect("schema field is a string");
    // Parse both sides as JSON and compare structurally (field order may differ).
    let got_v: serde_json::Value = serde_json::from_str(schema_str)
        .unwrap_or_else(|e| panic!("schema is not valid JSON: {e}\n  raw: {schema_str}"));
    let expected_v: serde_json::Value = serde_json::from_str(avro_schema).unwrap();
    assert2::assert!(schema_type_omitted);
    assert2::assert!(got_v == expected_v);

    // Assert GET /subjects lists "av-value".
    let subs = get_json(&app, "/subjects").await;
    eprintln!("INTEROP GET /subjects = {subs}");
    let names: Vec<String> = subs
        .as_array()
        .expect("subjects is an array")
        .iter()
        .map(|v| v.as_str().expect("string").to_string())
        .collect();
    assert2::assert!(names.contains(&"av-value".to_string()));

    eprintln!(
        "INTEROP PASS: our StoreReader successfully decoded cp-schema-registry's _schemas records"
    );

    cancel.cancel();
    broker.shutdown().await;
}
