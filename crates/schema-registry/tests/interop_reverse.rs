//! Docker interop test: prove cp-schema-registry can replay `_schemas` records
//! written by Krabka.

#![recursion_limit = "256"]

use std::time::Duration;

use krabka_schema_registry::{
    config::{RegistryConfig, SecurityConfig},
    format::SchemaType,
    ids::{SchemaId, SchemaVersion},
    kafkastore::{KafkaStore, RegisterSchema, record::SchemaReference},
};
use tokio_util::sync::CancellationToken;

mod docker_support;
mod interop_support;
use interop_support::{
    ContainerGuard, DIRECT_ADDR, docker_logs, docker_mapped_port, docker_pull,
    docker_run_schema_registry, start_host_broker, wait_for_registry,
};

fn config() -> RegistryConfig {
    RegistryConfig {
        bootstrap: DIRECT_ADDR.into(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: "reverse-interop".into(),
        advertised_url: "http://127.0.0.1:0".into(),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
        runtime: krabka_schema_registry::config::RegistryRuntimeConfig::default(),
        security: SecurityConfig::default(),
    }
}

async fn register(store: &KafkaStore, subject: &str, ty: SchemaType, schema: &str) -> i32 {
    store
        .register(RegisterSchema {
            subject,
            ty,
            schema,
            references: &[],
            message_type: None,
            import_id: None,
            import_version: None,
        })
        .await
        .unwrap()
        .id
        .0
}

async fn get_json(http: &reqwest::Client, base: &str, path: &str) -> serde_json::Value {
    let response = http.get(format!("{base}{path}")).send().await.unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert2::assert!(status.is_success(), "GET {path}: {status} {text}");
    serde_json::from_str(&text).unwrap()
}

async fn post_json(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let response = http
        .post(format!("{base}{path}"))
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert2::assert!(status.is_success(), "POST {path}: {status} {text}");
    serde_json::from_str(&text).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn cp_replays_krabka_schema_registry_records() {
    docker_pull();
    let (broker, _directory) = start_host_broker().await;
    let cancel = CancellationToken::new();
    let store = KafkaStore::start(&config(), cancel.clone()).await.unwrap();
    let cases = [
        (
            "av-value",
            SchemaType::Avro,
            r#"{"type":"record","name":"A","fields":[{"name":"id","type":"int"}]}"#,
        ),
        (
            "proto-value",
            SchemaType::Protobuf,
            "syntax = \"proto3\"; message P { int32 id = 1; }",
        ),
        (
            "json-value",
            SchemaType::Json,
            r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#,
        ),
    ];
    let mut expected_schemas = Vec::new();
    for (subject, ty, schema) in cases {
        expected_schemas.push((
            subject,
            ty,
            register(&store, subject, ty, schema).await,
            schema,
        ));
    }
    let base_schema = r#"{"type":"record","name":"Base","fields":[{"name":"id","type":"int"}]}"#;
    register(&store, "base-value", SchemaType::Avro, base_schema).await;
    let parent_schema =
        r#"{"type":"record","name":"Parent","fields":[{"name":"base","type":"Base"}]}"#;
    let references = [SchemaReference {
        name: "base.avsc".into(),
        subject: "base-value".into(),
        version: SchemaVersion(1),
    }];
    store
        .register(RegisterSchema {
            subject: "parent-value",
            ty: SchemaType::Avro,
            schema: parent_schema,
            references: &references,
            message_type: None,
            import_id: None,
            import_version: None,
        })
        .await
        .unwrap();
    register(
        &store,
        "retired-value",
        SchemaType::Avro,
        r#"{"type":"string"}"#,
    )
    .await;
    store
        .soft_delete_version("retired-value", SchemaVersion(1))
        .await
        .unwrap();
    store
        .permanent_delete_version("retired-value", SchemaVersion(1))
        .await
        .unwrap();
    register(
        &store,
        "retired-value",
        SchemaType::Avro,
        r#"{"type":"long"}"#,
    )
    .await;
    store
        .set_subject_mode("imported-value", "IMPORT".into())
        .await
        .unwrap();
    store
        .register(RegisterSchema {
            subject: "imported-value",
            ty: SchemaType::Avro,
            schema: r#"{"type":"bytes"}"#,
            references: &[],
            message_type: None,
            import_id: Some(SchemaId(50)),
            import_version: Some(SchemaVersion(7)),
        })
        .await
        .unwrap();
    store
        .set_subject_mode("imported-value", "READWRITE".into())
        .await
        .unwrap();
    store.set_global_compat("FORWARD".into()).await.unwrap();
    store
        .set_subject_compat("av-value", "FULL".into())
        .await
        .unwrap();
    store
        .set_subject_mode("json-value", "READONLY".into())
        .await
        .unwrap();
    let mut expected = store.store.read().clone();
    cancel.cancel();
    drop(store);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let container_id = docker_run_schema_registry();
    let guard = ContainerGuard(container_id.clone());
    let base = format!("http://127.0.0.1:{}", docker_mapped_port(&container_id));
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    wait_for_registry(&http, &base, &container_id).await;

    let subjects = get_json(&http, &base, "/subjects").await;
    for (subject, ty, id, schema) in expected_schemas {
        assert2::check!(
            subjects
                .as_array()
                .unwrap()
                .contains(&serde_json::json!(subject))
        );
        let version = get_json(&http, &base, &format!("/subjects/{subject}/versions/1")).await;
        assert2::check!(version["id"] == id);
        let by_id = get_json(&http, &base, &format!("/schemas/ids/{id}")).await;
        if ty == SchemaType::Protobuf {
            assert2::check!(by_id["schema"].as_str().unwrap().contains("message P"));
        } else {
            let actual: serde_json::Value =
                serde_json::from_str(by_id["schema"].as_str().unwrap()).unwrap();
            let expected_schema: serde_json::Value = serde_json::from_str(schema).unwrap();
            assert2::check!(actual == expected_schema);
        }
    }
    assert2::check!(
        get_json(&http, &base, "/config/av-value").await["compatibilityLevel"] == "FULL"
    );
    assert2::check!(get_json(&http, &base, "/config").await["compatibilityLevel"] == "FORWARD");
    assert2::check!(get_json(&http, &base, "/mode/json-value").await["mode"] == "READONLY");
    let parent = get_json(&http, &base, "/subjects/parent-value/versions/1").await;
    assert2::check!(parent["references"][0]["subject"] == "base-value");
    assert2::check!(
        get_json(&http, &base, "/subjects/retired-value/versions").await == serde_json::json!([2])
    );
    assert2::check!(
        get_json(&http, &base, "/subjects/imported-value/versions/7").await["id"] == 50
    );
    let cp_schema = r#"{"type":"record","name":"Cp","fields":[]}"#;
    let cp_registration = post_json(
        &http,
        &base,
        "/subjects/cp-value/versions",
        serde_json::json!({"schema": cp_schema}),
    )
    .await;
    assert2::check!(cp_registration["id"] == 51);
    let logs = docker_logs(&container_id);
    assert2::check!(!logs.contains("SerializationException"));
    assert2::check!(!logs.contains("Error deserializing"));
    drop(guard);

    expected
        .register("cp-value", SchemaType::Avro, cp_schema, &[], None)
        .unwrap();
    let replay_cancel = CancellationToken::new();
    let replayed = KafkaStore::start(&config(), replay_cancel.clone())
        .await
        .unwrap();
    assert2::assert!(*replayed.store.read() == expected);
    assert2::assert!(
        register(
            &replayed,
            "next-value",
            SchemaType::Avro,
            r#"{"type":"null"}"#
        )
        .await
            == 52
    );
    replay_cancel.cancel();

    broker.shutdown().await;
}
