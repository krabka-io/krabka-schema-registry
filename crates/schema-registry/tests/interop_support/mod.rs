use std::{
    net::SocketAddr,
    process::Command,
    time::{Duration, Instant},
};

use krabka_broker::{Broker, BrokerConfig};
use krabka_units::prelude::*;

use crate::docker_support::SR_IMAGE;

pub const DIRECT_ADDR: &str = "127.0.0.1:9092";
#[allow(dead_code)]
pub const SR_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

pub async fn start_host_broker() -> (krabka_broker::BrokerHandle, tempfile::TempDir) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("krabka_broker=info,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr: SocketAddr = "0.0.0.0:9092".parse().expect("static listen addr");
    let controller_addr: SocketAddr = "0.0.0.0:9093".parse().expect("static controller addr");
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: "host.docker.internal:9092".into(),
        log_dir: dir.path().to_path_buf(),
        node_id: krabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(krabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: secs(3),
        heartbeat_timeout: secs(9),
        replica_lag_time_max: secs(30),
        controller_election_timeout: secs(5),
        controller_heartbeat_interval: millis(500),
        bootstrap_mode: krabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start broker");
    (handle, dir)
}

pub fn docker_pull() {
    let output = Command::new("docker")
        .args(["pull", SR_IMAGE])
        .output()
        .expect("spawn docker pull");
    assert2::assert!(output.status.success());
}

pub fn docker_run_schema_registry() -> String {
    let output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "-p",
            "0:8081",
            "-e",
            "SCHEMA_REGISTRY_HOST_NAME=localhost",
            "-e",
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://host.docker.internal:9092",
            "-e",
            "SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081",
            SR_IMAGE,
        ])
        .output()
        .expect("spawn docker run schema-registry");
    assert2::assert!(output.status.success());
    let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    assert2::assert!(!id.is_empty());
    id
}

pub fn docker_mapped_port(id: &str) -> u16 {
    let output = Command::new("docker")
        .args(["port", id, "8081"])
        .output()
        .expect("spawn docker port");
    assert2::assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    text.lines()
        .filter_map(|line| line.rsplit(':').next())
        .find_map(|port| port.trim().parse::<u16>().ok())
        .unwrap_or_else(|| panic!("could not parse mapped 8081 port from: {text:?}"))
}

pub fn docker_logs(id: &str) -> String {
    let output = Command::new("docker")
        .args(["logs", id])
        .output()
        .expect("spawn docker logs");
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

pub struct ContainerGuard(pub String);

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.0]).output();
    }
}

pub async fn wait_for_registry(http: &reqwest::Client, base: &str, container_id: &str) {
    let deadline = Instant::now() + Duration::from_mins(2);
    let url = format!("{base}/subjects");
    while Instant::now() < deadline {
        if http
            .get(&url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    panic!(
        "schema-registry never became ready:\n{}",
        docker_logs(container_id)
    );
}
