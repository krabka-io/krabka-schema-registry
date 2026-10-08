//! The three-broker loopback cluster, every broker's schema gate pointed at
//! the registry.
//!
//! The brokers share a static controller voter set. Each one's client and
//! controller listeners are bound before any broker starts and handed over
//! live, so the ports in the voter set and in each broker's self-registration
//! are concrete, and no other test can take one in between.

use std::{net::SocketAddr, sync::Arc};

use krabka_broker::{
    BootstrapMode, Broker, BrokerConfig, BrokerHandle, NodeId, schema_validation::SchemaValidator,
};
use krabka_units::{minutes, secs};
use tempfile::TempDir;
use tokio::net::TcpListener;

use crate::{
    broker_support,
    registry::{REGISTRY_PASSWORD, REGISTRY_USERNAME},
};

/// The number of brokers, and the replication factor of every topic.
pub const BROKERS: usize = 3;

/// One running broker and the log directory it owns.
pub struct Node {
    pub broker: BrokerHandle,
    pub dir: TempDir,
}

/// The broker config of node `index` (zero-based) in the static voter set.
fn node_config(
    index: usize,
    client_addr: SocketAddr,
    controller_addr: SocketAddr,
    voters: &[(NodeId, String)],
    log_dir: &std::path::Path,
    registry_url: &str,
) -> BrokerConfig {
    let id = u64::try_from(index + 1).unwrap();
    let mut config =
        BrokerConfig::for_tests(log_dir.to_path_buf()).with_internal_topics_for(BROKERS);
    config.broker_id = i32::try_from(id).unwrap();
    config.node_id = NodeId(id);
    // A broker registers its advertised `host:port` before it binds, so a `:0`
    // here would register port 0 and break the inter-broker dial.
    config.listen_addr = client_addr;
    config.advertised_listener = client_addr.to_string();
    config.controller_listen_addr = controller_addr;
    config.directory_id = uuid::Uuid::from_u128(u128::from(id));
    config.bootstrap_mode = BootstrapMode::Bootstrap;
    config.controller_quorum_voters = voters.to_vec();
    config.auto_join = false;
    config.bootstrap_servers = vec![];
    config.schema_validator = Some(Arc::new(
        SchemaValidator::new_with_basic_auth(
            registry_url.into(),
            false,
            64,
            minutes(5),
            secs(2),
            REGISTRY_USERNAME.into(),
            REGISTRY_PASSWORD.into(),
        )
        .unwrap(),
    ));
    config
}

/// Start the cluster with fail-closed validation against `registry_url`, and
/// wait until every broker sees every other registered and has its
/// coordinators loaded.
///
/// The brokers start concurrently: a broker's start waits for a controller
/// leader, and none can be elected until a majority of the voters is up.
pub async fn start(registry_url: &str) -> Vec<Node> {
    let mut client_listeners = Vec::with_capacity(BROKERS);
    let mut controller_listeners = Vec::with_capacity(BROKERS);
    for _ in 0..BROKERS {
        client_listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        controller_listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let voters: Vec<(NodeId, String)> = controller_listeners
        .iter()
        .zip(1..)
        .map(|(listener, id)| (NodeId(id), listener.local_addr().unwrap().to_string()))
        .collect();

    let mut starts = Vec::with_capacity(BROKERS);
    let mut dirs = Vec::with_capacity(BROKERS);
    for (index, (client, controller)) in client_listeners
        .into_iter()
        .zip(controller_listeners)
        .enumerate()
    {
        let dir = TempDir::new().unwrap();
        let config = node_config(
            index,
            client.local_addr().unwrap(),
            controller.local_addr().unwrap(),
            &voters,
            dir.path(),
            registry_url,
        );
        starts.push(tokio::spawn(Broker::start_with_listeners(
            config,
            Some(controller),
            Some(client),
        )));
        dirs.push(dir);
    }

    let mut nodes = Vec::with_capacity(BROKERS);
    for (start, dir) in starts.into_iter().zip(dirs) {
        let broker = start.await.unwrap().unwrap();
        nodes.push(Node { broker, dir });
    }
    nodes[0].broker.wait_until_controller_leader().await;
    for node in &nodes {
        node.broker.wait_until_brokers_registered(BROKERS).await;
    }
    // A registry node joins the election group and initializes a
    // transactional producer, so the coordinators come up before it starts.
    for node in &nodes {
        broker_support::wait_until_coordinators_ready(&node.broker).await;
    }
    nodes
}

/// The node whose id is `node_id`.
pub fn node_with_id(nodes: &[Node], node_id: u64) -> &Node {
    nodes
        .iter()
        .find(|node| node.broker.node_id() == node_id)
        .unwrap()
}

/// The node that leads partition 0 of `topic`, in `observer`'s metadata.
pub fn leader_of<'a>(nodes: &'a [Node], observer: &BrokerHandle, topic: &str) -> &'a Node {
    let leader_id = observer
        .partition_leader_for_test(topic, 0)
        .unwrap_or_else(|| panic!("{topic} has no leader"));
    node_with_id(nodes, leader_id)
}

/// Shut the brokers down in order.
pub async fn shutdown(nodes: Vec<Node>) {
    for node in nodes {
        node.broker.shutdown().await;
    }
}
