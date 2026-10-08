//! In-process registry nodes behind HTTP Basic auth, and the REST calls the
//! suite makes to them.
//!
//! Each node runs the full stack a deployment does: the `_schemas` store, the
//! primary election, and the auth and forwarding middleware.

use std::{collections::HashMap, sync::Arc, time::Duration};

use assert2::assert;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use krabka_schema_registry::{
    auth::{AuthState, basic::BasicAuthStore},
    config::{RegistryConfig, RegistryRuntimeConfig, SecurityConfig},
    election::{Election, PrimaryState},
    kafkastore::KafkaStore,
    rest::{self, AppState, SecurityLayers, forward::ForwardState},
};
use serde_json::Value;
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// The one registry user, which the brokers authenticate as.
pub const REGISTRY_USERNAME: &str = "broker";
/// [`REGISTRY_USERNAME`]'s password.
pub const REGISTRY_PASSWORD: &str = "live-registry-secret";
const REGISTRY_FORWARD_SECRET: &str = "live-registry-forward-secret";

/// A running registry node.
pub struct RegistryNode {
    pub url: String,
    pub primary: watch::Receiver<PrimaryState>,
    /// Cancels only the node's election session, so the node keeps serving
    /// while another one takes over as primary.
    pub election_cancel: CancellationToken,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl RegistryNode {
    /// Stop serving, and wait until the server task has finished.
    pub async fn stop(self) {
        self.cancel.cancel();
        self.task.await.unwrap();
    }
}

fn registry_config(bootstrap: &str, url: &str, id: usize) -> RegistryConfig {
    RegistryConfig {
        bootstrap: bootstrap.into(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 3,
        client_id: format!("live-registry-{id}"),
        advertised_url: url.into(),
        group_id: "live-schema-registry".into(),
        leader_eligibility: true,
        runtime: RegistryRuntimeConfig::default(),
        security: SecurityConfig::default(),
    }
}

/// Start registry node `id` against the cluster at `bootstrap`, serving on
/// `listener`. The listener is bound by the caller so a URL can be handed to
/// the brokers before the node exists.
pub async fn start(bootstrap: &str, id: usize, listener: TcpListener) -> RegistryNode {
    let url = format!("http://{}", listener.local_addr().unwrap());
    let config = registry_config(bootstrap, &url, id);
    let cancel = CancellationToken::new();
    let election_cancel = cancel.child_token();
    let store = KafkaStore::start(&config, cancel.clone()).await.unwrap();
    let primary = Election::start(&config, election_cancel.clone())
        .await
        .unwrap();
    store.install_primary(primary.clone());
    let auth = AuthState {
        audit: krabka_audit::AuditLog::disabled(),
        basic: Some(Arc::new(BasicAuthStore::from_users(HashMap::from([(
            REGISTRY_USERNAME.to_owned(),
            REGISTRY_PASSWORD.to_owned(),
        )])))),
        bearer: None,
        require_auth: true,
        realm: "live-registry".into(),
        forward_secret: Some(REGISTRY_FORWARD_SECRET.into()),
    };
    let app = rest::router_with_security(
        AppState { store },
        SecurityLayers {
            auth,
            authz: None,
            forward: ForwardState {
                primary: primary.clone(),
                http: reqwest::Client::new(),
                node_id: url.clone(),
                forward_max_body: config.runtime.forward_max_body,
                forward_secret: Some(REGISTRY_FORWARD_SECRET.into()),
            },
        },
    );
    let serve_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { serve_cancel.cancelled().await })
            .await
            .unwrap();
    });
    RegistryNode {
        url,
        primary,
        election_cancel,
        cancel,
        task,
    }
}

/// Wait up to 30 seconds for one of `nodes` to be elected primary, and answer
/// its index.
pub async fn wait_for_primary(nodes: &[RegistryNode]) -> usize {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(index) = nodes
                .iter()
                .position(|node| node.primary.borrow().is_primary)
            {
                return index;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("registry primary election")
}

/// Wait up to 30 seconds for `node` to become primary.
pub async fn wait_until_primary(node: &mut RegistryNode) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !node.primary.borrow().is_primary {
            node.primary.changed().await.unwrap();
        }
    })
    .await
    .expect("registry primary failover");
}

/// An HTTP client that sends [`REGISTRY_USERNAME`]'s Basic credentials on
/// every request.
pub fn authenticated_http() -> reqwest::Client {
    let credentials = STANDARD.encode(format!("{REGISTRY_USERNAME}:{REGISTRY_PASSWORD}"));
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        format!("Basic {credentials}").parse().unwrap(),
    );
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap()
}

/// Register `body` under `subject` through the node at `url`, retrying for up
/// to 30 seconds while the registry is not ready, and answer the schema id.
pub async fn register(client: &reqwest::Client, url: &str, subject: &str, body: Value) -> u32 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let response = tokio::time::timeout_at(
            deadline,
            client
                .post(format!("{url}/subjects/{subject}/versions"))
                .json(&body)
                .send(),
        )
        .await
        .expect("registry registration deadline")
        .unwrap();
        let status = response.status();
        if status.is_success() {
            let response_body: Value = tokio::time::timeout_at(deadline, response.json())
                .await
                .expect("registry response deadline")
                .unwrap();
            return u32::try_from(response_body["id"].as_u64().expect("schema id")).unwrap();
        }
        let response_body = tokio::time::timeout_at(deadline, response.text())
            .await
            .expect("registry response deadline")
            .unwrap_or_default();
        assert!(
            tokio::time::Instant::now() < deadline,
            "registry returned {status}: {response_body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait up to 30 seconds for the node at `url` to serve schema `id`.
pub async fn wait_for_schema(client: &reqwest::Client, url: &str, id: u32) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if client
                .get(format!("{url}/schemas/ids/{id}"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("schema replication deadline");
}

/// `GET url`, which must succeed, as JSON.
pub async fn get_json(client: &reqwest::Client, url: &str) -> Value {
    client
        .get(url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}
