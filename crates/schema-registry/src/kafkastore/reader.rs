//! Group-less reader. It tails `_schemas` partition 0 over a dedicated
//! connection, folds records into the shared store, and publishes the
//! last-applied offset for read-your-writes. It mirrors
//! remote-storage-topic's `partition_fetch_loop`.

use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use krabka_client_core::{
    ClientError, ClientSecurity, Connection, ConnectionOptions, DEFAULT_FETCH_RESPONSE_MAX,
    FetchMinBytes, IsolatedFetch, fetch_partition_with_isolation_progress,
};
use krabka_protocol::{
    owned::{
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
    },
    primitives::uuid::Uuid as WireUuid,
};
use krabka_units::prelude::*;
use parking_lot::RwLock;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{
    config::{RegistryConfig, RegistryRuntimeConfig},
    kafkastore::record::SchemaRecord,
    store::StoreState,
};

/// Shared state + offset watch returned by [`spawn`].
pub struct StoreReader {
    pub store: Arc<RwLock<StoreState>>,
    pub applied_rx: watch::Receiver<i64>,
    failures: Arc<AtomicU64>,
}

impl StoreReader {
    /// Number of reader connection, metadata, Fetch, and offset-reset failures.
    #[must_use]
    pub fn failure_count(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }
}

/// `PartialEq` but not `Eq`, because the quantities store `f64`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ReaderPolicy {
    retry_backoff: Time,
    fetch_max_wait: Time,
    fetch_max: ByteSize,
}

fn reader_policy(runtime: &RegistryRuntimeConfig) -> ReaderPolicy {
    ReaderPolicy {
        retry_backoff: runtime.store_reader_retry_backoff,
        fetch_max_wait: runtime.store_reader_fetch_max_wait,
        fetch_max: runtime.store_reader_fetch_max,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchErrorAction {
    Reconnect,
    RetrySameConnection,
    ResetToLogStart,
    RefreshTopic,
}

fn fetch_error_action(e: &ClientError) -> FetchErrorAction {
    match e {
        ClientError::Server { error_code: 1 } => FetchErrorAction::ResetToLogStart,
        ClientError::Server {
            error_code: 3 | 6 | 100,
        } => FetchErrorAction::RefreshTopic,
        ClientError::Connect { .. }
        | ClientError::Disconnected
        | ClientError::Timeout(_)
        | ClientError::Io(_) => FetchErrorAction::Reconnect,
        _ => FetchErrorAction::RetrySameConnection,
    }
}

fn resolve_bootstrap_addrs(bootstrap: &str) -> Vec<SocketAddr> {
    bootstrap
        .split(',')
        .filter_map(|b| b.trim().to_socket_addrs().ok())
        .flatten()
        .collect()
}

fn should_log_failure(consecutive: u64) -> bool {
    consecutive == 1 || consecutive.is_power_of_two()
}

async fn connect_topic_leader(
    bootstrap: &str,
    opts: &ConnectionOptions,
    topic: &str,
    current_topic_id: WireUuid,
) -> Result<(Connection, WireUuid), ClientError> {
    let mut last_error = ClientError::Disconnected;
    let mut bootstrap_conn = None;
    for addr in resolve_bootstrap_addrs(bootstrap) {
        match Connection::connect_with_options(addr, opts.clone()).await {
            Ok(conn) => {
                bootstrap_conn = Some(conn);
                break;
            }
            Err(error) => last_error = error,
        }
    }
    let conn = bootstrap_conn.ok_or(last_error)?;
    let metadata = conn
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                topic_id: WireUuid::ZERO,
                name: Some(topic.to_owned()),
                ..Default::default()
            }]),
            allow_auto_topic_creation: false,
            ..Default::default()
        })
        .await?;
    let topic_metadata = metadata
        .topics
        .iter()
        .find(|entry| entry.name.as_deref() == Some(topic))
        .ok_or(ClientError::Server { error_code: 3 })?;
    if topic_metadata.error_code != 0 {
        return Err(ClientError::Server {
            error_code: topic_metadata.error_code,
        });
    }
    let partition = topic_metadata
        .partitions
        .iter()
        .find(|partition| partition.partition_index == 0)
        .ok_or(ClientError::Server { error_code: 3 })?;
    if partition.error_code != 0 {
        return Err(ClientError::Server {
            error_code: partition.error_code,
        });
    }
    let topic_id = if topic_metadata.topic_id == WireUuid::ZERO {
        current_topic_id
    } else {
        topic_metadata.topic_id
    };
    let Some(leader) = metadata
        .brokers
        .iter()
        .find(|broker| broker.node_id == partition.leader_id && broker.port > 0)
    else {
        return Ok((conn, topic_id));
    };
    let Some(addr) = format!("{}:{}", leader.host, leader.port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.next())
    else {
        return Err(ClientError::InvalidConfig(format!(
            "cannot resolve leader {}:{} for {topic}-0",
            leader.host, leader.port
        )));
    };
    conn.close();
    Ok((
        Connection::connect_with_options(addr, opts.clone()).await?,
        topic_id,
    ))
}

async fn log_start_offset(conn: &Connection, topic: &str) -> Result<i64, ClientError> {
    let response = conn
        .send(ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 1,
            topics: vec![ListOffsetsTopic {
                name: topic.to_owned(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: 0,
                    timestamp: -2,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await?;
    let partition = response
        .topics
        .iter()
        .find(|entry| entry.name == topic)
        .and_then(|entry| {
            entry
                .partitions
                .iter()
                .find(|partition| partition.partition_index == 0)
        })
        .ok_or(ClientError::Server { error_code: 3 })?;
    if partition.error_code != 0 {
        return Err(ClientError::Server {
            error_code: partition.error_code,
        });
    }
    Ok(partition.offset)
}

async fn sleep_or_cancel(cancel: &CancellationToken, duration: Time) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => true,
        () = tokio::time::sleep(duration.to_std()) => false,
    }
}

/// Apply one decoded record to the store. It returns nothing and is idempotent
/// for SCHEMA records. It is a separate function so that a unit test can call
/// it.
pub fn apply_record(store: &RwLock<StoreState>, rec: SchemaRecord) {
    match rec {
        SchemaRecord::Schema(k, v) => store.write().apply_schema(&k, &v),
        SchemaRecord::Tombstone(k) => {
            store
                .write()
                .permanent_delete_version(&k.subject, k.version);
        }
        SchemaRecord::DeleteSubject(k, _v) => {
            store.write().soft_delete_subject(&k.subject);
        }
        SchemaRecord::VersionHighWater(k, v) => {
            store
                .write()
                .observe_next_version(&k.subject, v.next_version);
        }
        SchemaRecord::Mode(k, Some(v)) => {
            let mut s = store.write();
            match k.subject {
                Some(subj) => s.set_subject_mode(&subj, v.mode),
                None => s.set_global_mode(v.mode),
            }
        }
        SchemaRecord::Mode(k, None) => {
            let mut s = store.write();
            match k.subject {
                Some(subj) => s.clear_subject_mode(&subj),
                None => s.clear_global_mode(),
            }
        }
        SchemaRecord::Config(k, Some(v)) => {
            let mut s = store.write();
            match k.subject {
                Some(subj) => s.set_subject_compat(&subj, v.compatibility_level),
                None => s.set_global_compat(v.compatibility_level),
            }
        }
        SchemaRecord::Config(k, None) => {
            // CONFIG tombstone: clear the per-subject override so the subject
            // reverts to the global level.
            // Global CONFIG tombstones (k.subject = None) are ignored: there is
            // no DELETE /config endpoint in our REST surface, so we never emit
            // them, and ignoring them is safe if an external SR sends one.
            if let Some(subj) = k.subject {
                store.write().clear_subject_compat(&subj);
            }
        }
        SchemaRecord::Noop | SchemaRecord::Unknown => {}
    }
}

/// Spawn the reader. It returns the shared store and an offset watch
/// immediately. The background task runs until `cancel` fires.
#[must_use]
pub fn spawn(
    cfg: &RegistryConfig,
    topic_id: WireUuid,
    initial_state: StoreState,
    security: Option<ClientSecurity>,
    cancel: CancellationToken,
) -> StoreReader {
    let store = Arc::new(RwLock::new(initial_state));
    let (applied_tx, applied_rx) = watch::channel(-1_i64);
    let topic = cfg.schemas_topic.clone();
    let bootstrap = cfg.bootstrap.clone();
    let client_id = format!("{}-reader", cfg.client_id);
    let store_bg = store.clone();
    let policy = reader_policy(&cfg.runtime);
    let dispatch_queue_capacity = cfg.runtime.client_dispatch_queue_capacity;
    let frame_max = cfg.runtime.client_frame_max;
    let failures = Arc::new(AtomicU64::new(0));
    let failures_bg = failures.clone();

    tokio::spawn(async move {
        let opts = ConnectionOptions {
            client_id,
            dispatch_queue_capacity,
            frame_max,
            security: security.map(Box::new),
            ..Default::default()
        };
        let mut next = 0_i64;
        let mut topic_id = topic_id;
        let mut consecutive_failures = 0_u64;
        loop {
            let conn = match connect_topic_leader(&bootstrap, &opts, &topic, topic_id).await {
                Ok((conn, resolved_topic_id)) => {
                    topic_id = resolved_topic_id;
                    conn
                }
                Err(error) => {
                    consecutive_failures += 1;
                    failures_bg.fetch_add(1, Ordering::Relaxed);
                    if should_log_failure(consecutive_failures) {
                        tracing::warn!(
                            error = %error,
                            consecutive_failures,
                            "store reader: leader resolution failed; backing off"
                        );
                    }
                    if sleep_or_cancel(&cancel, policy.retry_backoff).await {
                        return;
                    }
                    continue;
                }
            };

            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => {
                        conn.close();
                        return;
                    }
                    res = fetch_partition_with_isolation_progress(&conn, IsolatedFetch {
                        topic: &topic,
                        topic_id,
                        partition: 0,
                        fetch_offset: next,
                        max_wait: policy.fetch_max_wait,
                        max: DEFAULT_FETCH_RESPONSE_MAX,
                        partition_max: policy.fetch_max,
                        fetch_min: FetchMinBytes::default(),
                        isolation_level: 1,
                    }) => {
                        match res {
                            Ok(progress) => {
                                consecutive_failures = 0;
                                for r in progress.records {
                                    if r.offset < next {
                                        continue;
                                    }
                                    let key = r.key.as_deref().unwrap_or_default();
                                    apply_record(
                                        &store_bg,
                                        SchemaRecord::decode(key, r.value.as_deref()),
                                    );
                                }
                                if let Some(cursor) = progress.next_offset {
                                    next = next.max(cursor);
                                    // Publish cursor progress after all visible
                                    // records have been applied. This also
                                    // advances over aborted/control batches.
                                    let _ = applied_tx.send(next - 1);
                                }
                            }
                            Err(error) => {
                                let action = fetch_error_action(&error);
                                consecutive_failures += 1;
                                failures_bg.fetch_add(1, Ordering::Relaxed);
                                if should_log_failure(consecutive_failures) {
                                    tracing::warn!(
                                        error = %error,
                                        action = ?action,
                                        consecutive_failures,
                                        "store reader: fetch error; backing off"
                                    );
                                }
                                if action == FetchErrorAction::ResetToLogStart {
                                    match log_start_offset(&conn, &topic).await {
                                        Ok(offset) => {
                                            tracing::warn!(
                                                fetch_offset = next,
                                                log_start_offset = offset,
                                                "store reader: requested history was compacted away; resetting to log start"
                                            );
                                            next = offset;
                                        }
                                        Err(reset_error) => {
                                            consecutive_failures += 1;
                                            failures_bg.fetch_add(1, Ordering::Relaxed);
                                            if should_log_failure(consecutive_failures) {
                                                tracing::warn!(
                                                    error = %reset_error,
                                                    consecutive_failures,
                                                    "store reader: log-start reset failed; backing off"
                                                );
                                            }
                                            conn.close();
                                            if sleep_or_cancel(&cancel, policy.retry_backoff).await {
                                                return;
                                            }
                                            break;
                                        }
                                    }
                                }
                                if sleep_or_cancel(&cancel, policy.retry_backoff).await {
                                    return;
                                }
                                if matches!(
                                    action,
                                    FetchErrorAction::Reconnect | FetchErrorAction::RefreshTopic
                                ) {
                                    conn.close();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    StoreReader {
        store,
        applied_rx,
        failures,
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use krabka_client_core::ClientError;

    use super::*;
    use crate::{
        config::RegistryRuntimeConfig,
        format::SchemaType,
        ids::{SchemaId, SchemaVersion},
        kafkastore::record::{SchemaKey, SchemaValue},
    };

    #[test]
    fn reader_policy_uses_configured_runtime() {
        let runtime = RegistryRuntimeConfig {
            store_reader_retry_backoff: millis(333),
            store_reader_fetch_max_wait: millis(777),
            store_reader_fetch_max: mebibytes(2),
            ..RegistryRuntimeConfig::default()
        };

        assert2::check!(
            reader_policy(&runtime)
                == ReaderPolicy {
                    retry_backoff: millis(333),
                    fetch_max_wait: millis(777),
                    fetch_max: mebibytes(2),
                }
        );
    }

    #[test]
    fn apply_record_folds_schema_and_ignores_noop() {
        let store = RwLock::new(StoreState::default());
        let k = SchemaKey::new("av-value", SchemaVersion(1));
        let v = SchemaValue {
            subject: "av-value".into(),
            version: SchemaVersion(1),
            id: SchemaId(1),
            schema_type: None,
            message_type: None,
            references: vec![],
            schema: "{\"type\":\"int\"}".into(),
            deleted: false,
        };
        apply_record(&store, SchemaRecord::Schema(k, v));
        apply_record(&store, SchemaRecord::Noop);
        let snapshot = store.read();
        assert2::assert!(snapshot.versions("av-value", false).unwrap() == vec![SchemaVersion(1)]);
        assert2::assert!(
            snapshot.schema_by_id(SchemaId(1), false).unwrap().1 == "{\"type\":\"int\"}".to_owned()
        );
    }

    #[test]
    fn fetch_errors_choose_recovery_action() {
        let cases = [
            (
                "offset out of range",
                ClientError::Server { error_code: 1 },
                FetchErrorAction::ResetToLogStart,
            ),
            (
                "unknown topic or partition",
                ClientError::Server { error_code: 3 },
                FetchErrorAction::RefreshTopic,
            ),
            (
                "not leader or follower",
                ClientError::Server { error_code: 6 },
                FetchErrorAction::RefreshTopic,
            ),
            (
                "unknown topic id",
                ClientError::Server { error_code: 100 },
                FetchErrorAction::RefreshTopic,
            ),
            (
                "disconnected",
                ClientError::Disconnected,
                FetchErrorAction::Reconnect,
            ),
            (
                "timeout",
                ClientError::Timeout(krabka_units::millis(1)),
                FetchErrorAction::Reconnect,
            ),
            (
                "connect",
                ClientError::Connect {
                    addr: "127.0.0.1:9092".parse().unwrap(),
                    source: io::Error::new(io::ErrorKind::ConnectionRefused, "refused"),
                },
                FetchErrorAction::Reconnect,
            ),
            (
                "io",
                ClientError::Io(io::Error::new(io::ErrorKind::ConnectionReset, "reset")),
                FetchErrorAction::Reconnect,
            ),
            (
                "other server error",
                ClientError::Server { error_code: 7 },
                FetchErrorAction::RetrySameConnection,
            ),
        ];
        for (_name, error, expected) in cases {
            assert2::assert!(fetch_error_action(&error) == expected);
        }
    }

    #[test]
    fn retry_warnings_are_rate_limited() {
        let logged: Vec<u64> = (1..=16)
            .filter(|count| should_log_failure(*count))
            .collect();
        assert2::check!(logged == vec![1, 2, 4, 8, 16]);
    }

    #[test]
    fn apply_record_handles_mode_delete_tombstone() {
        use crate::kafkastore::record::{
            DeleteSubjectKey, DeleteSubjectValue, ModeKey, ModeValue, VersionHighWaterKey,
            VersionHighWaterValue,
        };
        let store = RwLock::new(StoreState::default());
        let v = SchemaValue {
            subject: "s".into(),
            version: SchemaVersion(1),
            id: SchemaId(1),
            schema_type: None,
            message_type: None,
            references: vec![],
            schema: "{\"type\":\"int\"}".into(),
            deleted: false,
        };
        apply_record(
            &store,
            SchemaRecord::Schema(SchemaKey::new("s", SchemaVersion(1)), v),
        );
        // global mode set then clear (Mode(None) -> clear_global_mode)
        apply_record(
            &store,
            SchemaRecord::Mode(
                ModeKey {
                    keytype: "MODE".into(),
                    subject: None,
                    magic: 0,
                },
                Some(ModeValue {
                    mode: "READONLY".into(),
                }),
            ),
        );
        assert2::assert!(store.read().global_mode() == "READONLY");
        apply_record(
            &store,
            SchemaRecord::Mode(
                ModeKey {
                    keytype: "MODE".into(),
                    subject: None,
                    magic: 0,
                },
                None,
            ),
        );
        assert2::assert!(store.read().global_mode() == "READWRITE");
        // subject mode set then clear
        apply_record(
            &store,
            SchemaRecord::Mode(
                ModeKey {
                    keytype: "MODE".into(),
                    subject: Some("s".into()),
                    magic: 0,
                },
                Some(ModeValue {
                    mode: "IMPORT".into(),
                }),
            ),
        );
        assert2::assert!(store.read().subject_mode("s") == Some("IMPORT"));
        apply_record(
            &store,
            SchemaRecord::Mode(
                ModeKey {
                    keytype: "MODE".into(),
                    subject: Some("s".into()),
                    magic: 0,
                },
                None,
            ),
        );
        assert2::assert!(store.read().subject_mode("s") == None);
        // soft-delete the subject, then permanently delete its version via a tombstone
        apply_record(
            &store,
            SchemaRecord::DeleteSubject(
                DeleteSubjectKey {
                    keytype: "DELETE_SUBJECT".into(),
                    subject: "s".into(),
                    magic: 0,
                },
                DeleteSubjectValue {
                    subject: "s".into(),
                    version: SchemaVersion(1),
                },
            ),
        );
        assert2::assert!(store.read().versions("s", false).is_none());
        apply_record(
            &store,
            SchemaRecord::Tombstone(SchemaKey::new("s", SchemaVersion(1))),
        );
        assert2::assert!(store.read().versions("s", true).is_none());

        apply_record(
            &store,
            SchemaRecord::VersionHighWater(
                VersionHighWaterKey {
                    keytype: "NOOP".into(),
                    subject: "compacted".into(),
                    magic: 0,
                },
                VersionHighWaterValue {
                    next_version: SchemaVersion(4),
                },
            ),
        );
        let registered = store
            .write()
            .register(
                "compacted",
                SchemaType::Avro,
                "{\"type\":\"int\"}",
                &[],
                None,
            )
            .unwrap();
        assert2::assert!(registered.version == SchemaVersion(4));
    }
}
