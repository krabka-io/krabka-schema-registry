//! Readiness of the in-process broker that the integration suites start.
//!
//! A broker does not create `__consumer_offsets` or `__transaction_state`
//! when it starts. The first `FindCoordinator` that needs one asks for it and
//! answers `COORDINATOR_NOT_AVAILABLE` until the topic exists and is loaded.
//! A registry node joins the election group and initializes a transactional
//! producer, so a suite brings both topics up before it starts one, as
//! Kafka's `IntegrationTestHarness` calls `createOffsetsTopic`.

use krabka_broker::BrokerHandle;

/// Creates the group and transaction coordinator topics on `broker`, and
/// waits until both coordinators have loaded the partitions that it leads.
///
/// In a multi-node cluster, call it on every broker.
pub async fn wait_until_coordinators_ready(broker: &BrokerHandle) {
    broker.wait_until_group_coordinator_ready().await;
    broker.wait_until_transaction_coordinator_ready().await;
}
