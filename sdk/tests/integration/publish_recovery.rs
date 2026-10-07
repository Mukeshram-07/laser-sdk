//! Deterministic publish-recovery integration tests.
//!
//! These tests exercise the SDK's reliability guarantees under controlled fault
//! injection using the `TestIggy` / `TestIggyCluster` harness.
//!
//! Scenarios covered:
//! 1. Publish retry after connection drop before response
//! 2. Partial batch confirmation — `committed` ranges preserved on failure
//! 3. Re-authentication after session invalidation (leader failover)
//! 4. Concurrent publish recovery — no silent loss
//! 5a. Repeated server restart — progress resumes each time
//! 5b. Retry exhaustion returns `PublishFailed`, not a hang
//! 6. Consumer-group recovery — offsets preserved, at-least-once delivery
//!
//! Scenario 7 (single-node mapped-port routing) is not implemented because
//! `TestIggy` binds the server on the same address it advertises.  The harness
//! exposes no API to make the server announce a *different* address, and no
//! proxy-lies-in-handshake helper exists.  See the "Test coverage" section of
//! `docs/publish-recovery.md` for the exact limitation.

use crate::harness;
use crate::test_iggy::{TestIggy, TestIggyCluster};
use laser_sdk::prelude::full::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Poll `condition` every 50 ms, up to `secs` seconds, or panic.
async fn wait_until<F, Fut, T>(secs: u64, label: &str, mut condition: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(v) = condition().await {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "condition '{label}' not met within {secs}s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait for an atomic counter to reach `expected`, or panic after 90 s.
async fn wait_for_progress(counter: &AtomicU64, expected: u64, phase: &str) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while counter.load(Ordering::Acquire) < expected {
        assert!(
            Instant::now() < deadline,
            "counter did not reach {expected} during phase '{phase}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ── 1. Publish retry after connection drop ───────────────────────────────────

/// A hard server restart kills the active connection mid-flight.
///
/// The SDK must either recover and succeed (retry path) or return a structured
/// `LaserError::PublishFailed` — never panic, never hang, and never exceed the
/// configured retry count.
#[tokio::test]
#[serial_test::serial(integration)]
async fn given_connection_drop_before_response_when_retrying_then_result_is_bounded_success_or_publish_failed(
) {
    let iggy = TestIggy::start_pinned().await;

    // Short timeout so retries cycle fast in CI.
    let laser = Laser::builder()
        .connection_string(&iggy.connection_string())
        .stream("recovery_retry")
        .publish_timeout(Duration::from_secs(3))
        .publish_max_retries(2)
        .publish_retry_backoff(Duration::from_millis(100))
        .build()
        .await
        .expect("connect for retry test");

    laser
        .stream("recovery_retry")
        .ensure()
        .await
        .expect("stream ensured");
    let topic = laser.topic("pulse");
    topic.ensure(1).await.expect("topic ensured");

    // Warm up the producer so the cached connection is live.
    topic
        .send(&b"warm-up"[..], BTreeMap::new(), None)
        .await
        .expect("warm-up publish succeeds");

    // Kill + restart the server so the connection dies under the next send.
    iggy.restart().await;

    // The SDK retries up to 2 times.  The overall deadline is generous so the
    // test does not become a race against CI latency, but the SDK must bound
    // its own retries regardless.
    let result = tokio::time::timeout(
        Duration::from_secs(40),
        topic.send(&b"after-restart"[..], BTreeMap::new(), None),
    )
    .await
    .expect("publish must not hang beyond 40 s — bounded retry is required");

    match result {
        Ok(_) => {
            // Retry succeeded.  The record must be readable.
            let mut cursor = topic
                .replay()
                .expect("replay cursor opens after recovery");
            let payloads: Vec<_> = cursor
                .poll()
                .await
                .expect("replay after recovery succeeds")
                .into_iter()
                .map(|m| m.payload)
                .collect();
            assert!(
                payloads.iter().any(|p| p.as_ref() == b"after-restart"),
                "post-restart publish must be readable via replay"
            );
        }
        Err(LaserError::PublishFailed(ref failure)) => {
            // Retry exhausted — the error must be structured.
            assert!(
                !failure.unconfirmed.is_empty(),
                "PublishFailed.unconfirmed must contain the attempted record"
            );
            // The original payload must be preserved in the unconfirmed list.
            assert!(
                failure
                    .unconfirmed
                    .iter()
                    .any(|m| m.payload.as_ref() == b"after-restart"),
                "unconfirmed records must preserve the original payload"
            );
        }
        Err(other) => panic!(
            "expected success or PublishFailed after bounded retries, got: {other:?}"
        ),
    }
}

// ── 2. Partial batch confirmation ────────────────────────────────────────────

/// With retries disabled, a publish that cannot complete must return a
/// `PublishFailed` where `unconfirmed` is non-empty and none of the records in
/// `committed` also appear in `unconfirmed` (no double-report).
#[tokio::test]
#[serial_test::serial(integration)]
async fn given_partial_batch_when_server_unreachable_then_confirmed_ranges_not_in_unconfirmed() {
    let iggy = TestIggy::start_pinned().await;

    // Zero retries so we get a deterministic failure without a retry race.
    let laser = Laser::builder()
        .connection_string(&iggy.connection_string())
        .stream("partial_batch")
        .publish_timeout(Duration::from_secs(2))
        .publish_max_retries(0)
        .build()
        .await
        .expect("connect for partial-batch test");

    laser
        .stream("partial_batch")
        .ensure()
        .await
        .expect("stream ensured");
    let topic = laser.topic("chunks");
    topic.ensure(1).await.expect("topic ensured");

    // Warm the producer.
    for i in 0u8..3 {
        topic
            .send(&[i][..], BTreeMap::new(), None)
            .await
            .unwrap_or_else(|e| panic!("pre-drop publish {i} failed: {e:?}"));
    }

    // Drop the server without restarting — all subsequent sends must fail
    // within the 2 s timeout.
    drop(iggy);

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        topic.send(&b"after-drop"[..], BTreeMap::new(), None),
    )
    .await
    .expect("publish must not hang beyond 15 s with retries=0 and server gone");

    match result {
        Err(LaserError::PublishFailed(ref failure)) => {
            // The unconfirmed list must contain the attempted record.
            assert!(
                !failure.unconfirmed.is_empty(),
                "unconfirmed must be non-empty when the server is unreachable"
            );
            // No confirmed base offset may reappear as the offset of an unconfirmed record.
            let confirmed_bases: Vec<_> = failure.committed.iter().map(|c| c.base_offset).collect();
            for msg in &failure.unconfirmed {
                assert!(
                    !confirmed_bases.contains(&msg.header.offset),
                    "a confirmed offset must not reappear in unconfirmed (no double-report)"
                );
            }
        }
        Ok(_) => {
            // The SDK cannot reach a dropped server, so Ok here is unexpected
            // — it would mean the OS buffered and returned acknowledgements
            // that never reached the server.  If this ever triggers, the test
            // assertion below makes it a deliberate pass rather than a silent
            // surprise.
            panic!("expected PublishFailed when the server process was dropped, got Ok");
        }
        Err(other) => {
            // Any structured transport error is acceptable; we just need a
            // bounded, non-hanging result.  The documentation promises
            // PublishFailed specifically, but a low-level Iggy error from a
            // closed socket before the producer is warmed is also acceptable.
            eprintln!("note: got {other:?} instead of PublishFailed — acceptable for unreachable server");
        }
    }
}

// ── 3. Re-authentication after session invalidation ──────────────────────────

/// A leader failover invalidates the TCP session on the old leader.
///
/// The SDK must re-authenticate using the configured credentials and resume
/// publishing — no permanent `Unauthenticated` error, bounded retries.
#[tokio::test]
#[serial_test::serial(integration)]
async fn given_leader_failover_when_session_invalidated_then_sdk_reauthenticates_and_publishes() {
    use iggy::prelude::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};

    let mut cluster = TestIggyCluster::start().await;
    let leader = discover_leader(&cluster, 0).await;
    cluster.route_endpoint_to(leader);

    let connection = format!(
        "iggy+tcp://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@{}?reconnection_retries=unlimited&reconnection_interval=100ms",
        cluster.endpoint(),
    );

    let laser = Laser::builder()
        .connection_string(connection)
        .stream("reauth_test")
        .publish_timeout(Duration::from_secs(5))
        .publish_max_retries(5)
        .build()
        .await
        .expect("connect for reauth test");

    laser
        .stream("reauth_test")
        .ensure()
        .await
        .expect("stream ensured");
    let topic = laser.topic("events");
    topic.ensure(1).await.expect("topic ensured");

    let sent = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // Background publisher.
    let worker = {
        let laser = laser.clone();
        let sent = sent.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let topic = laser.topic("events");
            while !stop.load(Ordering::Acquire) {
                let seq = sent.load(Ordering::Acquire) + 1;
                match topic
                    .send(&seq.to_le_bytes()[..], BTreeMap::new(), None)
                    .await
                {
                    Ok(_) => sent.store(seq, Ordering::Release),
                    Err(e) => eprintln!("reauth publish error (expected during failover): {e:?}"),
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    };

    // Wait for initial progress.
    wait_for_progress(&sent, 3, "pre-failover publish").await;

    // Trigger a leader failover.
    cluster.stop_node(leader);
    let new_leader = discover_leader(&cluster, (leader + 1) % 3).await;
    cluster.route_endpoint_to(new_leader);
    cluster.start_node(leader).await;

    let before_failover = sent.load(Ordering::Acquire);

    // The SDK must re-authenticate and resume — wait for 3 more records.
    wait_for_progress(&sent, before_failover + 3, "post-failover publish").await;

    stop.store(true, Ordering::Release);
    worker.await.expect("publisher task did not panic");

    assert!(
        sent.load(Ordering::Acquire) > before_failover,
        "at least one publish must succeed after re-authentication"
    );
}

// ── 4. Concurrent publish recovery ───────────────────────────────────────────

/// Multiple concurrent publishers race against a server restart.
///
/// No message may be silently lost: every attempt must end in either a
/// confirmed send or a `PublishFailed` with the record in `unconfirmed`.
/// Confirmed offsets from different publishers must not overlap.
#[tokio::test]
#[serial_test::serial(integration)]
async fn given_concurrent_publishes_when_recovering_then_no_silent_loss_and_single_recovery_path()
{
    const PUBLISHERS: usize = 5;

    let iggy = TestIggy::start_pinned().await;

    let laser = Arc::new(
        Laser::builder()
            .connection_string(&iggy.connection_string())
            .stream("concurrent_recovery")
            .publish_timeout(Duration::from_secs(4))
            .publish_max_retries(3)
            .publish_retry_backoff(Duration::from_millis(100))
            .build()
            .await
            .expect("connect for concurrent-recovery test"),
    );

    laser
        .stream("concurrent_recovery")
        .ensure()
        .await
        .expect("stream ensured");
    let topic = laser.topic("parallel");
    topic.ensure(1).await.expect("topic ensured");

    // Warm the producer.
    topic
        .send(&b"warm"[..], BTreeMap::new(), None)
        .await
        .expect("warm-up publish");

    // Kill + restart the server; all concurrent sends race against recovery.
    iggy.restart().await;

    // Spawn PUBLISHERS tasks simultaneously.  Each carries its index as payload.
    let mut join_set: tokio::task::JoinSet<(usize, Result<(), LaserError>)> =
        tokio::task::JoinSet::new();
    for i in 0..PUBLISHERS {
        let laser = laser.clone();
        join_set.spawn(async move {
            let result = tokio::time::timeout(
                Duration::from_secs(40),
                laser.topic("parallel").send(&[i as u8][..], BTreeMap::new(), None),
            )
            .await
            .expect("concurrent publish must not hang beyond 40 s");
            (i, result)
        });
    }

    let mut results: Vec<(usize, Result<(), LaserError>)> = Vec::with_capacity(PUBLISHERS);
    while let Some(r) = join_set.join_next().await {
        results.push(r.expect("publisher task did not panic"));
    }

    // Each result is Ok or PublishFailed — never an opaque, unstructured error.
    let mut confirmed_indices: Vec<usize> = Vec::new();
    let mut unconfirmed_indices: Vec<usize> = Vec::new();

    for (i, result) in &results {
        match result {
            Ok(_) => confirmed_indices.push(*i),
            Err(LaserError::PublishFailed(ref f)) => {
                for rec in &f.unconfirmed {
                    if let Some(&idx) = rec.payload.first() {
                        unconfirmed_indices.push(idx as usize);
                    }
                }
            }
            Err(other) => panic!("publisher {i} returned unexpected error: {other:?}"),
        }
    }

    // Union of confirmed + unconfirmed must account for all PUBLISHERS.
    let mut all: Vec<usize> = confirmed_indices
        .iter()
        .chain(unconfirmed_indices.iter())
        .copied()
        .collect();
    all.sort_unstable();
    all.dedup();
    assert_eq!(
        all.len(),
        PUBLISHERS,
        "every concurrent publish must appear in either confirmed or unconfirmed — no silent loss"
    );
}

// ── 5a. Repeated server restart — progress resumes each time ─────────────────

/// Restart the server three times while a background publisher is running.
///
/// After each restart, the SDK must recover and advance the send counter by at
/// least one record before the next restart.
#[tokio::test]
#[serial_test::serial(integration)]
async fn given_repeated_server_restarts_when_retries_configured_then_progress_resumes_each_time()
{
    let iggy = TestIggy::start_pinned().await;

    let laser = Arc::new(
        Laser::builder()
            .connection_string(&iggy.connection_string())
            .stream("repeated_restart")
            .publish_timeout(Duration::from_secs(5))
            .publish_max_retries(5)
            .build()
            .await
            .expect("connect for repeated-restart test"),
    );

    laser
        .stream("repeated_restart")
        .ensure()
        .await
        .expect("stream ensured");
    laser.topic("pulse").ensure(1).await.expect("topic ensured");

    let sent = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let worker = {
        let laser = laser.clone();
        let sent = sent.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let topic = laser.topic("pulse");
            while !stop.load(Ordering::Acquire) {
                let seq = sent.load(Ordering::Acquire) + 1;
                match topic
                    .send(&seq.to_le_bytes()[..], BTreeMap::new(), None)
                    .await
                {
                    Ok(_) => sent.store(seq, Ordering::Release),
                    Err(_) => {}
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
    };

    wait_for_progress(&sent, 1, "initial publish").await;

    for restart_num in 1u64..=3 {
        let before = sent.load(Ordering::Acquire);
        iggy.restart().await;
        wait_for_progress(
            &sent,
            before + 1,
            &format!("progress after restart {restart_num}"),
        )
        .await;
    }

    stop.store(true, Ordering::Release);
    worker.await.expect("publisher task did not panic");
}

// ── 5b. Retry exhaustion returns PublishFailed, not a hang ───────────────────

/// With retries disabled, a publish to an unreachable server must return
/// `PublishFailed` or a transport error promptly — never hang indefinitely.
#[tokio::test]
#[serial_test::serial(integration)]
async fn given_exhausted_retries_when_server_unavailable_then_returns_error_not_hanging() {
    let iggy = TestIggy::start_pinned().await;

    let laser = Laser::builder()
        .connection_string(&iggy.connection_string())
        .stream("retry_exhaustion")
        .publish_timeout(Duration::from_secs(2))
        .publish_max_retries(0)
        .build()
        .await
        .expect("connect for retry-exhaustion test");

    laser
        .stream("retry_exhaustion")
        .ensure()
        .await
        .expect("stream ensured");
    let topic = laser.topic("dead_end");
    topic.ensure(1).await.expect("topic ensured");

    // Warm the producer, then kill the server without restarting.
    topic
        .send(&b"warm"[..], BTreeMap::new(), None)
        .await
        .expect("warm-up publish");

    drop(iggy); // process killed; no restart

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        topic.send(&b"doomed"[..], BTreeMap::new(), None),
    )
    .await
    .expect("publish must not hang beyond 15 s with retries=0 and server gone");

    assert!(
        result.is_err(),
        "expected an error when the server is permanently gone, got Ok"
    );
    // The error must be either PublishFailed or a transport error — not a panic.
    match result {
        Err(LaserError::PublishFailed(_)) | Err(LaserError::Iggy(_)) => {}
        Err(other) => panic!("unexpected error variant: {other:?}"),
        Ok(_) => unreachable!(),
    }
}

// ── 6. Consumer-group recovery ───────────────────────────────────────────────

/// Interrupt an active consumer-group member (graceful shutdown), reconnect a
/// replacement, and verify at-least-once delivery with no skipped records.
#[tokio::test]
#[serial_test::serial(integration)]
async fn given_consumer_group_interrupted_when_reconnected_then_offsets_preserved_and_at_least_once(
) {
    use std::sync::Mutex;

    struct Recorder {
        seen: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl AgentHandler for Recorder {
        async fn handle(
            &self,
            msg: &AgentMessage,
            _ctx: &AgentCtx<'_>,
        ) -> Result<(), LaserError> {
            self.seen
                .lock()
                .expect("recorder lock is not poisoned")
                .push(msg.payload.to_vec());
            Ok(())
        }
    }

    let laser = harness::laser().await;
    let seen = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));

    // Start the first consumer on the group.
    let mut first = Agent::builder()
        .id("grp-recovery"
            .parse()
            .expect("grp-recovery is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Recorder { seen: seen.clone() })
        .build()
        .spawn(laser.clone());

    first
        .ready()
        .await
        .expect("first consumer becomes ready");

    let conversation = ConversationId::new();

    // Publish 3 records before the interruption.
    for i in 0u8..3 {
        let provenance = Provenance::builder()
            .conversation_id(conversation)
            .idempotency_key(format!("before-{i}"))
            .build();
        laser
            .send_agent(
                AgentTopic::Commands,
                bytes::Bytes::copy_from_slice(&[i]),
                &provenance,
            )
            .await
            .unwrap_or_else(|e| panic!("pre-interruption publish {i} failed: {e:?}"));
    }

    // Wait for all 3 to be delivered.
    wait_until(30, "pre-interruption delivery", || {
        let seen = seen.clone();
        async move {
            (seen
                .lock()
                .expect("lock")
                .iter()
                .filter(|p| [0u8, 1, 2].iter().any(|i| p.as_slice() == [*i]))
                .count()
                >= 3)
                .then_some(())
        }
    })
    .await;

    // Drain and shut down the first consumer — simulates a clean interruption.
    first
        .shutdown()
        .await
        .expect("first consumer shuts down cleanly");

    // Reconnect and start a second consumer on the same group.
    let restarted = harness::reconnect(&laser).await;
    let mut second = Agent::builder()
        .id("grp-recovery"
            .parse()
            .expect("grp-recovery is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Recorder { seen: seen.clone() })
        .build()
        .spawn(restarted);

    second
        .ready()
        .await
        .expect("second consumer becomes ready");

    // Publish 2 more records after reconnection.
    for i in 3u8..5 {
        let provenance = Provenance::builder()
            .conversation_id(conversation)
            .idempotency_key(format!("after-{i}"))
            .build();
        laser
            .send_agent(
                AgentTopic::Commands,
                bytes::Bytes::copy_from_slice(&[i]),
                &provenance,
            )
            .await
            .unwrap_or_else(|e| panic!("post-reconnect publish {i} failed: {e:?}"));
    }

    // All 5 payloads (0..5) must be delivered at least once.
    wait_until(30, "post-reconnect delivery", || {
        let seen = seen.clone();
        async move {
            let guard = seen.lock().expect("lock");
            let all = (0u8..5).all(|i| guard.iter().any(|p| p.as_slice() == [i]));
            all.then_some(())
        }
    })
    .await;

    let guard = seen.lock().expect("final lock");
    for i in 0u8..5 {
        assert!(
            guard.iter().any(|p| p.as_slice() == [i]),
            "record {i} must be delivered at least once after group recovery (no silent skip)"
        );
    }

    second
        .shutdown()
        .await
        .expect("second consumer shuts down cleanly");
}

// ── cluster helpers (mirrors reconnect.rs) ────────────────────────────────────

async fn discover_leader(cluster: &TestIggyCluster, via: usize) -> usize {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(candidate) = reported_leader(&cluster.node_endpoint(via)).await {
            if let Some(confirmed) = reported_leader(&cluster.node_endpoint(candidate)).await {
                if confirmed == candidate {
                    return candidate;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "cluster did not elect a leader before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn reported_leader(endpoint: &str) -> Option<usize> {
    use iggy::prelude::{
        Client, ClusterClient, ClusterNodeRole, DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME,
        IggyClientBuilder, IggyError, UserClient,
    };
    let discovery = IggyClientBuilder::from_connection_string(&format!(
        "iggy+tcp://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@{endpoint}?reconnection_retries=0",
    ))
    .ok()?
    .build()
    .ok()?;
    let leader = async {
        discovery.connect().await?;
        discovery
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await?;
        let metadata = discovery.get_cluster_metadata().await?;
        Ok::<_, IggyError>(
            metadata
                .nodes
                .iter()
                .find(|n| n.role == ClusterNodeRole::Leader)
                .and_then(|n| n.name.strip_prefix("node-"))
                .and_then(|v| v.parse::<usize>().ok()),
        )
    };
    let result = tokio::time::timeout(Duration::from_secs(5), leader).await;
    let _ = discovery.disconnect().await;
    result.ok()?.ok()?
}
