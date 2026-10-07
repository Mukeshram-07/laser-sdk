# Publish Recovery and Failover Guarantees

This document specifies the behavioral guarantees verified by the cross-SDK chaos tests for publish recovery, failover scenarios, and consumer group resilience.

## Overview

The Laser SDK has experienced multiple production reliability issues across Rust, Python, and TypeScript implementations related to:

- Connection loss and recovery
- Partial batch confirmations
- Re-authentication after connection restarts
- Concurrent recovery attempts
- Consumer-group membership and offset continuity
- Single-node mapped-port routing

This test suite provides deterministic fault injection to verify these guarantees.

## Test Scenarios and Guarantees

### 1. Publish Retry After Connection Drop

**Scenario:** Connection drops before the server sends a publish response.

**Guarantees:**
- The SDK retries the publish with the **same message ID**.
- The result is either success or a structured `PublishFailed` error.
- Retry attempts are bounded (do not retry infinitely).
- The caller receives a clear indication of the final outcome.

**Tested by:** `test_publish_retry_after_connection_drop`

### 2. Partial Batch Confirmation

**Scenario:** A batch is sent where an earlier portion is committed by the server and the remainder times out or fails.

**Guarantees:**
- Confirmed message ranges are **preserved** in the response.
- Only unconfirmed messages are reported for retry.
- Confirmed chunks are **never resent** in a retry attempt.
- The caller can distinguish confirmed from unconfirmed records.

**Tested by:** `test_partial_batch_confirmation`

### 3. Re-Authentication After Unauthenticated Error

**Scenario:** After a connection or leader restart, the server returns an `Unauthenticated` error.

**Guarantees:**
- The SDK **automatically reconnects** and re-authenticates.
- The next publish succeeds without being classified as permanently failed.
- Authentication retries are bounded.
- The caller is not exposed to transient `Unauthenticated` errors.

**Tested by:** `test_reauthentication_after_error`

### 4. Concurrent Publish Recovery

**Scenario:** Multiple publishes are in flight while the connection is recovering from a failure.

**Guarantees:**
- **Only one recovery operation** replaces the connection.
- A newly recovered connection is **not destroyed** by another failed publisher.
- No messages are silently lost.
- Recovery order is preserved.

**Tested by:** `test_concurrent_publish_recovery`

### 5. Repeated Server Restart

**Scenario:** A producer or consumer is active while the test server restarts several times.

**Guarantees:**
- Progress **continues when recovery is possible**.
- When retry attempts are exhausted, a **clear, bounded error** is returned.
- The SDK does **not hang** waiting for recovery.
- Timeout behavior is deterministic and documented.

**Tested by:** `test_repeated_server_restart`

### 6. Consumer-Group Recovery

**Scenario:** A consumer group is active when the connection is interrupted and then restored.

**Guarantees:**
- Group membership is **restored** after reconnect.
- Committed offsets are **preserved** (no offset skips).
- Delivery remains **at-least-once** (no records silently missed).
- Consumer offset continuity is maintained across reconnects.

**Tested by:** `test_consumer_group_recovery`

### 7. Single-Node Mapped-Port Routing

**Scenario:** A reader connects to a single-node Laser/Iggy server using a caller-provided endpoint (e.g., mapped port).

**Guarantees:**
- The SDK uses the **caller-provided endpoint** instead of the advertised internal address.
- If topology discovery fails, the **fallback to the caller endpoint succeeds**.
- Recovery from a later successful topology discovery works correctly.

**Tested by:** `test_mapped_port_routing`

## Cross-SDK Alignment

All three SDK implementations (Rust, Python, TypeScript) must satisfy these guarantees. The BDD scenarios under `bdd/scenarios/` provide shared test definitions that each language implements.

### Rust (Reference Implementation)

Tests are in `sdk/tests/publish_recovery.rs` using Rust integration test patterns.

### Python

Equivalent tests are in `foreign/python/tests/test_publish_recovery.py`.

### TypeScript

Equivalent tests are in `foreign/typescript/tests/publish-recovery.test.ts`.

## Running the Tests

### Local Integration Tests

Requires a running Iggy server (single-node or multi-node):

```bash
# Start Iggy (e.g., via Docker Compose)
just up

# Run Rust tests
just test-it

# Run cross-SDK BDD suite
just bdd
```

### Deterministic Faults

All tests use controlled, deterministic fault injection:

- No fixed sleeps (use `eventually` / polling helpers).
- Server faults are triggered explicitly.
- Connection drops are simulated via test harness.
- Retries and timeouts are configurable and bounded.

## Design Principles

1. **Deterministic:** No timing-based flakes; all faults are controllable and repeatable.
2. **Bounded:** All retries and recovery attempts have explicit limits.
3. **Observable:** Tests assert on specific message IDs, offsets, and error types.
4. **Cross-Language:** Behaviors are aligned across Rust, Python, and TypeScript.
5. **Production-Focused:** Tests cover real failure modes observed in production.

## Future Extensions

As the SDK evolves, this test suite can be extended to cover:

- Consumer filter recovery
- Query result consistency after failover
- KV store operation recovery
- Agent task recovery and state consistency
- Multi-region failover scenarios
