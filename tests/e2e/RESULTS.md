# Local verification — October 10, 2026

Verified on Docker Desktop Kubernetes v1.36.1 (arm64), using Apache Kafka/Connect
4.2.2 and the locally built Healer image. The final complete `run.sh` execution
passed; the disposable stack was left healthy with no fixture connectors or
deliberate-failure markers, and the test port-forwards were closed.

## Passed checks

- All Go tests: race detector, randomized order, three runs; `go vet` passed.
- Docker smoke tests: non-root/read-only runtime, API reads/updates, optional
  authentication, password redaction, attribution, and graceful shutdown.
- Live API: 47 validation/status/header checks, authenticated reads and writes,
  missing/incorrect/malformed credentials, unauthorized writes, password
  redaction, invalid/oversized JSON, and unchanged configuration after rejection.
- Additional live TCP checks: stalled PATCH and PUT request bodies returned 408;
  neither created a cluster nor changed the startup configuration.
- Real Kafka records consumed; failed connectors and tasks restarted and recovered.
- Disabled task remediation, one failed task among three, and paused states.
- Fixed, disabled, exponential, capped, and healthy-reset backoff behavior.
- Injected restart 401/409/500/redirect/connection failures: attempts recorded,
  retries backed off, redirects not followed, subsequent recovery verified.
- Malformed/null/incomplete/500/redirect status responses: no remediation from
  invalid snapshots. Unit regressions verify existing backoff history is retained.
- Runtime interval, timeout, log-level, and task-policy updates.
- Trusted HTTPS Connect authentication and rejected TLS hostname mismatch.
- Same-named connectors in independent clusters; one slow cluster did not block
  another. An audit of 1,356 live action events verified cluster/endpoint/connector/
  action identity, task IDs where applicable, and recorded attempt outcomes.
- Cluster deletion/recreation/replacement, identical PUT, and credential changes.
- Broker interruption: Healer API remained responsive; broker/workers recovered.
- Kubernetes SIGTERM: graceful API/poller shutdown and successful startup afterward.

## Findings and scope

Fixed an application bug where `null` or missing connector status could be
accepted as a valid snapshot and erase backoff history. Empty `{}` snapshots
remain valid. Added explicit restart-request, acceptance, and recorded-attempt
logs, with target identity on each action event.

The test broker is ephemeral. Disabling automatic topic creation prevents old
workers from recreating missing internal topics with the wrong cleanup policy;
Connect creates its compacted internal topics, and the test explicitly provisions
its output topic. After broker replacement the script recreates its fixtures.
This tests interruption/recovery, not Kafka data durability.

HTTP fault responses come from the test gateway around a real worker. A genuine
multi-worker rebalance, production connector plugins, other Kafka versions,
broker authentication, real network partitions, and production TLS/ingress
deployment are not covered by this local run.
