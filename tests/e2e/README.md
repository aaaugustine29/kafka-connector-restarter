# Docker Desktop Kubernetes end-to-end test

This is a disposable local integration stack, not a production deployment. It uses
Apache Kafka/Connect 4.2.2, one combined KRaft broker/controller, two independent
distributed Connect workers, a fault-injection gateway, and the locally built
Kafka Connect Healer application. No images
are pushed. All resources are scoped to `kafka-connect-healer-e2e`, and commands
explicitly select the `docker-desktop` context.

Requirements: Docker Desktop with Kubernetes enabled, kubectl, Bash, curl, jq,
and enough Docker Desktop memory for three Kafka JVMs (allow at least 4 GiB plus Kubernetes).
Docker Desktop must expose locally built Docker images to its Kubernetes runtime.
Do not deploy this manifest if the namespace already contains unrelated resources.

Run from the repository root:

```sh
docker build -t kafka-connect-healer:e2e .
docker build -f tests/e2e/Dockerfile.connect -t kafka-connect-healer-connect:e2e tests/e2e
kubectl --context docker-desktop apply -f tests/e2e/stack.yaml
bash tests/e2e/run.sh
```

The Java fixture is a genuine SourceConnector running inside Kafka Connect. It
produces records and deliberately throws ConnectException when marker files are
present. This provides repeatable connector/task failures without relying on a
third-party system. The fixture records actual connector/task start timestamps,
which the script checks independently of Healer's logs.

Checks cover all API routes with missing/incorrect credentials, unauthorized writes,
password redaction, response headers/status codes, malformed/oversized JSON and
invalid configuration, runtime interval/timeout/log-level changes, Kafka Connect
Basic Auth over trusted HTTPS, TLS hostname validation, Kafka data flow, disabled
task remediation, single-task and multi-task restart/recovery, fixed/disabled/
exponential/capped backoff and healthy-state reset, connector restart/recovery,
paused states, cluster removal/recreation/replacement, identical PUT handling,
credential updates, independent clusters with identical connector names, a slow
cluster alongside a responsive cluster, broker interruption, and Kubernetes
SIGTERM/shutdown/restart.

The HTTPS gateway forwards normal requests to the real worker. Its injected
401/409/500/redirect/disconnection responses verify attempt recording and retry
timing. Invalid/null/incomplete status snapshots and slow status responses verify
error handling. These synthetic HTTP faults are not a claim that a real broker
rebalance or network partition was reproduced.

Only fixture connectors named `task`, `connector`, and `multi` are removed/recreated.
Successful runs delete these connectors, terminate their local port-forwards, and
leave the stack running for inspection. A failed run prints logs and closes its
port-forwards, but may leave deliberate-failure markers/connectors in place; rerun
the test after fixing the cause to reset those fixtures.

Inspect the deployed stack:

```sh
kubectl --context docker-desktop -n kafka-connect-healer-e2e get pods
kubectl --context docker-desktop -n kafka-connect-healer-e2e logs deployment/healer -c healer --tail=100
kubectl --context docker-desktop -n kafka-connect-healer-e2e port-forward service/healer 18080:8080
```

In another terminal:

```sh
curl -u healer-test:healer-test-only http://127.0.0.1:18080/config
curl -u healer-test:healer-test-only http://127.0.0.1:18080/clusters
```

These credentials are deliberately fake and committed as test fixtures. Do not
reuse them. This test uses ConfigMaps for fake passwords and bakes a self-signed
certificate/private key into the local Connect fixture image. The certificate
lasts 30 days from image creation; rebuild that fixture with `--no-cache` after it
expires. Real deployments must use Secrets, appropriate certificate management,
and production-ready authentication. Only the test CA is added to Healer's trust
store; TLS verification is not disabled. Kafka broker traffic is plaintext.

Kafka data and Connect failure-marker data are ephemeral; pod replacement loses
them. Multi-worker Connect groups, broker authentication, real network partitions,
production connector plugins, and other Kafka versions require separate tests.
This stack is not automatically deployed by CI.

See [RESULTS.md](RESULTS.md) for the last verified local run and its limitations.

When finished, remove only the disposable test namespace (this deletes its test
Kafka topics, connectors, and other resources):

```sh
kubectl --context docker-desktop delete namespace kafka-connect-healer-e2e
```
