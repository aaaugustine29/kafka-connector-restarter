#!/usr/bin/env bash
# Run against the stack in stack.yaml, not an existing production cluster.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
test_directory=$(mktemp -d)
connect_forward=""
healer_forward=""
shutdown_reader=""
healer_scaled_down=false
broker_scaled_down=false
secondary_forward=""
kube() { kubectl --context docker-desktop -n kafka-connect-healer-e2e "$@"; }
cleanup() {
  result=$?
  if [[ "$result" -ne 0 ]]; then
    kube logs deployment/healer --container healer --tail=35 >&2 || true
    kube logs deployment/connect --tail=35 >&2 || true
  fi
  [[ -z "$connect_forward" ]] || kill "$connect_forward" 2>/dev/null || true
  [[ -z "$healer_forward" ]] || kill "$healer_forward" 2>/dev/null || true
  [[ -z "$shutdown_reader" ]] || kill "$shutdown_reader" 2>/dev/null || true
  [[ -z "$secondary_forward" ]] || kill "$secondary_forward" 2>/dev/null || true
  if [[ "$healer_scaled_down" == true ]]; then
    kube scale deployment/healer --replicas=1 >/dev/null || true
  fi
  if [[ "$broker_scaled_down" == true ]]; then
    kube scale deployment/broker --replicas=1 >/dev/null || true
  fi
  rm -f "$test_directory/connect-forward.log" "$test_directory/healer-forward.log" "$test_directory/ca.crt" "$test_directory/shutdown.log" "$test_directory/response" "$test_directory/headers" "$test_directory/proxy-times" "$test_directory/secondary-forward.log" "$test_directory/proxy-forward.log"
  rmdir "$test_directory"
}
trap cleanup EXIT

kube rollout status deployment/broker --timeout=45s
kube rollout status deployment/connect --timeout=45s
kube rollout status deployment/healer --timeout=45s
kube exec deployment/connect -- cat /opt/kafka/e2e-tls/ca.crt > "$test_directory/ca.crt"
kube port-forward --address 127.0.0.1 service/connect :8443 > "$test_directory/connect-forward.log" 2>&1 &
connect_forward=$!
kube port-forward --address 127.0.0.1 service/healer :8080 > "$test_directory/healer-forward.log" 2>&1 &
healer_forward=$!
for attempt in {1..40}; do
  if grep -q 'Forwarding from' "$test_directory/connect-forward.log" && grep -q 'Forwarding from' "$test_directory/healer-forward.log"; then
    break
  fi
  sleep 0.25
done
connect_port=$(sed -n 's/Forwarding from 127.0.0.1:\([0-9]*\).*/\1/p' "$test_directory/connect-forward.log" | head -1)
healer_port=$(sed -n 's/Forwarding from 127.0.0.1:\([0-9]*\).*/\1/p' "$test_directory/healer-forward.log" | head -1)
[[ -n "$connect_port" && -n "$healer_port" ]]
connect_url="https://localhost:$connect_port"
healer_url="http://127.0.0.1:$healer_port"
connect_api() { curl --fail --silent --show-error --max-time 10 --cacert "$test_directory/ca.crt" --user connect-test:connect-test-only "$@"; }
healer_api() { curl --fail --silent --show-error --max-time 10 --user healer-test:healer-test-only "$@"; }
patch() { healer_api --request PATCH --header 'Content-Type: application/json' --data "$1" "$healer_url/config"; }
status_is() { curl --fail --silent --max-time 10 --cacert "$test_directory/ca.crt" --user connect-test:connect-test-only "$connect_url/connectors/$1/status" | jq -e --arg connector "$2" --arg task "$3" '.connector.state == $connector and .tasks[0].state == $task' >/dev/null; }
wait_status() {
  for attempt in {1..80}; do
    if status_is "$@"; then return; fi
    sleep 0.5
  done
  echo "Timed out waiting for $1 to reach connector=$2 task=$3" >&2
  return 1
}
task_starts() { kube exec deployment/connect -- sh -c 'wc -l < /data/task/task-starts'; }

[[ "$(curl --silent --output /dev/null --write-out '%{http_code}' "$healer_url/config")" == 401 ]]
[[ "$(curl --silent --cacert "$test_directory/ca.crt" --output /dev/null --write-out '%{http_code}' "$connect_url/connectors")" == 401 ]]
connect_api "$connect_url/" | jq -e '.version == "4.2.2"' >/dev/null
connect_api "$connect_url/connector-plugins" | jq -e 'any(.[]; .class == "e2e.HealerTestConnector")' >/dev/null
healer_api "$healer_url/config" | jq -e '(.apiConfig.authConfig | has("password") | not)' >/dev/null
healer_api "$healer_url/clusters" | jq -e '.test.https == true and (.test.authConfig | has("password") | not)' >/dev/null
echo 'PASS: API authentication, trusted HTTPS Connect authentication, and password redaction'

patch '{"pollingBehavior":{"interval":"1s","restartFailedTasks":false,"backoff":{"enabled":true,"baseDelay":"2s","maxDelay":"4s","exponential":true}},"loggingConfig":{"level":"DEBUG"}}'
[[ "$(curl --silent --user healer-test:healer-test-only --output /dev/null --write-out '%{http_code}' \
  --request PATCH --header 'Content-Type: application/json' --data '{"pollingBehavior":{"interval":"0s"}}' "$healer_url/config")" == 400 ]]
healer_api "$healer_url/config" | jq -e '.pollingBehavior.interval == "1s"' >/dev/null
echo 'PASS: valid PATCH applied and invalid PATCH rejected without changing configuration'

# Remove only this fixture's old connectors when rerunning against the test namespace.
for name in task connector multi; do
  response=$(curl --silent --show-error --max-time 10 --cacert "$test_directory/ca.crt" --user connect-test:connect-test-only \
    --request DELETE --output /dev/null --write-out '%{http_code}' "$connect_url/connectors/$name")
  [[ "$response" == 204 || "$response" == 404 ]]
done
kube exec deployment/connect -- sh -c 'mkdir -p /data/task /data/connector; rm -f /data/task/fail-task /data/task/task-starts /data/task/connector-starts /data/connector/fail-connector /data/connector/task-starts /data/connector/connector-starts'
kube exec deployment/broker -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 \
  --create --if-not-exists --topic healer-e2e-records --partitions 1 --replication-factor 1 >/dev/null
connect_api --request POST --header 'Content-Type: application/json' \
  --data '{"name":"task","config":{"connector.class":"e2e.HealerTestConnector","tasks.max":"1","test.id":"task"}}' "$connect_url/connectors" >/dev/null
wait_status task RUNNING RUNNING
record=$(kube exec deployment/broker -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker:9092 \
  --topic healer-e2e-records --from-beginning --max-messages 1 --timeout-ms 15000)
[[ "$record" == record-* ]]
printf '%s\n' "$record"
echo 'PASS: real connector/task running and records consumed from Kafka'

kube exec deployment/connect -- touch /data/task/fail-task
wait_status task RUNNING FAILED
initial_starts=$(task_starts)
sleep 4
[[ "$(task_starts)" == "$initial_starts" ]]
echo 'PASS: failed tasks are not restarted when restartFailedTasks=false'
patch '{"pollingBehavior":{"restartFailedTasks":true}}'
for attempt in {1..60}; do
  if [[ "$(task_starts)" -ge "$((initial_starts + 4))" ]]; then break; fi
  sleep 0.5
done
[[ "$(task_starts)" -ge "$((initial_starts + 4))" ]]
# Task start times independently establish retries and delay growth/capping.
kube exec deployment/connect -- cat /data/task/task-starts | awk '
  NR > 2 { gap = $1 - previous; print "task retry gap:", gap, "ms"; if (gap < 1800 || gap > 6500) exit 1; if (NR > 3 && gap < 3500) exit 1 }
  { previous = $1 }
  END { if (NR < 5) exit 1 }
'
kube exec deployment/connect -- rm /data/task/fail-task
wait_status task RUNNING RUNNING
echo 'PASS: failed-task restarts, exponential/capped backoff, and automatic recovery'

# Healthy status should clear attempts: a new failure retries at the base delay again.
sleep 2
baseline=$(task_starts)
kube exec deployment/connect -- touch /data/task/fail-task
for attempt in {1..40}; do
  if [[ "$(task_starts)" -ge "$((baseline + 2))" ]]; then break; fi
  sleep 0.25
done
[[ "$(task_starts)" -ge "$((baseline + 2))" ]]
kube exec deployment/connect -- tail -n 2 /data/task/task-starts | awk 'NR == 1 { first = $1 } NR == 2 { gap = $1 - first; print "reset backoff retry gap:", gap, "ms"; if (gap < 1800 || gap > 3500) exit 1 }'
kube exec deployment/connect -- rm /data/task/fail-task
wait_status task RUNNING RUNNING
echo 'PASS: healthy task clears backoff history'

kube exec deployment/connect -- touch /data/connector/fail-connector
connect_api --request POST --header 'Content-Type: application/json' \
  --data '{"name":"connector","config":{"connector.class":"e2e.HealerTestConnector","tasks.max":"1","test.id":"connector"}}' "$connect_url/connectors" >/dev/null
for attempt in {1..40}; do
  starts=$(kube exec deployment/connect -- sh -c 'wc -l < /data/connector/connector-starts')
  if [[ "$starts" -ge 3 ]]; then break; fi
  sleep 0.5
done
[[ "$starts" -ge 3 ]]
connect_api "$connect_url/connectors/connector/status" | jq -e '.connector.state == "FAILED"' >/dev/null
kube exec deployment/connect -- rm /data/connector/fail-connector
wait_status connector RUNNING RUNNING
echo 'PASS: failed connector restarted and recovered automatically'

connect_api --request PUT "$connect_url/connectors/task/pause" >/dev/null
wait_status task PAUSED PAUSED
baseline=$(task_starts)
sleep 3
[[ "$(task_starts)" == "$baseline" ]]
connect_api --request PUT "$connect_url/connectors/task/resume" >/dev/null
wait_status task RUNNING RUNNING
echo 'PASS: paused connectors/tasks are left alone'

source tests/e2e/extended.sh

definition='{"host":"connect","port":"8443","https":true,"authConfig":{"enabled":true,"username":"connect-test","password":"connect-test-only"}}'
healer_api --request DELETE "$healer_url/clusters/test"
healer_api "$healer_url/clusters" | jq -e '. == {}' >/dev/null
baseline=$(task_starts)
kube exec deployment/connect -- touch /data/task/fail-task
wait_status task RUNNING FAILED
sleep 3
[[ "$(task_starts)" == "$baseline" ]]
healer_api --request PUT --header 'Content-Type: application/json' --data "$definition" "$healer_url/clusters/test"
for attempt in {1..40}; do
  if [[ "$(task_starts)" -gt "$baseline" ]]; then break; fi
  sleep 0.25
done
[[ "$(task_starts)" -gt "$baseline" ]]
kube exec deployment/connect -- rm /data/task/fail-task
wait_status task RUNNING RUNNING
echo 'PASS: deleting a cluster stops remediation; creating it starts remediation again'

poller_starts=$(kube logs deployment/healer --container healer | grep -c 'msg="polling started"')
healer_api --request PUT --header 'Content-Type: application/json' --data "$definition" "$healer_url/clusters/test"
[[ "$(kube logs deployment/healer --container healer | grep -c 'msg="polling started"')" == "$poller_starts" ]]
wrong_credentials='{"host":"connect","port":"8443","https":true,"authConfig":{"enabled":true,"username":"connect-test","password":"deliberately-wrong"}}'
healer_api --request PUT --header 'Content-Type: application/json' --data "$wrong_credentials" "$healer_url/clusters/test"
sleep 3
kube logs deployment/healer --container healer --tail=30 | grep 'unexpected HTTP status: 401' >/dev/null
healer_api --request PUT --header 'Content-Type: application/json' --data "$definition" "$healer_url/clusters/test"
sleep 2
[[ "$(kube logs deployment/healer --container healer | grep -c 'msg="polling started"')" == "$((poller_starts + 2))" ]]
echo 'PASS: identical PUT does not recreate poller; replacement credentials take effect'

# Finish with healthy connectors and leave the deployed stack available for inspection.
connect_api --request DELETE "$connect_url/connectors/task"
connect_api --request DELETE "$connect_url/connectors/connector"
kube logs deployment/healer --container healer --tail=300 | grep -E 'restart request accepted|backoff|polling stopped' | tail -20

# Capture the outgoing pod's logs before Kubernetes removes it.
kube logs --follow deployment/healer --container healer > "$test_directory/shutdown.log" 2>&1 &
shutdown_reader=$!
for attempt in {1..20}; do
  if [[ -s "$test_directory/shutdown.log" ]]; then break; fi
  sleep 0.25
done
healer_scaled_down=true
kube scale deployment/healer --replicas=0
kube wait --for=delete pod --selector app=healer --timeout=30s
wait "$shutdown_reader"
shutdown_reader=""
grep -q 'API graceful shutdown completed' "$test_directory/shutdown.log"
grep -q 'Kafka Connect Healer stopped' "$test_directory/shutdown.log"
kube scale deployment/healer --replicas=1
healer_scaled_down=false
kube rollout status deployment/healer --timeout=45s
echo 'PASS: Kubernetes SIGTERM gracefully stops polling/API; application starts again'
echo 'All Kafka Connect Healer end-to-end checks passed'
