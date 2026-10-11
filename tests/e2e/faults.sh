# Faults are injected at a test gateway; ordinary requests reach real Kafka Connect.
definition='{"host":"connect","port":"8443","https":true,"authConfig":{"enabled":true,"username":"connect-test","password":"connect-test-only"}}'
proxy_definition='{"host":"connect","port":"8444","https":true,"authConfig":{"enabled":true,"username":"connect-test","password":"connect-test-only"}}'
replace_test() { healer_api --request PUT --header 'Content-Type: application/json' --data "$1" "$healer_url/clusters/test"; }
mode() { kube exec deployment/connect -- sh -c 'printf "%s" "$1" > /data/proxy-mode.new; mv /data/proxy-mode.new /data/proxy-mode' sh "$1"; }
proxy_restarts() { kube exec deployment/connect -- awk -v mode="$1" '$2 == "POST" && $3 ~ /\/restart/ && $4 == mode { n++ } END { print n+0 }' /data/proxy-requests; }
wait_proxy_restarts() {
  for attempt in {1..80}; do
    if [[ "$(proxy_restarts "$1")" -ge "$2" ]]; then return; fi
    sleep 0.25
  done
  echo "No expected retries for $1" >&2
  return 1
}
mode pass
kube exec deployment/connect -- touch /data/proxy-requests
replace_test "$proxy_definition"
sleep 2

for fault in 401 409 500 redirect disconnect; do
  mode "restart-$fault"
  # Replacing the worker gives each case fresh backoff history.
  replace_test "$definition"
  replace_test "$proxy_definition"
  baseline=$(task_starts)
  before_requests=$(proxy_restarts "restart-$fault")
  kube exec deployment/connect -- touch /data/task/fail-task
  wait_status task RUNNING FAILED
  wait_proxy_restarts "restart-$fault" "$((before_requests + 4))"
  [[ "$(task_starts)" == "$baseline" ]]
  kube exec deployment/connect -- awk -v mode="restart-$fault" '$2 == "POST" && $3 ~ /\/restart/ && $4 == mode { print $1 }' /data/proxy-requests \
    | tail -n 4 > "$test_directory/proxy-times"
  awk 'NR > 1 { gap = $1 - previous; print "failed HTTP retry:", gap, "ms"; if (gap < 1800 || gap > 6500) exit 1; if (NR > 2 && gap < 3500) exit 1 } { previous = $1 } END { if (NR != 4) exit 1 }' "$test_directory/proxy-times"
  if [[ "$fault" == redirect ]]; then
    [[ "$(kube exec deployment/connect -- awk '$3 == "/redirect-target" { n++ } END { print n+0 }' /data/proxy-requests)" == 0 ]]
  fi
  kube logs deployment/healer --container healer --tail=90 | grep 'restart attempt recorded.*attempt_count=4' >/dev/null
  kube logs deployment/healer --container healer --tail=90 | grep 'remediation request failed' >/dev/null
  kube exec deployment/connect -- rm /data/task/fail-task
  mode pass
  wait_status task RUNNING RUNNING
  sleep 2
  echo "PASS: restart $fault response/error recorded as an attempt, backed off, and later recovered"
done

# Decode/fetch failure must not act or treat the snapshot as healthy/empty.
for fault in malformed null missing 500 redirect; do
  mode "status-$fault"
  before_requests=$(kube exec deployment/connect -- awk '$2 == "POST" { n++ } END { print n+0 }' /data/proxy-requests)
  baseline=$(task_starts)
  kube exec deployment/connect -- touch /data/task/fail-task
  wait_status task RUNNING FAILED
  sleep 3
  [[ "$(task_starts)" == "$baseline" ]]
  [[ "$(kube exec deployment/connect -- awk '$2 == "POST" { n++ } END { print n+0 }' /data/proxy-requests)" == "$before_requests" ]]
  kube logs deployment/healer --container healer --tail=30 | grep 'poll cycle failed while retrieving connector statuses' >/dev/null
  pass_requests=$(proxy_restarts pass)
  mode pass
  wait_proxy_restarts pass "$((pass_requests + 1))"
  kube exec deployment/connect -- rm /data/task/fail-task
  wait_status task RUNNING RUNNING
  sleep 2
  echo "PASS: status $fault does not remediate or accept a corrupted snapshot"
done

# Bounded request timeout and application configuration updates still work.
patch '{"communicationConfig":{"requestTimeout":"400ms"}}'
mode status-delay
sleep 3
kube logs deployment/healer --container healer --tail=30 | grep 'Client.Timeout' >/dev/null
patch '{"pollingBehavior":{"interval":"2s"}}'
healer_api "$healer_url/config" | jq -e '.pollingBehavior.interval == "2s"' >/dev/null
mode pass
patch '{"communicationConfig":{"requestTimeout":"3s"},"pollingBehavior":{"interval":"1s"}}'
sleep 2
echo 'PASS: timeout bounds slow requests and does not deadlock runtime updates'

# Hostname mismatch must fail rather than disabling TLS verification.
replace_test '{"host":"connect.kafka-connect-healer-e2e.svc","port":"8443","https":true,"authConfig":{"enabled":true,"username":"connect-test","password":"connect-test-only"}}'
sleep 3
kube logs deployment/healer --container healer --tail=30 | grep 'certificate is valid for' >/dev/null
replace_test "$definition"
sleep 2
echo 'PASS: TLS hostname verification rejects an invalid server identity'

# Multi-cluster isolation with independent Connect groups and identical connector names.
kube rollout status deployment/connect-secondary --timeout=45s
kube port-forward --address 127.0.0.1 service/connect-secondary :8443 > "$test_directory/secondary-forward.log" 2>&1 &
secondary_forward=$!
for attempt in {1..40}; do
  if grep -q 'Forwarding from' "$test_directory/secondary-forward.log"; then break; fi
  sleep 0.25
done
secondary_port=$(sed -n 's/Forwarding from 127.0.0.1:\([0-9]*\).*/\1/p' "$test_directory/secondary-forward.log" | head -1)
[[ -n "$secondary_port" ]]
secondary_url="https://localhost:$secondary_port"
connect_api --request POST --header 'Content-Type: application/json' --data '{"name":"task","config":{"connector.class":"e2e.HealerTestConnector","tasks.max":"1","test.id":"task"}}' "$secondary_url/connectors" >/dev/null
for attempt in {1..60}; do
  if connect_api "$secondary_url/connectors/task/status" | jq -e '.tasks[0].state == "RUNNING"' >/dev/null; then break; fi
  sleep 0.5
done
expect_api 201 PUT /clusters/secondary '{"host":"connect-secondary","port":"8443","https":true,"authConfig":{"enabled":true,"username":"connect-test","password":"connect-test-only"}}'
kube exec deployment/connect-secondary -- touch /data/task/fail-task
sleep 4
[[ "$(kube exec deployment/connect-secondary -- sh -c 'wc -l < /data/task/task-starts')" -gt 1 ]]
primary_starts=$(task_starts)
sleep 2
[[ "$(task_starts)" == "$primary_starts" ]]
# A status timeout in one cluster must not hold up a different cluster.
replace_test "$proxy_definition"
mode status-delay
patch '{"communicationConfig":{"requestTimeout":"5s"}}'
secondary_starts=$(kube exec deployment/connect-secondary -- sh -c 'wc -l < /data/task/task-starts')
sleep 6
[[ "$(kube exec deployment/connect-secondary -- sh -c 'wc -l < /data/task/task-starts')" -gt "$secondary_starts" ]]
kube exec deployment/connect-secondary -- rm /data/task/fail-task
mode pass
replace_test "$definition"
patch '{"communicationConfig":{"requestTimeout":"3s"}}'
expect_api 204 DELETE /clusters/secondary
connect_api --request DELETE "$secondary_url/connectors/task"
kill "$secondary_forward"
wait "$secondary_forward" 2>/dev/null || true
secondary_forward=""
echo 'PASS: same-named connectors have independent cluster state; a slow cluster does not block another'

# Verify identities on interleaved logs, not just that a message exists somewhere.
kube logs deployment/healer --container healer | awk '
  /msg="(remediation action determined|requesting (connector|task) restart|(connector|task) restart request accepted|restart attempt recorded|remediation request failed|(connector|task) restart skipped because it is in the backoff window)"/ {
    if ($0 !~ /connect_cluster=/ || $0 !~ /endpoint=/ || $0 !~ /connector=/ || $0 !~ /action=restart_(connector|task)/) exit 1
    if ($0 ~ /action=restart_task/ && $0 !~ /task_id=[0-9]+/) exit 1
    if ($0 ~ /action=restart_connector/ && $0 ~ /task_id=/) exit 1
    if ($0 ~ /msg="restart attempt recorded"/ && ($0 !~ /attempt_count=[1-9][0-9]*/ || $0 !~ /attempted_at=/ || $0 !~ /status_code=/)) exit 1
    if ($0 ~ /connect_cluster=test .*connector=task/) primary++
    if ($0 ~ /connect_cluster=secondary .*connector=task/) secondary++
    records++
  }
  END { if (records == 0 || primary == 0 || secondary == 0) exit 1; print "PASS:", records, "action events retain cluster, endpoint, connector, action, task identity, and attempt outcomes" }
'

# Broker interruption is recoverable without crashing the Healer API.
kube scale deployment/broker --replicas=0
broker_scaled_down=true
kube wait --for=delete pod --selector app=broker --timeout=30s
expect_api 200 GET /config
kube scale deployment/broker --replicas=1
broker_scaled_down=false
kube rollout status deployment/broker --timeout=45s
kube exec deployment/broker -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 \
  --create --if-not-exists --topic healer-e2e-records --partitions 1 --replication-factor 1 >/dev/null
# Kafka data is deliberately ephemeral here. Restart both Connect workers to
# recreate internal topics against the new single-broker cluster.
kube rollout restart deployment/connect deployment/connect-secondary
kube rollout status deployment/connect --timeout=45s
kube rollout status deployment/connect-secondary --timeout=45s
# Port-forwards target a specific pod; replacing the worker invalidates the old one.
kill "$connect_forward" 2>/dev/null || true
wait "$connect_forward" 2>/dev/null || true
kube port-forward --address 127.0.0.1 service/connect :8443 > "$test_directory/connect-forward.log" 2>&1 &
connect_forward=$!
for attempt in {1..40}; do
  if grep -q 'Forwarding from' "$test_directory/connect-forward.log"; then break; fi
  sleep 0.25
done
connect_port=$(sed -n 's/Forwarding from 127.0.0.1:\([0-9]*\).*/\1/p' "$test_directory/connect-forward.log" | head -1)
[[ -n "$connect_port" ]]
connect_url="https://localhost:$connect_port"
for name in task connector; do
  connect_api --request POST --header 'Content-Type: application/json' \
    --data "{\"name\":\"$name\",\"config\":{\"connector.class\":\"e2e.HealerTestConnector\",\"tasks.max\":\"1\",\"test.id\":\"$name\"}}" "$connect_url/connectors" >/dev/null
  wait_status "$name" RUNNING RUNNING
done
echo 'PASS: broker interruption leaves the Healer API responsive; test broker/workers recover'
