# Sourced by run.sh with its authenticated APIs, port-forwards, and cleanup trap.
api_checks=0
expect_api() {
  local expected="$1" method="$2" route="$3" payload="${4-}" content_type="${5-application/json}"
  local actual
  actual=$(curl --silent --show-error --max-time 12 --user healer-test:healer-test-only \
    --request "$method" --header "Content-Type: $content_type" --data "$payload" \
    --output "$test_directory/response" --dump-header "$test_directory/headers" \
    --write-out '%{http_code}' "$healer_url$route")
  if [[ "$actual" != "$expected" ]]; then
    echo "FAIL: $method $route returned $actual, expected $expected" >&2
    cat "$test_directory/response" >&2
    return 1
  fi
  api_checks=$((api_checks + 1))
}

# All routes require auth, not just GET /config.
for route in / /config /clusters; do
  [[ "$(curl --silent --max-time 5 --output /dev/null --write-out '%{http_code}' "$healer_url$route")" == 401 ]]
  [[ "$(curl --silent --max-time 5 --user wrong:wrong --output /dev/null --write-out '%{http_code}' "$healer_url$route")" == 401 ]]
  for authorization in 'Basic not-base64' 'Bearer token' 'Basic Og=='; do
    [[ "$(curl --silent --max-time 5 --header "Authorization: $authorization" --output /dev/null --write-out '%{http_code}' "$healer_url$route")" == 401 ]]
  done
done
for operation in 'PATCH /config' 'PUT /clusters/denied' 'DELETE /clusters/denied'; do
  method=${operation%% *}
  route=${operation#* }
  [[ "$(curl --silent --max-time 5 --request "$method" --output /dev/null --write-out '%{http_code}' "$healer_url$route")" == 401 ]]
  [[ "$(curl --silent --max-time 5 --user wrong:wrong --request "$method" --output /dev/null --write-out '%{http_code}' "$healer_url$route")" == 401 ]]
done
healer_api "$healer_url/clusters" | jq -e 'has("denied") | not' >/dev/null
expect_api 200 GET /config
grep -qi '^Cache-Control: no-store' "$test_directory/headers"
expect_api 200 GET /clusters
grep -qi '^Cache-Control: no-store' "$test_directory/headers"
expect_api 404 GET /does-not-exist
expect_api 405 POST /config '{}'
expect_api 405 POST /clusters '{}'
expect_api 404 DELETE /clusters/missing

before_configuration=$(healer_api "$healer_url/config")
for payload in \
  '' '[]' 'null' '{}' '{}' \
  '{"unknown":1}' '{"pollingBehavior":null}' \
  '{"pollingBehavior":{"interval":"1s","interval":"2s"}}' \
  '{"pollingBehavior":{"interval":"1s","Interval":"2s"}}' \
  '{"pollingBehavior":{"interval":"-1s"}}' \
  '{"pollingBehavior":{"interval":1000}}' \
  '{"communicationConfig":{"requestTimeout":"0s"}}' \
  '{"pollingBehavior":{"backoff":{"baseDelay":"0s"}}}' \
  '{"pollingBehavior":{"backoff":{"maxDelay":"1s"}}}' \
  '{"pollingBehavior":{"restartFailedTasks":"yes"}}' \
  '{"loggingConfig":{"level":"NOT-A-LEVEL"}}' \
  '{"apiConfig":{"authConfig":{"enabled":false}}}' \
  '{"apiConfig":{"authConfig":{"password":"must-not-leak"},"unknown":1}}' \
  '{} {}'; do
  if [[ "$payload" == '{}' ]]; then
    expect_api 204 PATCH /config "$payload"
  else
    expect_api 400 PATCH /config "$payload"
    ! grep -q 'must-not-leak' "$test_directory/response"
  fi
  [[ "$(healer_api "$healer_url/config")" == "$before_configuration" ]]
done
expect_api 415 PATCH /config '{}' text/plain
expect_api 415 PATCH /config '{}' ''
oversized=$(printf '%65540s' '')
expect_api 413 PATCH /config "$oversized"
expect_api 400 PATCH /config $'{"loggingConfig":{"level":"\xff"}}'
expect_api 204 PATCH /config '{"PollingBehavior":{"Interval":"1s"}}' 'application/json; charset=utf-8'

for payload in \
  '' '{}' '[]' 'null' \
  '{"host":"connect","port":"8443","unknown":true}' \
  '{"host":"connect","host":"connect","port":"8443"}' \
  '{"host":"connect","port":8443}' \
  '{"host":"https://connect","port":"8443"}' \
  '{"host":"connect","port":"0"}' \
  '{"host":"connect","port":"65536"}' \
  '{"host":"connect","port":"8443","https":null}' \
  '{"host":"connect","port":"8443","authConfig":{"enabled":true,"username":"u","password":"must-not-leak"}}' \
  '{"host":"connect","port":"8443","https":true,"authConfig":{"enabled":true,"username":"u"}}'; do
  expect_api 400 PUT /clusters/rejected "$payload"
  ! grep -q 'must-not-leak' "$test_directory/response"
done
expect_api 415 PUT /clusters/rejected '{}' text/plain
expect_api 413 PUT /clusters/rejected "$oversized"
healer_api "$healer_url/clusters" | jq -e 'has("rejected") | not' >/dev/null
expect_api 201 PUT '/clusters/name%20with%20space' '{"host":"connect","port":"8443"}'
grep -qi '^Location: /clusters/name%20with%20space' "$test_directory/headers"
expect_api 204 DELETE '/clusters/name%20with%20space'
echo "PASS: $api_checks API validation/status/header cases and unauthorized write protection"

# Runtime ticker and logging updates, including preserving omitted fields.
patch '{"pollingBehavior":{"interval":"5s"},"communicationConfig":{"requestTimeout":"2s"},"loggingConfig":{"level":"WARN"}}'
sleep 1
debug_before=$(kube logs deployment/healer --container healer | grep -c 'level=DEBUG')
sleep 2
[[ "$(kube logs deployment/healer --container healer | grep -c 'level=DEBUG')" == "$debug_before" ]]
healer_api "$healer_url/config" | jq -e '.pollingBehavior.interval == "5s" and .communicationConfig.requestTimeout == "2s" and .pollingBehavior.restartFailedTasks == true' >/dev/null
patch '{"pollingBehavior":{"interval":"1s"},"communicationConfig":{"requestTimeout":"3s"},"loggingConfig":{"level":"DEBUG"}}'
sleep 2
[[ "$(kube logs deployment/healer --container healer | grep -c 'level=DEBUG')" -gt "$debug_before" ]]
echo 'PASS: runtime interval, timeout, and log-level updates; omitted fields preserved'

# Fixed and disabled backoff on a real FAILED task.
patch '{"pollingBehavior":{"backoff":{"exponential":false,"baseDelay":"2s","maxDelay":"4s"}}}'
baseline=$(task_starts)
kube exec deployment/connect -- touch /data/task/fail-task
for attempt in {1..60}; do
  if [[ "$(task_starts)" -ge "$((baseline + 4))" ]]; then break; fi
  sleep 0.25
done
[[ "$(task_starts)" -ge "$((baseline + 4))" ]]
kube exec deployment/connect -- tail -n 3 /data/task/task-starts | awk 'NR > 1 { gap = $1 - previous; print "fixed backoff:", gap, "ms"; if (gap < 1800 || gap > 3500) exit 1 } { previous = $1 }'
patch '{"pollingBehavior":{"backoff":{"enabled":false}}}'
baseline=$(task_starts)
sleep 4
[[ "$(task_starts)" -ge "$((baseline + 3))" ]]
patch '{"pollingBehavior":{"restartFailedTasks":false}}'
sleep 1
baseline=$(task_starts)
sleep 3
[[ "$(task_starts)" == "$baseline" ]]
kube exec deployment/connect -- rm /data/task/fail-task
patch '{"pollingBehavior":{"restartFailedTasks":true,"backoff":{"enabled":true,"exponential":true}}}'
wait_status task RUNNING RUNNING
sleep 2
echo 'PASS: fixed/disabled backoff and disabling task restarts at runtime'

# Several tasks: only the failed task is restarted.
kube exec deployment/connect -- mkdir -p /data/multi
connect_api --request POST --header 'Content-Type: application/json' --data '{"name":"multi","config":{"connector.class":"e2e.HealerTestConnector","tasks.max":"3","test.tasks":"3","test.id":"multi"}}' "$connect_url/connectors" >/dev/null
for attempt in {1..40}; do
  if connect_api "$connect_url/connectors/multi/status" | jq -e '.tasks | length == 3 and all(.[]; .state == "RUNNING")' >/dev/null; then break; fi
  sleep 0.5
done
connect_api "$connect_url/connectors/multi/status" | jq -e '.tasks | length == 3 and all(.[]; .state == "RUNNING")' >/dev/null
healthy_zero=$(kube exec deployment/connect -- sh -c 'wc -l < /data/multi/task-0-starts')
healthy_two=$(kube exec deployment/connect -- sh -c 'wc -l < /data/multi/task-2-starts')
failed_one=$(kube exec deployment/connect -- sh -c 'wc -l < /data/multi/task-1-starts')
kube exec deployment/connect -- touch /data/multi/fail-task-1
sleep 5
[[ "$(kube exec deployment/connect -- sh -c 'wc -l < /data/multi/task-1-starts')" -gt "$failed_one" ]]
[[ "$(kube exec deployment/connect -- sh -c 'wc -l < /data/multi/task-0-starts')" == "$healthy_zero" ]]
[[ "$(kube exec deployment/connect -- sh -c 'wc -l < /data/multi/task-2-starts')" == "$healthy_two" ]]
kube exec deployment/connect -- rm /data/multi/fail-task-1
for attempt in {1..40}; do
  if connect_api "$connect_url/connectors/multi/status" | jq -e 'all(.tasks[]; .state == "RUNNING")' >/dev/null; then break; fi
  sleep 0.5
done
connect_api "$connect_url/connectors/multi/status" | jq -e 'all(.tasks[]; .state == "RUNNING")' >/dev/null
connect_api --request DELETE "$connect_url/connectors/multi"
echo 'PASS: one failed task among three is restarted without touching healthy tasks'

source tests/e2e/faults.sh
