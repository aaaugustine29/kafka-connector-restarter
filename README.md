# kafka-connector-restarter

Polls the Kafka Connect REST API and restarts failed connectors. When task restarts are enabled, it also restarts failed tasks whose connector is running. Polling begins after the first interval; the service stops on SIGINT or SIGTERM.

## Run

Requires Go 1.27.1 or later. With Kafka Connect available at the default `http://localhost:8083`:

```sh
go run ./cmd
```

To use another Kafka Connect host, set `RESTARTER_CONNECT_HOST` and optionally `RESTARTER_CONNECT_PORT` before running. Use `RESTARTER_CONNECT_HTTPS=true` when connecting over HTTPS.

The HTTP API listens on port 8080 on all interfaces. `GET /` lists the available routes, and `GET /config` returns the current configuration without the password. An API startup or serving failure is logged as an error while polling continues. On SIGINT or SIGTERM, polling requests are canceled and the API has up to five seconds to finish active requests before its connections are closed.

API connections have a five-second header timeout, a ten-second response-write timeout, and a one-minute idle timeout. Restart logs report requests as accepted, since an HTTP success response does not prove that the connector or task has finished restarting. Repeated failure detection and backoff skips are logged at DEBUG; request attempts are logged at INFO and failures at ERROR. Normal shutdown cancellation does not generate polling failure logs.

## Configuration

Startup settings are environment variables. Durations are positive strings with units, such as `250ms`, `10s`, `1.5s`, or `1m30s`. Supported units are `ns`, `us` (or `µs`), `ms`, `s`, `m`, and `h`. Invalid, nonpositive, or overflowing values fall back to the defaults; if the maximum backoff delay is lower than the base delay, it is raised to the base delay.

Duration variables no longer end in `_MS`: for example, replace `RESTARTER_POLL_INTERVAL_MS=10000` with `RESTARTER_POLL_INTERVAL=10s`. The old variable names are no longer read.

| Variable | Default | Purpose |
| --- | --- | --- |
| `RESTARTER_CONNECT_HOST` | `localhost` | Kafka Connect API hostname or IP address. |
| `RESTARTER_CONNECT_PORT` | `8083` | Kafka Connect API port. |
| `RESTARTER_CONNECT_HTTPS` | `false` | Use HTTPS instead of HTTP. |
| `RESTARTER_CONNECT_BASIC_AUTH_ENABLED` | `false` | Enable HTTP Basic authentication. Requires HTTPS and both credentials; otherwise authentication is disabled. |
| `RESTARTER_CONNECT_BASIC_AUTH_USERNAME` | unset | Basic authentication username. |
| `RESTARTER_CONNECT_BASIC_AUTH_PASSWORD` | unset | Basic authentication password. |
| `RESTARTER_HTTP_REQUEST_TIMEOUT` | `10s` | Timeout for each Kafka Connect HTTP request. |
| `RESTARTER_POLL_INTERVAL` | `10s` | Time between polling ticks. |
| `RESTARTER_RESTART_FAILED_TASKS` | `true` | Restart failed tasks when their connector is running. Failed connectors are restarted regardless of this setting. |
| `RESTARTER_RESTART_BACKOFF_ENABLED` | `true` | Skip restart requests that are still within their backoff window. |
| `RESTARTER_RESTART_BACKOFF_BASE_DELAY` | `20s` | Delay after the first restart attempt. |
| `RESTARTER_RESTART_BACKOFF_MAX_DELAY` | `10m` | Maximum delay between attempts. |
| `RESTARTER_RESTART_BACKOFF_EXPONENTIAL_ENABLED` | `true` | Double the delay after each successive attempt, up to the maximum. When false, use the base delay each time. |
| `RESTARTER_LOG_LEVEL` | `INFO` | Minimum log level (for example `DEBUG`, `INFO`, `WARN`, or `ERROR`). |

Backoff is tracked separately for each connector restart and each task restart. With exponential backoff enabled, the delay after attempt number `n` is `min(base delay × 2^(n−1), maximum delay)`. For example, the default delays begin at 20 seconds, then 40 seconds, 80 seconds, and so on up to 10 minutes. Every outbound restart request counts as an attempt, including requests that return errors. A `RUNNING` connector clears its connector backoff, and a `RUNNING` task clears that task's backoff; a future failure then starts again at the base delay. If status retrieval fails, no actions or resets happen on that poll.

Configuration JSON uses the same duration-string format while retaining the existing field names. For example, `pollingBehavior.interval` is `"10s"`, `communicationConfig.requestTimeout` is `"500ms"`, and `pollingBehavior.backoff.maxDelay` is `"10m0s"`. Output uses Go's canonical duration spelling; for example, `10m` is written as `10m0s`. Numeric JSON durations are no longer accepted. Runtime updates reject invalid configuration without changing the stored values. The GET config handler omits the authentication password.

## Tests

```sh
go test ./...
```
