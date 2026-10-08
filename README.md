# kafka-connector-restarter

Polls the Kafka Connect REST API and restarts failed connectors. When task restarts are enabled, it also restarts failed tasks whose connector is running. Polling begins after the first interval; the service stops on SIGINT or SIGTERM.

## Run

Requires Go 1.27.1 or later. Supply a base YAML configuration file when running:

```sh
go run ./cmd --config=/path/to/config.yaml
```

The repository does not include configuration files. A base file containing `{}` uses all application defaults, including the Kafka Connect endpoint `http://localhost:8083`. To also load credentials from a Secret YAML overlay:

```sh
go run ./cmd --config=/path/to/config.yaml --secret-config=/path/to/secret-config.yaml
```

`--config` defaults to `config.yaml`, relative to the working directory. The base file is required. `--secret-config` is optional, but when supplied its file is required too. Unreadable or invalid files cause startup to fail before the HTTP server or polling starts.

The HTTP API listens on port 8080 on all interfaces. `GET /` lists the available routes, and `GET /config` returns the current configuration without the password. An API startup or serving failure is logged as an error while polling continues. On SIGINT or SIGTERM, polling requests are canceled and the API has up to five seconds to finish active requests before its connections are closed.

API connections have a five-second header timeout, a ten-second response-write timeout, and a one-minute idle timeout. Restart logs report requests as accepted, since an HTTP success response does not prove that the connector or task has finished restarting. Repeated failure detection and backoff skips are logged at DEBUG; request attempts are logged at INFO and failures at ERROR. Normal shutdown cancellation does not generate polling failure logs.

## Configuration

Configuration is read once at startup, in this order:

1. Application defaults.
2. The base YAML file, normally mounted from a Kubernetes ConfigMap.
3. The optional Secret YAML overlay, normally mounted from a Kubernetes Secret.

Both files use the same schema. Each explicitly supplied field replaces its previous value, including `false` and empty strings. Omitted fields retain their previous values; an empty mapping (`{}`) changes nothing. A supplied nested mapping only changes the fields it contains, rather than replacing the entire section. The overlay can change any configuration field, although its intended purpose is supplying credentials.

The merged configuration is validated after both files have been loaded, so a base file can enable authentication and leave credentials for the overlay. Keep actual credentials outside version control and out of ConfigMaps. A Secret overlay can supply credentials as follows:

```yaml
connectConfig:
  authConfig:
    enabled: true
    username: replace-me
    password: replace-me
```

Basic authentication requires `connectConfig.https: true`, a nonblank username, and a nonempty password. Invalid combinations fail startup; authentication is never silently disabled.

The available settings and defaults are:

| YAML field | Default |
| --- | --- |
| `connectConfig.host` | `localhost` |
| `connectConfig.port` | `"8083"` |
| `connectConfig.https` | `false` |
| `connectConfig.authConfig.enabled` | `false` |
| `connectConfig.authConfig.username` | empty string |
| `connectConfig.authConfig.password` | empty string |
| `communicationConfig.requestTimeout` | `10s` |
| `pollingBehavior.interval` | `10s` |
| `pollingBehavior.restartFailedTasks` | `true` |
| `pollingBehavior.backoff.enabled` | `true` |
| `pollingBehavior.backoff.baseDelay` | `20s` |
| `pollingBehavior.backoff.maxDelay` | `10m` |
| `pollingBehavior.backoff.exponential` | `true` |
| `loggingConfig.level` | `INFO` |

`connectConfig.host` is a hostname or IP address, and `connectConfig.port` is a string such as `"8083"`. `loggingConfig.level` accepts Go slog levels such as `DEBUG`, `INFO`, `WARN`, and `ERROR`.

Durations are positive strings with units, such as `250ms`, `10s`, `1.5s`, or `1m30s`. Supported units are `ns`, `us` (or `µs`), `ms`, `s`, `m`, and `h`. Numeric, invalid, nonpositive, or overflowing durations are rejected. The maximum backoff delay must be at least the base delay. The default base delay is `20s`, independently of any configured polling interval.

Each file must contain exactly one YAML mapping document. Unknown keys, duplicate keys, null values, aliases, and YAML merge keys are rejected. Use `{}` for a file that intentionally keeps existing values. Decoder errors omit raw values to prevent credentials from reaching logs.

### Migration from environment variables

The application no longer reads `RESTARTER_*` environment variables. Move their values into the corresponding YAML fields and mount the file. For example, `RESTARTER_POLL_INTERVAL=30s` becomes `pollingBehavior.interval: 30s`, and the basic-auth username and password become `connectConfig.authConfig.username` and `connectConfig.authConfig.password` in the Secret overlay. Remove the old environment entries from the Deployment to avoid misleading configuration.

## Kubernetes

Mount the base YAML file from a ConfigMap and the optional overlay from a Secret, using read-only directory mounts. Pass their paths through `--config` and `--secret-config` in the container arguments. The image entrypoint must launch the application binary. If authentication is unnecessary, omit the Secret and `--secret-config` argument.

For Kustomize deployments, generate the ConfigMap and Secret from the two YAML files and keep generated name hashes enabled. Changes to either input file generate a new resource name and update the Deployment's volume reference when applied, triggering replacement of the pod. This makes base configuration and credential changes follow the same deployment process. See [Kustomize generators](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/kustomization/).

Use one replica and the `Recreate` strategy to avoid overlapping pollers issuing duplicate restart requests during updates. Updates include a brief polling interruption and reset the application's in-memory restart backoff history.

For Helm deployments, mount the same two files and use pod-template checksum annotations for both chart-managed ConfigMaps and Secrets. Independently managed Secret changes also need an explicit pod restart or a deployment controller that triggers one. See [Helm's rollout guidance](https://helm.sh/docs/howto/charts_tips_and_tricks/#automatically-roll-deployments).

Configuration is only read at startup. Updating a mounted file does not change the running application's configuration; restart the pod to apply it. Mounting files does not require the application to access the Kubernetes API. See [ConfigMaps](https://kubernetes.io/docs/concepts/configuration/configmap/) and [Secrets](https://kubernetes.io/docs/concepts/configuration/secret/).

## Restart backoff

Backoff is tracked separately for each connector restart and each task restart. With exponential backoff enabled, the delay after attempt number `n` is `min(base delay × 2^(n−1), maximum delay)`. For example, the default delays begin at 20 seconds, then 40 seconds, 80 seconds, and so on up to 10 minutes. When exponential backoff is disabled, each attempt uses the base delay. Failed connectors are restarted regardless of `pollingBehavior.restartFailedTasks`; that setting controls failed tasks whose connector is running.

Every outbound restart request counts as an attempt, including requests that return errors. A `RUNNING` connector clears its connector backoff, and a `RUNNING` task clears that task's backoff; a future failure then starts again at the base delay. If status retrieval fails, no actions or resets happen on that poll.

## Configuration API

`GET /config` returns the effective configuration as JSON and omits the authentication password. Durations use the same string format as YAML, with Go's canonical spelling (for example, `10m` is written as `10m0s`). Numeric JSON durations are rejected. Internal runtime updates use the same validation as startup and reject invalid configuration without changing stored values. The HTTP API exposes no configuration write endpoint.

## Tests

```sh
go test ./...
```
