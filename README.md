# kafka-connector-restarter

Polls the REST APIs of named Kafka Connect clusters and restarts failed connectors. When task restarts are enabled, it also restarts failed tasks whose connector is running. Polling begins after the first interval; the service stops on SIGINT or SIGTERM.

## Run

Requires Go 1.27.1 or later. Run with all default values:

```sh
go run ./cmd
```

Supply optional application settings and cluster definitions using separate YAML files:

```sh
go run ./cmd --config=/path/to/application.yaml --connect-clusters=/path/to/connect-clusters.yaml
```

The repository does not include configuration files. Omit `--config` to use all application defaults; an application file containing `{}` has the same effect. Omit `--connect-clusters` to poll one cluster named `default` at `http://localhost:8083`. To supply cluster definitions, use a file containing a mapping of cluster names to endpoint settings. A cluster file containing `{}` starts no pollers; it still starts the application API.

Optional Secret overlays are loaded independently:

```sh
go run ./cmd \
  --config=/path/to/application.yaml \
  --secret-config=/path/to/application-secret.yaml \
  --connect-clusters=/path/to/connect-clusters.yaml \
  --connect-clusters-secret-config=/path/to/connect-clusters-secret.yaml
```

Both base files and each Secret overlay are optional, but any explicitly supplied file is required. No files are discovered automatically. Unreadable or invalid files cause startup to fail before the HTTP server or polling starts; an invalid file does not fall back to defaults. An application Secret overlay can be supplied without a base file and merges onto application defaults.

The HTTP API listens on port 8080 on all interfaces. `GET /` lists the available routes, and `GET /config` returns application settings without the API password. Cluster definitions are not exposed by the application API. Optional API authentication protects all routes. An API startup or serving failure is logged as an error while polling continues. On SIGINT or SIGTERM, polling requests are canceled and the API has up to five seconds to finish active requests before its connections are closed.

API connections have a five-second header timeout, a five-second timeout for reading the entire request including its body, a ten-second response-write timeout, and a one-minute idle timeout. All polling, restart, and backoff logs include `connect_cluster` and `endpoint`; action logs also identify the connector and, for task restarts, `task_id`. Startup logs report the configured cluster count and source, and each poller logs its effective settings, changes, and shutdown at INFO. Restart logs report requests as accepted, since an HTTP success response does not prove that the connector or task has finished restarting. Repeated failure detection, backoff skips, and poll-cycle counts and durations are logged at DEBUG; request attempts are logged at INFO and failures at ERROR. Authentication credentials are omitted. Normal shutdown cancellation does not generate polling failure logs.

## Configuration

Both configuration packages read their own files once at startup, in this order:

1. Application defaults; for clusters, a single `default` cluster when no base file is supplied, or endpoint defaults for each named cluster in a supplied file.
2. Its optional base YAML file, normally mounted from a Kubernetes ConfigMap.
3. Its optional Secret YAML overlay, normally mounted from a Kubernetes Secret.

Application settings live in `internal/config`; cluster configuration lives in `internal/connectcluster`. The files use independent schemas. Application settings remain at the root of the application file, with sections such as `pollingBehavior` and `apiConfig`. The cluster file contains named entries:

```yaml
production:
  host: connect.production.svc
  port: "8083"
  https: true
  authConfig:
    enabled: true
staging:
  host: connect.staging.svc
```

The map key identifies the cluster; there is no separate `name` field. When a cluster base file is supplied, only its entries and those added by its overlay are created. Each new cluster starts with endpoint defaults, so the staging entry above retains port `8083` and disables HTTPS and authentication.

Each supplied field replaces its previous value, including `false` and empty strings. Omitted fields retain their previous values; an empty mapping (`{}`) changes nothing. A supplied nested mapping changes only its supplied fields. Cluster overlays merge each named entry onto that cluster's existing values, preserving its endpoint and omitted credentials. An overlay may add a named cluster or change any field. Without a cluster base file, the overlay merges onto the initial `default` cluster and may add further clusters.

Each merged configuration is validated after its base file and overlay have been loaded, so the base can enable authentication and leave credentials for the overlay. Keep actual credentials outside version control and out of ConfigMaps. A cluster Secret overlay can supply credentials as follows:

```yaml
production:
  authConfig:
    username: replace-me
    password: replace-me
```

Cluster Basic Auth requires `https: true`, a nonblank username, and a nonempty password. Invalid combinations fail startup; authentication is never silently disabled. All clusters share the application's polling behavior, request timeout, and logging policy, while maintaining independent restart backoff history.

The available settings and defaults are:

| YAML field | Default |
| --- | --- |
| `<cluster>.host` | `localhost` |
| `<cluster>.port` | `"8083"` |
| `<cluster>.https` | `false` |
| `<cluster>.authConfig.enabled` | `false` |
| `<cluster>.authConfig.username` | empty string |
| `<cluster>.authConfig.password` | empty string |
| `apiConfig.authConfig.enabled` | `false` |
| `apiConfig.authConfig.username` | empty string |
| `apiConfig.authConfig.password` | empty string |
| `communicationConfig.requestTimeout` | `10s` |
| `pollingBehavior.interval` | `10s` |
| `pollingBehavior.restartFailedTasks` | `true` |
| `pollingBehavior.backoff.enabled` | `true` |
| `pollingBehavior.backoff.baseDelay` | `20s` |
| `pollingBehavior.backoff.maxDelay` | `10m` |
| `pollingBehavior.backoff.exponential` | `true` |
| `loggingConfig.level` | `INFO` |

In the cluster file, `<cluster>.host` is a DNS hostname or unbracketed IP address (for example, `kafka-connect`, `127.0.0.1`, or `2001:db8::1`), without a URL scheme, port, path, or whitespace. Invalid host syntax is rejected at startup. `<cluster>.port` is a string such as `"8083"`. In the application file, `loggingConfig.level` accepts Go slog levels such as `DEBUG`, `INFO`, `WARN`, and `ERROR`. Connect requests do not follow redirects; a redirect is treated as an unsuccessful status rather than allowing a restart POST to become a GET or send credentials to another endpoint.

Durations are positive strings with units, such as `250ms`, `10s`, `1.5s`, or `1m30s`. Supported units are `ns`, `us` (or `µs`), `ms`, `s`, `m`, and `h`. Numeric, invalid, nonpositive, or overflowing durations are rejected. The maximum backoff delay must be at least the base delay. The default base delay is `20s`, independently of any configured polling interval.

Each file must contain exactly one YAML mapping document. Unknown keys, duplicate keys, null values, aliases, and YAML merge keys are rejected. Use `{}` for a file that intentionally keeps existing values. Decoder errors omit raw values to prevent credentials from reaching logs.

### Migration from environment variables

The application no longer reads `RESTARTER_*` environment variables. Move their values into the corresponding YAML fields and mount the file. For example, `RESTARTER_POLL_INTERVAL=30s` becomes `pollingBehavior.interval: 30s`, and Connect credentials become `<cluster>.authConfig.username` and `<cluster>.authConfig.password` in the cluster Secret overlay. Move endpoint settings into a named entry in the cluster file. Remove the old environment entries from the Deployment to avoid misleading configuration.

## Kubernetes

Mount any application and cluster base files from a ConfigMap and their optional overlays from a Secret, using read-only directory mounts. A single ConfigMap can contain both base files, and a single Secret can contain both overlays. Pass the file paths through the corresponding command-line flags. The image entrypoint must launch the application binary. If defaults suffice, omit the corresponding file flags and mounts.

For Kustomize deployments, generate the ConfigMap and Secret from these YAML files and keep generated name hashes enabled. Changes to input files generate a new resource name and update the Deployment's volume reference when applied, triggering replacement of the pod. This makes configuration and credential changes follow the same deployment process. See [Kustomize generators](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/kustomization/).

Use one replica and the `Recreate` strategy to avoid overlapping pollers issuing duplicate restart requests during updates. Updates include a brief polling interruption and reset the application's in-memory restart backoff history.

For Helm deployments, mount the same files and use pod-template checksum annotations for both chart-managed ConfigMaps and Secrets. Independently managed Secret changes also need an explicit pod restart or a deployment controller that triggers one. See [Helm's rollout guidance](https://helm.sh/docs/howto/charts_tips_and_tricks/#automatically-roll-deployments).

Configuration is only read at startup. Updating a mounted file does not change the running application's configuration; restart the pod to apply it. Mounting files does not require the application to access the Kubernetes API. See [ConfigMaps](https://kubernetes.io/docs/concepts/configuration/configmap/) and [Secrets](https://kubernetes.io/docs/concepts/configuration/secret/).

## Restart backoff

Backoff is tracked separately for each cluster, connector restart, and task restart. Identical connector names in different clusters do not share backoff state. With exponential backoff enabled, the delay after attempt number `n` is `min(base delay × 2^(n−1), maximum delay)`. For example, the default delays begin at 20 seconds, then 40 seconds, 80 seconds, and so on up to 10 minutes. When exponential backoff is disabled, each attempt uses the base delay. Failed connectors are restarted regardless of `pollingBehavior.restartFailedTasks`; that setting controls failed tasks whose connector is running.

Every outbound restart request counts as an attempt, including requests that return errors. A `RUNNING` connector clears its connector backoff, and a `RUNNING` task clears that task's backoff; a future failure then starts again at the base delay. If status retrieval fails, no actions or resets happen on that poll.

## Configuration API

API Basic Auth is disabled by default and is independent of Kafka Connect authentication. To enable it, put this in the base configuration:

```yaml
apiConfig:
  authConfig:
    enabled: true
```

Supply credentials in the Secret YAML overlay, not the ConfigMap:

```yaml
apiConfig:
  authConfig:
    username: restarter-admin
    password: replace-me
```

Enabled API authentication requires a nonblank username without a colon and a nonempty password; otherwise startup fails. Missing, malformed, or incorrect credentials return `401 Unauthorized` with a Basic Auth challenge before any route handler runs. All routes require the same credentials. Clients can use `curl --user restarter-admin https://your-restarter-host/config` to be prompted for the password. Credentials are never included in authentication error responses or logs.

API authentication is startup-only: PATCH cannot enable, disable, or change its credentials. Update the startup files and restart to apply those changes. Resubmitting identical API settings is a no-op and is allowed.

Basic Auth does not encrypt credentials. The service still listens using HTTP; use a trusted TLS-terminating ingress/proxy and restrict direct backend access. Protect the proxy-to-service connection as appropriate for your deployment, and do not expose the unauthenticated default API to untrusted networks. See [Go's Basic Auth documentation](https://pkg.go.dev/net/http#Request.SetBasicAuth).

`GET /config` returns only the effective application configuration as JSON, omits the API authentication password, and sets `Cache-Control: no-store` to prevent caching. Durations use the same string format as YAML, with Go's canonical spelling (for example, `10m` is written as `10m0s`). Numeric JSON durations are rejected.

`PATCH /config` accepts a partial application JSON object using the same field names and requires `Content-Type: application/json` (parameters such as `charset=utf-8` are accepted). Missing, malformed, or unsupported content types return `415` with `Accept-Patch: application/json`. Supplied fields replace their current values; omitted fields retain them, including an omitted API authentication password. The complete resulting configuration must pass the same validation as startup. Invalid updates return `400` without changing stored values. Field errors identify the affected path without exposing submitted values. Successful updates return `204 No Content` and notify the polling and logging workers. Bodies larger than 64 KiB return `413`; body-read timeouts return `408`.

```sh
curl -X PATCH http://localhost:8080/config \
  -H 'Content-Type: application/json' \
  -d '{"pollingBehavior":{"interval":"30s","restartFailedTasks":false}}'
```

JSON handling uses Go's `encoding/json/v2`. Null values and duplicate keys are rejected at every nesting level. Duplicate checking also catches escaped spellings and casing differences such as `interval` and `Interval`. Repeating a key in separate objects is allowed. Unknown fields and invalid UTF-8 are rejected too. Cluster configuration fields are unknown to this endpoint and return `400`. JSON decoding errors use JSON Pointer paths, such as `/pollingBehavior/interval`; configuration validation errors use dotted paths.

Application updates affect only the running process. YAML files are never modified, and restarting restores configuration from those files. Cluster membership, endpoints, and credentials are startup-only; update their YAML files and restart to change them.

## Tests

```sh
go test ./...
```
