package environment

import "time"

const (
	PollingIntervalEnv        = "RESTARTER_POLL_INTERVAL_MS"
	PollingRestartFailedTasks = "RESTARTER_RESTART_FAILED_TASKS"

	ConnectBasicAuthEnabledEnv  = "RESTARTER_CONNECT_BASIC_AUTH_ENABLED"
	ConnectBasicAuthUsernameEnv = "RESTARTER_CONNECT_BASIC_AUTH_USERNAME"
	ConnectBasicAuthPasswordEnv = "RESTARTER_CONNECT_BASIC_AUTH_PASSWORD"

	HTTPRequestTimeout = "RESTARTER_HTTP_REQUEST_TIMEOUT_MS"

	ConnectHostEnv    = "RESTARTER_CONNECT_HOST"
	ConnectPortEnv    = "RESTARTER_CONNECT_PORT"
	ConnectSecureHTTP = "RESTARTER_CONNECT_HTTPS"

	DefaultPollingInterval         = 10 * time.Second
	DefaultConnectBasicAuthEnabled = false
	DefaultRestartFailedTasks      = true
	DefaultHTTPRequestTimeout      = 10 * time.Second
	DefaultConnectHost             = "localhost"
	DefaultConnectPort             = "8083"
	DefaultConnectSecureHTTP       = false
)
