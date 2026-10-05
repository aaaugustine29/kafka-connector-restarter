package environment

const (
	PollingIntervalEnv        = "RESTARTER_POLL_INTERVAL"
	PollingRestartFailedTasks = "RESTARTER_RESTART_FAILED_TASKS"

	RestartBackoffEnabled            = "RESTARTER_RESTART_BACKOFF_ENABLED"
	RestartBackoffBaseDelayEnv       = "RESTARTER_RESTART_BACKOFF_BASE_DELAY"
	RestartBackoffMaxDelayEnv        = "RESTARTER_RESTART_BACKOFF_MAX_DELAY"
	RestartBackoffExponentialEnabled = "RESTARTER_RESTART_BACKOFF_EXPONENTIAL_ENABLED"

	ConnectBasicAuthEnabledEnv  = "RESTARTER_CONNECT_BASIC_AUTH_ENABLED"
	ConnectBasicAuthUsernameEnv = "RESTARTER_CONNECT_BASIC_AUTH_USERNAME"
	ConnectBasicAuthPasswordEnv = "RESTARTER_CONNECT_BASIC_AUTH_PASSWORD"

	HTTPRequestTimeoutEnv = "RESTARTER_HTTP_REQUEST_TIMEOUT"

	ConnectHostEnv    = "RESTARTER_CONNECT_HOST"
	ConnectPortEnv    = "RESTARTER_CONNECT_PORT"
	ConnectSecureHTTP = "RESTARTER_CONNECT_HTTPS"

	LogLevelEnv = "RESTARTER_LOG_LEVEL"
)
