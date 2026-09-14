package environment

import "time"

const (
	DefaultPollingInterval                  = 10 * time.Second
	DefaultRestartFailedTasks               = true
	DefaultRestartBackoffEnabled            = true
	DefaultRestartBackoffBaseDelay          = 2 * DefaultPollingInterval
	DefaultRestartBackoffExponentialEnabled = true
	DefaultConnectBasicAuthEnabled          = false
	DefaultHTTPRequestTimeout               = 10 * time.Second
	DefaultConnectHost                      = "localhost"
	DefaultConnectPort                      = "8083"
	DefaultConnectSecureHTTP                = false
)
