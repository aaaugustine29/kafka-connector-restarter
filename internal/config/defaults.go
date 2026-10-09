package config

import (
	"log/slog"
	"time"
)

const (
	DefaultPollingInterval                  Duration = Duration(10 * time.Second)
	DefaultRestartFailedTasks                        = true
	DefaultRestartBackoffEnabled                     = true
	DefaultRestartBackoffBaseDelay                   = 2 * DefaultPollingInterval
	DefaultRestartBackoffMaxDelay           Duration = Duration(10 * time.Minute)
	DefaultRestartBackoffExponentialEnabled          = true
	DefaultAPIBasicAuthEnabled                       = false
	DefaultHTTPRequestTimeout               Duration = Duration(10 * time.Second)
	DefaultLogLevel                                  = slog.LevelInfo
)

func DefaultBackoffConfiguration() BackoffConfiguration {
	return BackoffConfiguration{
		Enabled:     DefaultRestartBackoffEnabled,
		BaseDelay:   DefaultRestartBackoffBaseDelay,
		MaxDelay:    DefaultRestartBackoffMaxDelay,
		Exponential: DefaultRestartBackoffExponentialEnabled,
	}
}

func DefaultPollingBehavior() PollingBehavior {
	return PollingBehavior{
		Interval:           DefaultPollingInterval,
		RestartFailedTasks: DefaultRestartFailedTasks,
		Backoff:            DefaultBackoffConfiguration(),
	}
}

func DefaultLoggingConfiguration() LoggingConfiguration {
	return LoggingConfiguration{Level: DefaultLogLevel}
}

func DefaultAPIConfiguration() APIConfiguration {
	return APIConfiguration{
		AuthConfig: AuthConfiguration{Enabled: DefaultAPIBasicAuthEnabled},
	}
}

func DefaultConfiguration() ApplicationConfiguration {
	return ApplicationConfiguration{
		PollingBehavior:     DefaultPollingBehavior(),
		CommunicationConfig: DefaultCommunicationConfiguration(),
		APIConfig:           DefaultAPIConfiguration(),
		LoggingConfig:       DefaultLoggingConfiguration(),
	}
}

func DefaultCommunicationConfiguration() CommunicationConfiguration {
	return CommunicationConfiguration{
		RequestTimeout: DefaultHTTPRequestTimeout,
	}
}
