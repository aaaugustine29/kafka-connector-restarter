package config

import (
	"log/slog"
	"time"
)

const (
	DefaultPollingInterval                  = 10 * time.Second
	DefaultRestartFailedTasks               = true
	DefaultRestartBackoffEnabled            = true
	DefaultRestartBackoffBaseDelay          = 2 * DefaultPollingInterval
	DefaultRestartBackoffMaxDelay           = 10 * time.Minute
	DefaultRestartBackoffExponentialEnabled = true
	DefaultConnectBasicAuthEnabled          = false
	DefaultHTTPRequestTimeout               = 10 * time.Second
	DefaultConnectAPIHost                   = "localhost"
	DefaultConnectAPIPort                   = "8083"
	DefaultConnectAPISecureHTTP             = false
	DefaultLogLevel                         = slog.LevelInfo
)

func DefaultAuthConfiguration() AuthConfiguration {
	return AuthConfiguration{
		Enabled: DefaultConnectBasicAuthEnabled,
	}
}

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

func DefaultConnectAPIConfiguration() ConnectAPIConfiguration {
	return NewConnectAPIConfiguration(
		DefaultConnectAPIHost,
		DefaultConnectAPIPort,
		DefaultConnectAPISecureHTTP,
		DefaultAuthConfiguration(),
	)
}

func DefaultLoggingConfiguration() LoggingConfiguration {
	return LoggingConfiguration{Level: DefaultLogLevel}
}

func DefaultConfiguration() Configuration {
	return Configuration{
		PollingBehavior:     DefaultPollingBehavior(),
		CommunicationConfig: DefaultCommunicationConfiguration(),
		ConnectConfig:       DefaultConnectAPIConfiguration(),
		LoggingConfig:       DefaultLoggingConfiguration(),
	}
}

func DefaultCommunicationConfiguration() CommunicationConfiguration {
	return CommunicationConfiguration{
		RequestTimeout: DefaultHTTPRequestTimeout,
	}
}
