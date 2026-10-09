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
	DefaultConnectBasicAuthEnabled                   = false
	DefaultAPIBasicAuthEnabled                       = false
	DefaultHTTPRequestTimeout               Duration = Duration(10 * time.Second)
	DefaultConnectAPIHost                            = "localhost"
	DefaultConnectAPIPort                            = "8083"
	DefaultConnectAPIHTTPS                           = false
	DefaultLogLevel                                  = slog.LevelInfo
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

func DefaultConnectAPIConfiguration() map[string]ConnectAPIConfiguration {
	return map[string]ConnectAPIConfiguration{
		"default": {
			Host:       DefaultConnectAPIHost,
			Port:       DefaultConnectAPIPort,
			HTTPS:      DefaultConnectAPIHTTPS,
			AuthConfig: DefaultAuthConfiguration(),
		}}
}

func DefaultLoggingConfiguration() LoggingConfiguration {
	return LoggingConfiguration{Level: DefaultLogLevel}
}

func DefaultAPIConfiguration() APIConfiguration {
	return APIConfiguration{
		AuthConfig: AuthConfiguration{Enabled: DefaultAPIBasicAuthEnabled},
	}
}

func DefaultConfiguration() Configuration {
	return Configuration{
		PollingBehavior:     DefaultPollingBehavior(),
		CommunicationConfig: DefaultCommunicationConfiguration(),
		ConnectConfigs:      DefaultConnectAPIConfiguration(),
		APIConfig:           DefaultAPIConfiguration(),
		LoggingConfig:       DefaultLoggingConfiguration(),
	}
}

func DefaultCommunicationConfiguration() CommunicationConfiguration {
	return CommunicationConfiguration{
		RequestTimeout: DefaultHTTPRequestTimeout,
	}
}
