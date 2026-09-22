package environment

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/config"
)

func LoadBackoffConfiguration() config.BackoffConfiguration {
	backoffConfig := config.DefaultBackoffConfiguration()

	backoffEnabledString := os.Getenv(RestartBackoffEnabled)
	if backoffEnabledString == "" {
		slog.Debug("backoff setting is not configured; using default", "setting", RestartBackoffEnabled, "value", config.DefaultRestartBackoffEnabled)
	} else {
		backoffEnabled, err := strconv.ParseBool(backoffEnabledString)
		if err != nil {
			slog.Warn("invalid backoff setting; using default", "setting", RestartBackoffEnabled, "value", backoffEnabledString, "default", config.DefaultRestartBackoffEnabled)
		} else {
			backoffConfig.Enabled = backoffEnabled
		}
	}

	backoffBaseDelayString := os.Getenv(RestartBackoffBaseDelayMS)
	if backoffBaseDelayString == "" {
		slog.Debug("backoff setting is not configured; using default", "setting", RestartBackoffBaseDelayMS, "value", config.DefaultRestartBackoffBaseDelay)
	} else {
		backoffBaseDelay, err := strconv.Atoi(backoffBaseDelayString)
		if err != nil || backoffBaseDelay <= 0 {
			slog.Warn("invalid backoff setting; using default", "setting", RestartBackoffBaseDelayMS, "value", backoffBaseDelayString, "default", config.DefaultRestartBackoffBaseDelay)
		} else {
			backoffConfig.BaseDelay = time.Duration(backoffBaseDelay) * time.Millisecond
		}
	}

	backoffMaxDelayString := os.Getenv(RestartBackoffMaxDelayMS)
	if backoffMaxDelayString == "" {
		slog.Debug("backoff setting is not configured; using default", "setting", RestartBackoffMaxDelayMS, "value", config.DefaultRestartBackoffMaxDelay)
	} else {
		backoffMaxDelay, err := strconv.Atoi(backoffMaxDelayString)
		if err != nil || backoffMaxDelay <= 0 {
			slog.Warn("invalid backoff setting; using default", "setting", RestartBackoffMaxDelayMS, "value", backoffMaxDelayString, "default", config.DefaultRestartBackoffMaxDelay)
		} else {
			backoffConfig.MaxDelay = time.Duration(backoffMaxDelay) * time.Millisecond
		}
	}

	if backoffConfig.MaxDelay < backoffConfig.BaseDelay {
		slog.Warn("backoff maximum delay is less than the base delay; using the base delay", "maximum_delay", backoffConfig.MaxDelay, "base_delay", backoffConfig.BaseDelay)
		backoffConfig.MaxDelay = backoffConfig.BaseDelay
	}

	exponentialBackoffEnabledString := os.Getenv(RestartBackoffExponentialEnabled)
	if exponentialBackoffEnabledString == "" {
		slog.Debug("backoff setting is not configured; using default", "setting", RestartBackoffExponentialEnabled, "value", config.DefaultRestartBackoffExponentialEnabled)
	} else {
		exponentialBackoffEnabled, err := strconv.ParseBool(exponentialBackoffEnabledString)
		if err != nil {
			slog.Warn("invalid backoff setting; using default", "setting", RestartBackoffExponentialEnabled, "value", exponentialBackoffEnabledString, "default", config.DefaultRestartBackoffExponentialEnabled)
		} else {
			backoffConfig.Exponential = exponentialBackoffEnabled
		}
	}
	return backoffConfig
}

func LoadConfig() config.Configuration {
	return config.Configuration{
		PollingBehavior:     LoadPollingBehavior(),
		CommunicationConfig: LoadCommunicationConfiguration(),
		ConnectConfig:       LoadConnectConfiguration(),
		LoggingConfig:       LoadLoggingConfiguration(),
	}
}

func LoadLoggingConfiguration() config.LoggingConfiguration {
	defaultConfig := config.DefaultLoggingConfiguration()
	levelValue := os.Getenv(LogLevelEnv)
	if levelValue == "" {
		slog.Debug("log level is not configured; using default", "setting", LogLevelEnv, "value", defaultConfig.Level)
		return defaultConfig
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(levelValue))); err != nil {
		slog.Warn("invalid log level; using default", "setting", LogLevelEnv, "value", levelValue, "default", defaultConfig.Level)
		return defaultConfig
	}

	slog.Debug("log level configured", "level", level)
	return config.LoggingConfiguration{Level: level}
}

func LoadPollingBehavior() config.PollingBehavior {
	return config.PollingBehavior{
		Interval:           LoadPollingInterval(),
		RestartFailedTasks: LoadRestartFailedTasks(),
		Backoff:            LoadBackoffConfiguration(),
	}
}

func LoadPollingInterval() time.Duration {
	defaultConfig := config.DefaultPollingBehavior()
	pollInterval := os.Getenv(PollingIntervalEnv)
	if pollInterval == "" {
		slog.Debug("polling interval is not configured; using default", "setting", PollingIntervalEnv, "value", defaultConfig.Interval)
		return defaultConfig.Interval
	}

	pollIntervalMS, err := strconv.Atoi(pollInterval)
	if err != nil || pollIntervalMS <= 0 {
		slog.Warn("invalid polling interval; using default", "setting", PollingIntervalEnv, "value", pollInterval, "default", defaultConfig.Interval)
		return defaultConfig.Interval
	}

	pollingInterval := time.Duration(pollIntervalMS) * time.Millisecond
	slog.Debug("polling interval configured", "value", pollingInterval)
	return pollingInterval
}

func LoadRestartFailedTasks() bool {
	defaultConfig := config.DefaultPollingBehavior()
	restartFailedTasksString := os.Getenv(PollingRestartFailedTasks)
	if restartFailedTasksString == "" {
		slog.Debug("task restart setting is not configured; using default", "setting", PollingRestartFailedTasks, "value", defaultConfig.RestartFailedTasks)
		return defaultConfig.RestartFailedTasks
	}

	restartFailedTasks, err := strconv.ParseBool(restartFailedTasksString)
	if err != nil {
		slog.Warn("invalid task restart setting; using default", "setting", PollingRestartFailedTasks, "value", restartFailedTasksString, "default", defaultConfig.RestartFailedTasks)
		return defaultConfig.RestartFailedTasks
	}

	slog.Debug("task restart setting configured", "value", restartFailedTasks)
	return restartFailedTasks
}

func LoadConnectConfiguration() config.ConnectAPIConfiguration {
	defaultConfig := config.DefaultConnectAPIConfiguration()
	host := os.Getenv(ConnectHostEnv)
	if host == "" {
		slog.Debug("Connect host is not configured; using default", "setting", ConnectHostEnv, "value", defaultConfig.Host)
		host = defaultConfig.Host
	}

	port := os.Getenv(ConnectPortEnv)
	if port == "" {
		slog.Debug("Connect port is not configured; using default", "setting", ConnectPortEnv, "value", defaultConfig.Port)
		port = defaultConfig.Port
	}

	https := LoadHTTPS()
	authConfig := LoadAuthConfiguration()
	if authConfig.Enabled && !https {
		slog.Warn("basic authentication requires HTTPS; disabling authentication", "setting", ConnectSecureHTTP)
		authConfig = config.DefaultAuthConfiguration()
	}
	return config.ConnectAPIConfiguration{
		Host:       host,
		Port:       port,
		HTTPS:      https,
		AuthConfig: authConfig,
	}
}

func LoadHTTPS() bool {
	defaultConfig := config.DefaultConnectAPIConfiguration()
	httpsValue := os.Getenv(ConnectSecureHTTP)
	if httpsValue == "" {
		slog.Debug("HTTPS setting is not configured; using default", "setting", ConnectSecureHTTP, "value", defaultConfig.HTTPS)
		return defaultConfig.HTTPS
	}

	https, err := strconv.ParseBool(httpsValue)
	if err != nil {
		slog.Warn("invalid HTTPS setting; using default", "setting", ConnectSecureHTTP, "value", httpsValue, "default", defaultConfig.HTTPS)
		return defaultConfig.HTTPS
	}

	slog.Debug("HTTPS setting configured", "value", https)
	return https
}

func LoadCommunicationConfiguration() config.CommunicationConfiguration {
	defaultConfig := config.DefaultCommunicationConfiguration()
	requestTimeoutString := os.Getenv(HTTPRequestTimeout)

	if requestTimeoutString == "" {
		slog.Debug("HTTP request timeout is not configured; using default", "setting", HTTPRequestTimeout, "value", defaultConfig.RequestTimeout)
		return defaultConfig
	}

	requestTimeoutMS, err := strconv.ParseInt(requestTimeoutString, 10, 64)
	if err != nil || requestTimeoutMS <= 0 {
		slog.Warn("invalid HTTP request timeout; using default", "setting", HTTPRequestTimeout, "value", requestTimeoutString, "default", defaultConfig.RequestTimeout)
		return defaultConfig
	}

	defaultConfig.RequestTimeout = time.Duration(requestTimeoutMS) * time.Millisecond
	return defaultConfig
}

func LoadAuthConfiguration() config.AuthConfiguration {
	defaultConfig := config.DefaultAuthConfiguration()
	authEnabledString := os.Getenv(ConnectBasicAuthEnabledEnv)
	if authEnabledString == "" {
		slog.Debug("basic authentication is not configured; using default", "setting", ConnectBasicAuthEnabledEnv, "value", defaultConfig.Enabled)
		return defaultConfig
	}
	authEnabled, err := strconv.ParseBool(authEnabledString)
	if err != nil || authEnabled == false {
		if err != nil {
			slog.Warn("invalid basic authentication setting; using default", "setting", ConnectBasicAuthEnabledEnv, "value", authEnabledString, "default", defaultConfig.Enabled)
		} else {
			slog.Debug("basic authentication is disabled")
		}
		return defaultConfig
	} else {
		authUsername := os.Getenv(ConnectBasicAuthUsernameEnv)
		authPassword := os.Getenv(ConnectBasicAuthPasswordEnv)
		if authUsername == "" || authPassword == "" {
			slog.Warn("basic authentication credentials are incomplete; disabling authentication")
			return defaultConfig
		} else {
			return config.AuthConfiguration{
				Enabled:  true,
				Username: authUsername,
				Password: authPassword,
			}
		}
	}
}
