package environment

import (
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type CommunicationConfiguration struct {
	RequestTimeout time.Duration
}

type AuthConfiguration struct {
	Enabled  bool
	Username string
	Password string
}

type BackoffConfiguration struct {
	Enabled     bool
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Exponential bool
}

type PollingBehavior struct {
	Interval           time.Duration
	RestartFailedTasks bool
	Backoff            BackoffConfiguration
}

type ConnectConfiguration struct {
	Host       string
	Port       string
	URL        string
	HTTPS      bool
	AuthConfig AuthConfiguration
}

type Configuration struct {
	PollingBehavior     PollingBehavior
	CommunicationConfig CommunicationConfiguration
	ConnectConfig       ConnectConfiguration
	LoggingConfig       LoggingConfiguration
}

type LoggingConfiguration struct {
	Level slog.Level
}

func LoadBackoffConfiguration() BackoffConfiguration {
	backoffConfig := BackoffConfiguration{
		Enabled:     DefaultRestartBackoffEnabled,
		BaseDelay:   DefaultRestartBackoffBaseDelay,
		MaxDelay:    DefaultRestartBackoffMaxDelay,
		Exponential: DefaultRestartBackoffExponentialEnabled,
	}

	backoffEnabledString := os.Getenv(RestartBackoffEnabled)
	if backoffEnabledString == "" {
		slog.Debug("backoff setting is not configured; using default", "setting", RestartBackoffEnabled, "value", DefaultRestartBackoffEnabled)
	} else {
		backoffEnabled, err := strconv.ParseBool(backoffEnabledString)
		if err != nil {
			slog.Warn("invalid backoff setting; using default", "setting", RestartBackoffEnabled, "value", backoffEnabledString, "default", DefaultRestartBackoffEnabled)
		} else {
			backoffConfig.Enabled = backoffEnabled
		}
	}

	backoffBaseDelayString := os.Getenv(RestartBackoffBaseDelayMS)
	if backoffBaseDelayString == "" {
		slog.Debug("backoff setting is not configured; using default", "setting", RestartBackoffBaseDelayMS, "value", DefaultRestartBackoffBaseDelay)
	} else {
		backoffBaseDelay, err := strconv.Atoi(backoffBaseDelayString)
		if err != nil || backoffBaseDelay <= 0 {
			slog.Warn("invalid backoff setting; using default", "setting", RestartBackoffBaseDelayMS, "value", backoffBaseDelayString, "default", DefaultRestartBackoffBaseDelay)
		} else {
			backoffConfig.BaseDelay = time.Duration(backoffBaseDelay) * time.Millisecond
		}
	}

	backoffMaxDelayString := os.Getenv(RestartBackoffMaxDelayMS)
	if backoffMaxDelayString == "" {
		slog.Debug("backoff setting is not configured; using default", "setting", RestartBackoffMaxDelayMS, "value", DefaultRestartBackoffMaxDelay)
	} else {
		backoffMaxDelay, err := strconv.Atoi(backoffMaxDelayString)
		if err != nil || backoffMaxDelay <= 0 {
			slog.Warn("invalid backoff setting; using default", "setting", RestartBackoffMaxDelayMS, "value", backoffMaxDelayString, "default", DefaultRestartBackoffMaxDelay)
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
		slog.Debug("backoff setting is not configured; using default", "setting", RestartBackoffExponentialEnabled, "value", DefaultRestartBackoffExponentialEnabled)
	} else {
		exponentialBackoffEnabled, err := strconv.ParseBool(exponentialBackoffEnabledString)
		if err != nil {
			slog.Warn("invalid backoff setting; using default", "setting", RestartBackoffExponentialEnabled, "value", exponentialBackoffEnabledString, "default", DefaultRestartBackoffExponentialEnabled)
		} else {
			backoffConfig.Exponential = exponentialBackoffEnabled
		}
	}
	return backoffConfig
}

func LoadConfig() Configuration {
	return Configuration{
		PollingBehavior:     LoadPollingBehavior(),
		CommunicationConfig: LoadCommunicationConfiguration(),
		ConnectConfig:       LoadConnectConfiguration(),
		LoggingConfig:       LoadLoggingConfiguration(),
	}
}

func LoadLoggingConfiguration() LoggingConfiguration {
	levelValue := os.Getenv(LogLevelEnv)
	if levelValue == "" {
		slog.Debug("log level is not configured; using default", "setting", LogLevelEnv, "value", DefaultLogLevel)
		return LoggingConfiguration{Level: DefaultLogLevel}
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(levelValue))); err != nil {
		slog.Warn("invalid log level; using default", "setting", LogLevelEnv, "value", levelValue, "default", DefaultLogLevel)
		return LoggingConfiguration{Level: DefaultLogLevel}
	}

	slog.Debug("log level configured", "level", level)
	return LoggingConfiguration{Level: level}
}

func LoadPollingBehavior() PollingBehavior {
	return PollingBehavior{
		Interval:           LoadPollingInterval(),
		RestartFailedTasks: LoadRestartFailedTasks(),
		Backoff:            LoadBackoffConfiguration(),
	}
}

func LoadPollingInterval() time.Duration {
	pollInterval := os.Getenv(PollingIntervalEnv)
	if pollInterval == "" {
		slog.Debug("polling interval is not configured; using default", "setting", PollingIntervalEnv, "value", DefaultPollingInterval)
		return DefaultPollingInterval
	}

	pollIntervalMS, err := strconv.Atoi(pollInterval)
	if err != nil || pollIntervalMS <= 0 {
		slog.Warn("invalid polling interval; using default", "setting", PollingIntervalEnv, "value", pollInterval, "default", DefaultPollingInterval)
		return DefaultPollingInterval
	}

	pollingInterval := time.Duration(pollIntervalMS) * time.Millisecond
	slog.Debug("polling interval configured", "value", pollingInterval)
	return pollingInterval
}

func LoadRestartFailedTasks() bool {
	restartFailedTasksString := os.Getenv(PollingRestartFailedTasks)
	if restartFailedTasksString == "" {
		slog.Debug("task restart setting is not configured; using default", "setting", PollingRestartFailedTasks, "value", DefaultRestartFailedTasks)
		return DefaultRestartFailedTasks
	}

	restartFailedTasks, err := strconv.ParseBool(restartFailedTasksString)
	if err != nil {
		slog.Warn("invalid task restart setting; using default", "setting", PollingRestartFailedTasks, "value", restartFailedTasksString, "default", DefaultRestartFailedTasks)
		return DefaultRestartFailedTasks
	}

	slog.Debug("task restart setting configured", "value", restartFailedTasks)
	return restartFailedTasks
}

func LoadConnectConfiguration() ConnectConfiguration {
	host := os.Getenv(ConnectHostEnv)
	if host == "" {
		slog.Debug("Connect host is not configured; using default", "setting", ConnectHostEnv, "value", DefaultConnectHost)
		host = DefaultConnectHost
	}

	port := os.Getenv(ConnectPortEnv)
	if port == "" {
		slog.Debug("Connect port is not configured; using default", "setting", ConnectPortEnv, "value", DefaultConnectPort)
		port = DefaultConnectPort
	}

	https := LoadHTTPS()
	authConfig := LoadAuthConfiguration()
	if authConfig.Enabled && !https {
		slog.Warn("basic authentication requires HTTPS; disabling authentication", "setting", ConnectSecureHTTP)
		authConfig = AuthConfiguration{
			Enabled:  DefaultConnectBasicAuthEnabled,
			Username: "",
			Password: "",
		}
	}
	scheme := "http"
	if https {
		scheme = "https"
	}

	return ConnectConfiguration{
		Host:  host,
		Port:  port,
		HTTPS: https,
		URL: (&url.URL{
			Scheme: scheme,
			Host:   net.JoinHostPort(host, port),
		}).String(),
		AuthConfig: authConfig,
	}
}

func LoadHTTPS() bool {
	httpsValue := os.Getenv(ConnectSecureHTTP)
	if httpsValue == "" {
		slog.Debug("HTTPS setting is not configured; using default", "setting", ConnectSecureHTTP, "value", DefaultConnectSecureHTTP)
		return DefaultConnectSecureHTTP
	}

	https, err := strconv.ParseBool(httpsValue)
	if err != nil {
		slog.Warn("invalid HTTPS setting; using default", "setting", ConnectSecureHTTP, "value", httpsValue, "default", DefaultConnectSecureHTTP)
		return DefaultConnectSecureHTTP
	}

	slog.Debug("HTTPS setting configured", "value", https)
	return https
}

func LoadCommunicationConfiguration() CommunicationConfiguration {
	requestTimeoutString := os.Getenv(HTTPRequestTimeout)
	var requestTimeoutInt int64
	var requestTimeout time.Duration
	var err error

	if requestTimeoutString == "" {
		slog.Debug("HTTP request timeout is not configured; using default", "setting", HTTPRequestTimeout, "value", DefaultHTTPRequestTimeout)
		requestTimeout = DefaultHTTPRequestTimeout
	} else {
		requestTimeoutInt, err = strconv.ParseInt(requestTimeoutString, 10, 64)
		if err != nil || requestTimeoutInt <= 0 {
			slog.Warn("invalid HTTP request timeout; using default", "setting", HTTPRequestTimeout, "value", requestTimeoutString, "default", DefaultHTTPRequestTimeout)
			requestTimeout = DefaultHTTPRequestTimeout
		} else {
			requestTimeout = time.Duration(requestTimeoutInt) * time.Millisecond
		}
	}

	return CommunicationConfiguration{
		RequestTimeout: requestTimeout,
	}
}

func LoadAuthConfiguration() AuthConfiguration {
	authEnabledString := os.Getenv(ConnectBasicAuthEnabledEnv)
	if authEnabledString == "" {
		slog.Debug("basic authentication is not configured; using default", "setting", ConnectBasicAuthEnabledEnv, "value", DefaultConnectBasicAuthEnabled)
		return AuthConfiguration{
			Enabled:  DefaultConnectBasicAuthEnabled,
			Username: "",
			Password: "",
		}
	}
	authEnabled, err := strconv.ParseBool(authEnabledString)
	if err != nil || authEnabled == false {
		if err != nil {
			slog.Warn("invalid basic authentication setting; using default", "setting", ConnectBasicAuthEnabledEnv, "value", authEnabledString, "default", DefaultConnectBasicAuthEnabled)
		} else {
			slog.Debug("basic authentication is disabled")
		}
		return AuthConfiguration{
			Enabled:  DefaultConnectBasicAuthEnabled,
			Username: "",
			Password: "",
		}
	} else {
		authUsername := os.Getenv(ConnectBasicAuthUsernameEnv)
		authPassword := os.Getenv(ConnectBasicAuthPasswordEnv)
		if authUsername == "" || authPassword == "" {
			slog.Warn("basic authentication credentials are incomplete; disabling authentication")
			return AuthConfiguration{
				Enabled:  DefaultConnectBasicAuthEnabled,
				Username: "",
				Password: "",
			}
		} else {
			return AuthConfiguration{
				Enabled:  true,
				Username: authUsername,
				Password: authPassword,
			}
		}
	}
}
