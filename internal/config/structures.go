package config

import "log/slog"

type CommunicationConfiguration struct {
	RequestTimeout Duration `json:"requestTimeout"`
}

type AuthConfiguration struct {
	Enabled  bool   `json:"enabled"`
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
}

type BackoffConfiguration struct {
	Enabled     bool     `json:"enabled"`
	BaseDelay   Duration `json:"baseDelay"`
	MaxDelay    Duration `json:"maxDelay"`
	Exponential bool     `json:"exponential"`
}

type PollingBehavior struct {
	Interval           Duration             `json:"interval"`
	RestartFailedTasks bool                 `json:"restartFailedTasks"`
	Backoff            BackoffConfiguration `json:"backoff"`
}

type ConnectAPIConfiguration struct {
	Host       string            `json:"host"`
	Port       string            `json:"port"`
	HTTPS      bool              `json:"https"`
	AuthConfig AuthConfiguration `json:"authConfig"`
}

type Configuration struct {
	PollingBehavior     PollingBehavior            `json:"pollingBehavior"`
	CommunicationConfig CommunicationConfiguration `json:"communicationConfig"`
	ConnectConfig       ConnectAPIConfiguration    `json:"connectConfig"`
	LoggingConfig       LoggingConfiguration       `json:"loggingConfig"`
}

type LoggingConfiguration struct {
	Level slog.Level `json:"level"`
}
