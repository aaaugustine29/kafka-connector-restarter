package config

import "log/slog"

type CommunicationConfiguration struct {
	RequestTimeout Duration `json:"requestTimeout" yaml:"requestTimeout"`
}

type AuthConfiguration struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
}

type BackoffConfiguration struct {
	Enabled     bool     `json:"enabled" yaml:"enabled"`
	BaseDelay   Duration `json:"baseDelay" yaml:"baseDelay"`
	MaxDelay    Duration `json:"maxDelay" yaml:"maxDelay"`
	Exponential bool     `json:"exponential" yaml:"exponential"`
}

type PollingBehavior struct {
	Interval           Duration             `json:"interval" yaml:"interval"`
	RestartFailedTasks bool                 `json:"restartFailedTasks" yaml:"restartFailedTasks"`
	Backoff            BackoffConfiguration `json:"backoff" yaml:"backoff"`
}

type ConnectAPIConfiguration struct {
	Host       string            `json:"host" yaml:"host"`
	Port       string            `json:"port" yaml:"port"`
	HTTPS      bool              `json:"https" yaml:"https"`
	AuthConfig AuthConfiguration `json:"authConfig" yaml:"authConfig"`
}

type Configuration struct {
	PollingBehavior     PollingBehavior            `json:"pollingBehavior" yaml:"pollingBehavior"`
	CommunicationConfig CommunicationConfiguration `json:"communicationConfig" yaml:"communicationConfig"`
	ConnectConfig       ConnectAPIConfiguration    `json:"connectConfig" yaml:"connectConfig"`
	APIConfig           APIConfiguration           `json:"apiConfig" yaml:"apiConfig"`
	LoggingConfig       LoggingConfiguration       `json:"loggingConfig" yaml:"loggingConfig"`
}

type APIConfiguration struct {
	AuthConfig AuthConfiguration `json:"authConfig" yaml:"authConfig"`
}

type LoggingConfiguration struct {
	Level slog.Level `json:"level" yaml:"level"`
}
