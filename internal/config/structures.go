package config

import (
	"log/slog"
	"net"
	"net/url"
	"time"
)

type CommunicationConfiguration struct {
	RequestTimeout time.Duration `json:"requestTimeout"`
}

type AuthConfiguration struct {
	Enabled  bool   `json:"enabled"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type BackoffConfiguration struct {
	Enabled     bool          `json:"enabled"`
	BaseDelay   time.Duration `json:"baseDelay"`
	MaxDelay    time.Duration `json:"maxDelay"`
	Exponential bool          `json:"exponential"`
}

type PollingBehavior struct {
	Interval           time.Duration        `json:"interval"`
	RestartFailedTasks bool                 `json:"restartFailedTasks"`
	Backoff            BackoffConfiguration `json:"backoff"`
}

type ConnectAPIConfiguration struct {
	Host       string            `json:"host"`
	Port       string            `json:"port"`
	URL        string            `json:"url"`
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

func NewConnectAPIConfiguration(host, port string, https bool, authConfig AuthConfiguration) ConnectAPIConfiguration {
	scheme := "http"
	if https {
		scheme = "https"
	}

	return ConnectAPIConfiguration{
		Host:       host,
		Port:       port,
		HTTPS:      https,
		AuthConfig: authConfig,
		URL: (&url.URL{
			Scheme: scheme,
			Host:   net.JoinHostPort(host, port),
		}).String(),
	}
}
