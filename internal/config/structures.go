package config

import (
	"log/slog"
	"maps"
)

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
	ConnectName string            `json:"name" yaml:"name"`
	Host        string            `json:"host" yaml:"host"`
	Port        string            `json:"port" yaml:"port"`
	HTTPS       bool              `json:"https" yaml:"https"`
	AuthConfig  AuthConfiguration `json:"authConfig" yaml:"authConfig"`
}

type Configuration struct {
	PollingBehavior     PollingBehavior                    `json:"pollingBehavior" yaml:"pollingBehavior"`
	CommunicationConfig CommunicationConfiguration         `json:"communicationConfig" yaml:"communicationConfig"`
	ConnectConfigs      map[string]ConnectAPIConfiguration `json:"connectConfig" yaml:"connectConfig"`
	APIConfig           APIConfiguration                   `json:"apiConfig" yaml:"apiConfig"`
	LoggingConfig       LoggingConfiguration               `json:"loggingConfig" yaml:"loggingConfig"`
}

type APIConfiguration struct {
	AuthConfig AuthConfiguration `json:"authConfig" yaml:"authConfig"`
}

type LoggingConfiguration struct {
	Level slog.Level `json:"level" yaml:"level"`
}

func ConfigurationsAreEqual(config1 Configuration, config2 Configuration) bool {
	if config1.APIConfig != config2.APIConfig ||
		config1.CommunicationConfig != config2.CommunicationConfig ||
		config1.LoggingConfig != config2.LoggingConfig ||
		config1.PollingBehavior != config2.PollingBehavior ||
		len(config1.ConnectConfigs) != len(config2.ConnectConfigs) ||
		!maps.Equal(config1.ConnectConfigs, config2.ConnectConfigs) {
		return false
	}
	return true
}
