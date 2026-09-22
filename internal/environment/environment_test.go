package environment

import (
	"log/slog"
	"testing"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/config"
)

type (
	AuthConfiguration    = config.AuthConfiguration
	BackoffConfiguration = config.BackoffConfiguration
	PollingBehavior      = config.PollingBehavior
)

const (
	DefaultLogLevel                         = config.DefaultLogLevel
	DefaultPollingInterval                  = config.DefaultPollingInterval
	DefaultRestartFailedTasks               = config.DefaultRestartFailedTasks
	DefaultRestartBackoffEnabled            = config.DefaultRestartBackoffEnabled
	DefaultRestartBackoffBaseDelay          = config.DefaultRestartBackoffBaseDelay
	DefaultRestartBackoffMaxDelay           = config.DefaultRestartBackoffMaxDelay
	DefaultRestartBackoffExponentialEnabled = config.DefaultRestartBackoffExponentialEnabled
	DefaultConnectBasicAuthEnabled          = config.DefaultConnectBasicAuthEnabled
	DefaultHTTPRequestTimeout               = config.DefaultHTTPRequestTimeout
	DefaultConnectHost                      = config.DefaultConnectAPIHost
	DefaultConnectPort                      = config.DefaultConnectAPIPort
	DefaultConnectSecureHTTP                = config.DefaultConnectAPISecureHTTP
)

func TestLoadLoggingConfiguration(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected slog.Level
	}{
		{name: "unset uses default", expected: DefaultLogLevel},
		{name: "invalid uses default", value: "verbose", expected: DefaultLogLevel},
		{name: "debug level is loaded", value: "debug", expected: slog.LevelDebug},
		{name: "warning level is loaded", value: "warn", expected: slog.LevelWarn},
		{name: "error level is loaded", value: "error", expected: slog.LevelError},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(LogLevelEnv, test.value)

			if got := LoadLoggingConfiguration().Level; got != test.expected {
				t.Fatalf("LoadLoggingConfiguration().Level = %v, want %v", got, test.expected)
			}
		})
	}
}

func TestLoadPollingInterval(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected time.Duration
	}{
		{name: "unset uses default", expected: DefaultPollingInterval},
		{name: "invalid uses default", value: "not-a-number", expected: DefaultPollingInterval},
		{name: "zero uses default", value: "0", expected: DefaultPollingInterval},
		{name: "negative uses default", value: "-1", expected: DefaultPollingInterval},
		{name: "valid value is milliseconds", value: "250", expected: 250 * time.Millisecond},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(PollingIntervalEnv, test.value)

			if got := LoadPollingInterval(); got != test.expected {
				t.Fatalf("LoadPollingInterval() = %v, want %v", got, test.expected)
			}
		})
	}
}

func TestLoadRestartFailedTasks(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected bool
	}{
		{name: "unset uses default", expected: DefaultRestartFailedTasks},
		{name: "invalid uses default", value: "sometimes", expected: DefaultRestartFailedTasks},
		{name: "true enables task restarts", value: "true", expected: true},
		{name: "false disables task restarts", value: "false", expected: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(PollingRestartFailedTasks, test.value)

			if got := LoadRestartFailedTasks(); got != test.expected {
				t.Fatalf("LoadRestartFailedTasks() = %t, want %t", got, test.expected)
			}
		})
	}
}

func TestLoadBackoffConfiguration(t *testing.T) {
	tests := []struct {
		name        string
		enabled     string
		baseDelayMS string
		maxDelayMS  string
		exponential string
		expected    BackoffConfiguration
	}{
		{
			name: "unset uses defaults",
			expected: BackoffConfiguration{
				Enabled:     DefaultRestartBackoffEnabled,
				BaseDelay:   DefaultRestartBackoffBaseDelay,
				MaxDelay:    DefaultRestartBackoffMaxDelay,
				Exponential: DefaultRestartBackoffExponentialEnabled,
			},
		},
		{
			name:        "valid values are loaded",
			enabled:     "true",
			baseDelayMS: "250",
			maxDelayMS:  "1000",
			exponential: "false",
			expected: BackoffConfiguration{
				Enabled:     true,
				BaseDelay:   250 * time.Millisecond,
				MaxDelay:    time.Second,
				Exponential: false,
			},
		},
		{
			name:        "disabled backoff retains the rest of its configuration",
			enabled:     "false",
			baseDelayMS: "250",
			maxDelayMS:  "1000",
			exponential: "false",
			expected: BackoffConfiguration{
				Enabled:     false,
				BaseDelay:   250 * time.Millisecond,
				MaxDelay:    time.Second,
				Exponential: false,
			},
		},
		{
			name:        "invalid values use defaults",
			enabled:     "sometimes",
			baseDelayMS: "not-a-number",
			maxDelayMS:  "not-a-number",
			exponential: "sometimes",
			expected: BackoffConfiguration{
				Enabled:     DefaultRestartBackoffEnabled,
				BaseDelay:   DefaultRestartBackoffBaseDelay,
				MaxDelay:    DefaultRestartBackoffMaxDelay,
				Exponential: DefaultRestartBackoffExponentialEnabled,
			},
		},
		{
			name:        "zero base delay uses default",
			baseDelayMS: "0",
			expected: BackoffConfiguration{
				Enabled:     DefaultRestartBackoffEnabled,
				BaseDelay:   DefaultRestartBackoffBaseDelay,
				MaxDelay:    DefaultRestartBackoffMaxDelay,
				Exponential: DefaultRestartBackoffExponentialEnabled,
			},
		},
		{
			name:        "negative base delay uses default",
			baseDelayMS: "-1",
			expected: BackoffConfiguration{
				Enabled:     DefaultRestartBackoffEnabled,
				BaseDelay:   DefaultRestartBackoffBaseDelay,
				MaxDelay:    DefaultRestartBackoffMaxDelay,
				Exponential: DefaultRestartBackoffExponentialEnabled,
			},
		},
		{
			name:        "maximum delay below the base delay uses the base delay",
			baseDelayMS: "500",
			maxDelayMS:  "250",
			expected: BackoffConfiguration{
				Enabled:     DefaultRestartBackoffEnabled,
				BaseDelay:   500 * time.Millisecond,
				MaxDelay:    500 * time.Millisecond,
				Exponential: DefaultRestartBackoffExponentialEnabled,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(RestartBackoffEnabled, test.enabled)
			t.Setenv(RestartBackoffBaseDelayMS, test.baseDelayMS)
			t.Setenv(RestartBackoffMaxDelayMS, test.maxDelayMS)
			t.Setenv(RestartBackoffExponentialEnabled, test.exponential)

			if got := LoadBackoffConfiguration(); got != test.expected {
				t.Fatalf("LoadBackoffConfiguration() = %#v, want %#v", got, test.expected)
			}
		})
	}
}

func TestLoadConfigIncludesPollingBehavior(t *testing.T) {
	t.Setenv(PollingIntervalEnv, "250")
	t.Setenv(PollingRestartFailedTasks, "false")
	t.Setenv(RestartBackoffEnabled, "false")
	t.Setenv(RestartBackoffBaseDelayMS, "500")
	t.Setenv(RestartBackoffMaxDelayMS, "1000")
	t.Setenv(RestartBackoffExponentialEnabled, "false")

	expected := PollingBehavior{
		Interval:           250 * time.Millisecond,
		RestartFailedTasks: false,
		Backoff: BackoffConfiguration{
			Enabled:     false,
			BaseDelay:   500 * time.Millisecond,
			MaxDelay:    time.Second,
			Exponential: false,
		},
	}
	if got := LoadConfig().PollingBehavior; got != expected {
		t.Fatalf("LoadConfig().PollingBehavior = %#v, want %#v", got, expected)
	}
}

func TestLoadConnectConfiguration(t *testing.T) {
	tests := []struct {
		name          string
		host          string
		port          string
		https         string
		expectedHost  string
		expectedPort  string
		expectedHTTPS bool
	}{
		{
			name:          "unset uses defaults",
			expectedHost:  DefaultConnectHost,
			expectedPort:  DefaultConnectPort,
			expectedHTTPS: DefaultConnectSecureHTTP,
		},
		{
			name:          "configured host and port",
			host:          "connect.example.test",
			port:          "9090",
			expectedHost:  "connect.example.test",
			expectedPort:  "9090",
			expectedHTTPS: false,
		},
		{
			name:          "HTTPS is loaded",
			https:         "true",
			expectedHost:  DefaultConnectHost,
			expectedPort:  DefaultConnectPort,
			expectedHTTPS: true,
		},
		{
			name:          "invalid HTTPS value uses default",
			https:         "sometimes",
			expectedHost:  DefaultConnectHost,
			expectedPort:  DefaultConnectPort,
			expectedHTTPS: DefaultConnectSecureHTTP,
		},
		{
			name:          "IPv6 host is loaded",
			host:          "2001:db8::1",
			port:          "8083",
			expectedHost:  "2001:db8::1",
			expectedPort:  "8083",
			expectedHTTPS: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(ConnectHostEnv, test.host)
			t.Setenv(ConnectPortEnv, test.port)
			t.Setenv(ConnectSecureHTTP, test.https)

			if got := LoadConnectConfiguration(); got.Host != test.expectedHost || got.Port != test.expectedPort || got.HTTPS != test.expectedHTTPS {
				t.Fatalf("LoadConnectConfiguration() = %#v, want host %q, port %q, HTTPS %t", got, test.expectedHost, test.expectedPort, test.expectedHTTPS)
			}
		})
	}
}

func TestLoadAuthConfiguration(t *testing.T) {
	tests := []struct {
		name     string
		enabled  string
		username string
		password string
		expected AuthConfiguration
	}{
		{
			name:     "unset disables authentication",
			expected: AuthConfiguration{Enabled: DefaultConnectBasicAuthEnabled},
		},
		{
			name:     "invalid value disables authentication",
			enabled:  "sometimes",
			expected: AuthConfiguration{Enabled: DefaultConnectBasicAuthEnabled},
		},
		{
			name:     "explicitly disabled authentication",
			enabled:  "false",
			expected: AuthConfiguration{Enabled: DefaultConnectBasicAuthEnabled},
		},
		{
			name:     "missing credentials disable authentication",
			enabled:  "true",
			username: "connect-user",
			expected: AuthConfiguration{Enabled: DefaultConnectBasicAuthEnabled},
		},
		{
			name:     "valid credentials enable authentication",
			enabled:  "true",
			username: "connect-user",
			password: "connect-password",
			expected: AuthConfiguration{
				Enabled:  true,
				Username: "connect-user",
				Password: "connect-password",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(ConnectBasicAuthEnabledEnv, test.enabled)
			t.Setenv(ConnectBasicAuthUsernameEnv, test.username)
			t.Setenv(ConnectBasicAuthPasswordEnv, test.password)

			if got := LoadAuthConfiguration(); got != test.expected {
				t.Fatalf("LoadAuthConfiguration() = %#v, want %#v", got, test.expected)
			}
		})
	}
}

func TestLoadConnectConfigurationDisablesBasicAuthWithoutHTTPS(t *testing.T) {
	t.Setenv(ConnectSecureHTTP, "false")
	t.Setenv(ConnectBasicAuthEnabledEnv, "true")
	t.Setenv(ConnectBasicAuthUsernameEnv, "connect-user")
	t.Setenv(ConnectBasicAuthPasswordEnv, "connect-password")

	if got := LoadConnectConfiguration().AuthConfig; got.Enabled {
		t.Fatalf("LoadConnectConfiguration().AuthConfig = %#v, want Basic Auth disabled without HTTPS", got)
	}
}

func TestLoadCommunicationConfiguration(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected time.Duration
	}{
		{name: "unset uses default", expected: DefaultHTTPRequestTimeout},
		{name: "invalid uses default", value: "not-a-number", expected: DefaultHTTPRequestTimeout},
		{name: "zero uses default", value: "0", expected: DefaultHTTPRequestTimeout},
		{name: "negative uses default", value: "-1", expected: DefaultHTTPRequestTimeout},
		{name: "valid value is milliseconds", value: "250", expected: 250 * time.Millisecond},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(HTTPRequestTimeout, test.value)

			if got := LoadCommunicationConfiguration().RequestTimeout; got != test.expected {
				t.Fatalf("LoadCommunicationConfiguration().RequestTimeout = %v, want %v", got, test.expected)
			}
		})
	}
}
