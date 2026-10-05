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
	DefaultConnectHTTPS                     = config.DefaultConnectAPIHTTPS
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
		expected config.Duration
	}{
		{name: "unset uses default", expected: DefaultPollingInterval},
		{name: "invalid uses default", value: "not-a-number", expected: DefaultPollingInterval},
		{name: "zero uses default", value: "0s", expected: DefaultPollingInterval},
		{name: "negative uses default", value: "-1s", expected: DefaultPollingInterval},
		{name: "missing units uses default", value: "250", expected: DefaultPollingInterval},
		{name: "overflow uses default", value: "9223372036854775808ns", expected: DefaultPollingInterval},
		{name: "milliseconds are loaded", value: "250ms", expected: config.Duration(250 * time.Millisecond)},
		{name: "fractional seconds are loaded", value: "1.5s", expected: config.Duration(1500 * time.Millisecond)},
		{name: "compound duration is loaded", value: "1m30s", expected: config.Duration(90 * time.Second)},
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
		baseDelay   string
		maxDelay    string
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
			baseDelay:   "250ms",
			maxDelay:    "1s",
			exponential: "false",
			expected: BackoffConfiguration{
				Enabled:     true,
				BaseDelay:   config.Duration(250 * time.Millisecond),
				MaxDelay:    config.Duration(time.Second),
				Exponential: false,
			},
		},
		{
			name:        "disabled backoff retains the rest of its configuration",
			enabled:     "false",
			baseDelay:   "250ms",
			maxDelay:    "1s",
			exponential: "false",
			expected: BackoffConfiguration{
				Enabled:     false,
				BaseDelay:   config.Duration(250 * time.Millisecond),
				MaxDelay:    config.Duration(time.Second),
				Exponential: false,
			},
		},
		{
			name:        "invalid values use defaults",
			enabled:     "sometimes",
			baseDelay:   "not-a-duration",
			maxDelay:    "not-a-duration",
			exponential: "sometimes",
			expected: BackoffConfiguration{
				Enabled:     DefaultRestartBackoffEnabled,
				BaseDelay:   DefaultRestartBackoffBaseDelay,
				MaxDelay:    DefaultRestartBackoffMaxDelay,
				Exponential: DefaultRestartBackoffExponentialEnabled,
			},
		},
		{
			name:      "zero base delay uses default",
			baseDelay: "0s",
			expected: BackoffConfiguration{
				Enabled:     DefaultRestartBackoffEnabled,
				BaseDelay:   DefaultRestartBackoffBaseDelay,
				MaxDelay:    DefaultRestartBackoffMaxDelay,
				Exponential: DefaultRestartBackoffExponentialEnabled,
			},
		},
		{
			name:      "negative base delay uses default",
			baseDelay: "-1s",
			expected: BackoffConfiguration{
				Enabled:     DefaultRestartBackoffEnabled,
				BaseDelay:   DefaultRestartBackoffBaseDelay,
				MaxDelay:    DefaultRestartBackoffMaxDelay,
				Exponential: DefaultRestartBackoffExponentialEnabled,
			},
		},
		{
			name:      "maximum delay below the base delay uses the base delay",
			baseDelay: "500ms",
			maxDelay:  "250ms",
			expected: BackoffConfiguration{
				Enabled:     DefaultRestartBackoffEnabled,
				BaseDelay:   config.Duration(500 * time.Millisecond),
				MaxDelay:    config.Duration(500 * time.Millisecond),
				Exponential: DefaultRestartBackoffExponentialEnabled,
			},
		},
		{
			name:      "overflowing durations use defaults",
			baseDelay: "9223372036854775808ns",
			maxDelay:  "9223372036854775808ns",
			expected:  config.DefaultBackoffConfiguration(),
		},
		{
			name:      "durations without units use defaults",
			baseDelay: "250",
			maxDelay:  "1000",
			expected:  config.DefaultBackoffConfiguration(),
		},
		{
			name:     "zero maximum delay uses default",
			maxDelay: "0s",
			expected: config.DefaultBackoffConfiguration(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(RestartBackoffEnabled, test.enabled)
			t.Setenv(RestartBackoffBaseDelayEnv, test.baseDelay)
			t.Setenv(RestartBackoffMaxDelayEnv, test.maxDelay)
			t.Setenv(RestartBackoffExponentialEnabled, test.exponential)

			if got := LoadBackoffConfiguration(); got != test.expected {
				t.Fatalf("LoadBackoffConfiguration() = %#v, want %#v", got, test.expected)
			}
		})
	}
}

func TestLoadConfigIncludesDurationConfiguration(t *testing.T) {
	t.Setenv(PollingIntervalEnv, "250ms")
	t.Setenv(HTTPRequestTimeoutEnv, "1.5s")
	t.Setenv(PollingRestartFailedTasks, "false")
	t.Setenv(RestartBackoffEnabled, "false")
	t.Setenv(RestartBackoffBaseDelayEnv, "500ms")
	t.Setenv(RestartBackoffMaxDelayEnv, "1s")
	t.Setenv(RestartBackoffExponentialEnabled, "false")

	expected := PollingBehavior{
		Interval:           config.Duration(250 * time.Millisecond),
		RestartFailedTasks: false,
		Backoff: BackoffConfiguration{
			Enabled:     false,
			BaseDelay:   config.Duration(500 * time.Millisecond),
			MaxDelay:    config.Duration(time.Second),
			Exponential: false,
		},
	}
	if got := LoadConfig().PollingBehavior; got != expected {
		t.Fatalf("LoadConfig().PollingBehavior = %#v, want %#v", got, expected)
	}
	if got := LoadConfig().CommunicationConfig.RequestTimeout; got != config.Duration(1500*time.Millisecond) {
		t.Fatalf("LoadConfig().CommunicationConfig.RequestTimeout = %v, want 1.5s", got)
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
			expectedHTTPS: DefaultConnectHTTPS,
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
			expectedHTTPS: DefaultConnectHTTPS,
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
		expected config.Duration
	}{
		{name: "unset uses default", expected: DefaultHTTPRequestTimeout},
		{name: "invalid uses default", value: "not-a-number", expected: DefaultHTTPRequestTimeout},
		{name: "zero uses default", value: "0s", expected: DefaultHTTPRequestTimeout},
		{name: "negative uses default", value: "-1s", expected: DefaultHTTPRequestTimeout},
		{name: "missing units uses default", value: "250", expected: DefaultHTTPRequestTimeout},
		{name: "overflow uses default", value: "9223372036854775808ns", expected: DefaultHTTPRequestTimeout},
		{name: "milliseconds are loaded", value: "250ms", expected: config.Duration(250 * time.Millisecond)},
		{name: "seconds are loaded", value: "2s", expected: config.Duration(2 * time.Second)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(HTTPRequestTimeoutEnv, test.value)

			if got := LoadCommunicationConfiguration().RequestTimeout; got != test.expected {
				t.Fatalf("LoadCommunicationConfiguration().RequestTimeout = %v, want %v", got, test.expected)
			}
		})
	}
}
