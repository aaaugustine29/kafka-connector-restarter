package environment

import (
	"testing"
	"time"
)

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

func TestLoadConfigIncludesPollingBehavior(t *testing.T) {
	t.Setenv(PollingIntervalEnv, "250")
	t.Setenv(PollingRestartFailedTasks, "false")

	if got := LoadConfig().PollingBehavior; got.Interval != 250*time.Millisecond || got.RestartFailedTasks {
		t.Fatalf("LoadConfig().PollingBehavior = %#v, want interval 250ms and RestartFailedTasks false", got)
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
		expectedURL   string
	}{
		{
			name:          "unset uses defaults",
			expectedHost:  DefaultConnectHost,
			expectedPort:  DefaultConnectPort,
			expectedHTTPS: DefaultConnectSecureHTTP,
			expectedURL:   "http://localhost:8083",
		},
		{
			name:          "configured host and port",
			host:          "connect.example.test",
			port:          "9090",
			expectedHost:  "connect.example.test",
			expectedPort:  "9090",
			expectedHTTPS: false,
			expectedURL:   "http://connect.example.test:9090",
		},
		{
			name:          "HTTPS uses a secure URL",
			https:         "true",
			expectedHost:  DefaultConnectHost,
			expectedPort:  DefaultConnectPort,
			expectedHTTPS: true,
			expectedURL:   "https://localhost:8083",
		},
		{
			name:          "invalid HTTPS value uses default",
			https:         "sometimes",
			expectedHost:  DefaultConnectHost,
			expectedPort:  DefaultConnectPort,
			expectedHTTPS: DefaultConnectSecureHTTP,
			expectedURL:   "http://localhost:8083",
		},
		{
			name:          "IPv6 host is formatted safely",
			host:          "2001:db8::1",
			port:          "8083",
			expectedHost:  "2001:db8::1",
			expectedPort:  "8083",
			expectedHTTPS: false,
			expectedURL:   "http://[2001:db8::1]:8083",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(ConnectHostEnv, test.host)
			t.Setenv(ConnectPortEnv, test.port)
			t.Setenv(ConnectSecureHTTP, test.https)

			if got := LoadConnectConfiguration(); got.Host != test.expectedHost || got.Port != test.expectedPort || got.HTTPS != test.expectedHTTPS || got.URL != test.expectedURL {
				t.Fatalf("LoadConnectConfiguration() = %#v, want host %q, port %q, HTTPS %t, URL %q", got, test.expectedHost, test.expectedPort, test.expectedHTTPS, test.expectedURL)
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
