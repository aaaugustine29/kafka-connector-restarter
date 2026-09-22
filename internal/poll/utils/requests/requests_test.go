package requests

import (
	"context"
	"net/http"
	"testing"

	"entropicworks.com/kafka-connector-restarter/internal/config"
)

func TestNewConnectAPIBuildsBaseURL(t *testing.T) {
	tests := []struct {
		name          string
		configuration config.ConnectAPIConfiguration
		wantURL       string
	}{
		{
			name:          "HTTP",
			configuration: config.ConnectAPIConfiguration{Host: "localhost", Port: "8083"},
			wantURL:       "http://localhost:8083",
		},
		{
			name:          "HTTPS",
			configuration: config.ConnectAPIConfiguration{Host: "connect.example.test", Port: "8443", HTTPS: true},
			wantURL:       "https://connect.example.test:8443",
		},
		{
			name:          "IPv6",
			configuration: config.ConnectAPIConfiguration{Host: "2001:db8::1", Port: "8083"},
			wantURL:       "http://[2001:db8::1]:8083",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connect := NewConnectAPI(&http.Client{}, test.configuration)
			if connect.BaseURL != test.wantURL {
				t.Fatalf("BaseURL = %q, want %q", connect.BaseURL, test.wantURL)
			}
		})
	}
}

func TestConnectAPINewRequestBasicAuth(t *testing.T) {
	tests := []struct {
		name     string
		auth     config.AuthConfiguration
		expected bool
	}{
		{
			name:     "disabled authentication omits credentials",
			expected: false,
		},
		{
			name: "enabled authentication adds credentials",
			auth: config.AuthConfiguration{
				Enabled:  true,
				Username: "connect-user",
				Password: "connect-password",
			},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connect := ConnectAPI{Auth: test.auth}
			request, err := connect.NewRequest(context.Background(), http.MethodGet, "https://connect.example.test/connectors")
			if err != nil {
				t.Fatalf("NewRequest() error = %v", err)
			}

			username, password, ok := request.BasicAuth()
			if ok != test.expected {
				t.Fatalf("request.BasicAuth() found credentials = %t, want %t", ok, test.expected)
			}
			if test.expected && (username != test.auth.Username || password != test.auth.Password) {
				t.Fatalf("request.BasicAuth() = (%q, %q), want (%q, %q)", username, password, test.auth.Username, test.auth.Password)
			}
		})
	}
}
