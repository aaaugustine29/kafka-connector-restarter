package requests

import (
	"context"
	"net/http"
	"testing"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
)

func TestConnectAPINewRequestBasicAuth(t *testing.T) {
	tests := []struct {
		name     string
		auth     environment.AuthConfiguration
		expected bool
	}{
		{
			name:     "disabled authentication omits credentials",
			expected: false,
		},
		{
			name: "enabled authentication adds credentials",
			auth: environment.AuthConfiguration{
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
