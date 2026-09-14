package poll

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
)

func TestMapConnectorStatusResponse(t *testing.T) {
	response := &http.Response{
		Body: io.NopCloser(strings.NewReader(`{
			"FileStreamSinkConnectorConnector_0": {
				"status": {
					"name": "FileStreamSinkConnectorConnector_0",
					"connector": {"state": "RUNNING", "worker_id": "10.0.0.162:8083"},
					"tasks": [{"id": 0, "state": "RUNNING", "worker_id": "10.0.0.162:8083"}],
					"type": "sink"
				}
			}
		}`)),
	}

	connectorStatuses, err := mapConnectorStatusResponse(response)
	if err != nil {
		t.Fatalf("mapConnectorStatusResponse() error = %v", err)
	}

	status, ok := connectorStatuses["FileStreamSinkConnectorConnector_0"]
	if !ok {
		t.Fatal("connector status was not mapped")
	}

	if status.Connector.State != "RUNNING" || status.Connector.WorkerID != "10.0.0.162:8083" {
		t.Fatalf("connector status = %#v, want a running connector on 10.0.0.162:8083", status.Connector)
	}

	if len(status.Tasks) != 1 || status.Tasks[0].ID != 0 || status.Tasks[0].WorkerID != "10.0.0.162:8083" {
		t.Fatalf("task statuses = %#v, want one running task", status.Tasks)
	}
}

func TestNewRequestBasicAuth(t *testing.T) {
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
			connect := connectAPI{auth: test.auth}
			request, err := connect.newRequest(context.Background(), http.MethodGet, "https://connect.example.test/connectors")
			if err != nil {
				t.Fatalf("newRequest() error = %v", err)
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
