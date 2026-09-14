package actions

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
	"entropicworks.com/kafka-connector-restarter/internal/poll/status"
)

func TestDetermineAction(t *testing.T) {
	tests := []struct {
		name         string
		connector    status.ConnectorStatus
		restartTasks bool
		expected     RemediationAction
	}{
		{
			name: "unhealthy connector is restarted",
			connector: status.ConnectorStatus{
				Name:      "source-connector",
				Connector: status.WorkerStatus{State: "FAILED"},
				Tasks:     []status.TaskStatus{{ID: 0, State: "FAILED"}},
			},
			restartTasks: false,
			expected: RemediationAction{
				ConnectorName: "source-connector",
				Restart:       true,
			},
		},
		{
			name: "healthy connector with healthy tasks needs no action",
			connector: status.ConnectorStatus{
				Name:      "sink-connector",
				Connector: status.WorkerStatus{State: "running"},
				Tasks: []status.TaskStatus{
					{ID: 0, State: "RUNNING"},
					{ID: 1, State: "running"},
				},
			},
			restartTasks: true,
			expected:     RemediationAction{ConnectorName: "sink-connector"},
		},
		{
			name: "healthy connector restarts only failed tasks",
			connector: status.ConnectorStatus{
				Name:      "mixed-connector",
				Connector: status.WorkerStatus{State: "RUNNING"},
				Tasks: []status.TaskStatus{
					{ID: 0, State: "RUNNING"},
					{ID: 1, State: "FAILED"},
					{ID: 2, State: "PAUSED"},
				},
			},
			restartTasks: true,
			expected: RemediationAction{
				ConnectorName:        "mixed-connector",
				TaskIDsToBeRestarted: []int{1},
			},
		},
		{
			name: "task restart policy skips failed tasks",
			connector: status.ConnectorStatus{
				Name:      "mixed-connector",
				Connector: status.WorkerStatus{State: "RUNNING"},
				Tasks: []status.TaskStatus{
					{ID: 1, State: "FAILED"},
				},
			},
			restartTasks: false,
			expected:     RemediationAction{ConnectorName: "mixed-connector"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := DetermineAction(test.connector, test.restartTasks); !reflect.DeepEqual(got, test.expected) {
				t.Fatalf("DetermineAction() = %#v, want %#v", got, test.expected)
			}
		})
	}
}

func TestGenerateRemediationActionURLs(t *testing.T) {
	tests := []struct {
		name     string
		actions  []RemediationAction
		expected []string
	}{
		{
			name: "failed connector generates connector restart URL",
			actions: []RemediationAction{
				{ConnectorName: "source-connector", Restart: true},
			},
			expected: []string{
				"http://connect.example.test:8083/connectors/source-connector/restart?includeTasks=true&onlyFailed=true",
			},
		},
		{
			name: "failed tasks generate task restart URLs",
			actions: []RemediationAction{
				{ConnectorName: "sink-connector", TaskIDsToBeRestarted: []int{1, 3}},
			},
			expected: []string{
				"http://connect.example.test:8083/connectors/sink-connector/tasks/1/restart",
				"http://connect.example.test:8083/connectors/sink-connector/tasks/3/restart",
			},
		},
		{
			name: "connector restart takes priority over task restarts",
			actions: []RemediationAction{
				{ConnectorName: "source-connector", Restart: true, TaskIDsToBeRestarted: []int{1}},
			},
			expected: []string{
				"http://connect.example.test:8083/connectors/source-connector/restart?includeTasks=true&onlyFailed=true",
			},
		},
		{
			name:     "no remediation creates no URLs",
			actions:  []RemediationAction{{ConnectorName: "healthy-connector"}},
			expected: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connect := status.ConnectAPI{BaseURL: "http://connect.example.test:8083"}

			if got := GenerateRemediationActionURLs(test.actions, connect); !reflect.DeepEqual(got, test.expected) {
				t.Fatalf("GenerateRemediationActionURLs() = %#v, want %#v", got, test.expected)
			}
		})
	}
}

func TestTakeAction(t *testing.T) {
	tests := []struct {
		name         string
		statusCode   int
		auth         environment.AuthConfiguration
		expectedAuth bool
		wantError    bool
	}{
		{name: "OK is accepted", statusCode: http.StatusOK},
		{name: "accepted is accepted", statusCode: http.StatusAccepted},
		{name: "no content is accepted", statusCode: http.StatusNoContent},
		{
			name:       "Basic Auth is sent",
			statusCode: http.StatusAccepted,
			auth: environment.AuthConfiguration{
				Enabled:  true,
				Username: "connect-user",
				Password: "connect-password",
			},
			expectedAuth: true,
		},
		{name: "unauthorized returns an error", statusCode: http.StatusUnauthorized, wantError: true},
		{name: "rebalance conflict returns an error", statusCode: http.StatusConflict, wantError: true},
		{name: "server error returns an error", statusCode: http.StatusInternalServerError, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodPost {
					t.Errorf("request method = %s, want %s", request.Method, http.MethodPost)
				}
				if request.URL.Path != "/connectors/source-connector/restart" {
					t.Errorf("request path = %s, want /connectors/source-connector/restart", request.URL.Path)
				}

				username, password, ok := request.BasicAuth()
				if ok != test.expectedAuth {
					t.Errorf("request.BasicAuth() found credentials = %t, want %t", ok, test.expectedAuth)
				}
				if test.expectedAuth && (username != test.auth.Username || password != test.auth.Password) {
					t.Errorf("request.BasicAuth() = (%q, %q), want (%q, %q)", username, password, test.auth.Username, test.auth.Password)
				}

				response.WriteHeader(test.statusCode)
			}))
			defer server.Close()

			connect := status.ConnectAPI{
				HTTPClient: server.Client(),
				Auth:       test.auth,
			}
			err := TakeAction(context.Background(), server.URL+"/connectors/source-connector/restart", connect)
			if (err != nil) != test.wantError {
				t.Fatalf("TakeAction() error = %v, want error = %t", err, test.wantError)
			}
		})
	}
}
