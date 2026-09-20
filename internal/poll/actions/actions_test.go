package actions

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
	"entropicworks.com/kafka-connector-restarter/internal/poll/status"
	"entropicworks.com/kafka-connector-restarter/internal/poll/utils/requests"
)

func TestDetermineActionsForConnector(t *testing.T) {
	tests := []struct {
		name         string
		connector    status.ConnectorStatus
		restartTasks bool
		expected     []RemediationAction
	}{
		{
			name: "unhealthy connector is restarted",
			connector: status.ConnectorStatus{
				Name:      "source-connector",
				Connector: status.WorkerStatus{State: "FAILED"},
				Tasks:     []status.TaskStatus{{ID: 0, State: "FAILED"}},
			},
			restartTasks: false,
			expected: []RemediationAction{
				{
					ConnectorName: "source-connector",
					Kind:          RestartConnector,
				},
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
			expected:     nil,
		},
		{
			name: "healthy connector creates one action per failed task",
			connector: status.ConnectorStatus{
				Name:      "mixed-connector",
				Connector: status.WorkerStatus{State: "RUNNING"},
				Tasks: []status.TaskStatus{
					{ID: 0, State: "RUNNING"},
					{ID: 1, State: "FAILED"},
					{ID: 2, State: "FAILED"},
				},
			},
			restartTasks: true,
			expected: []RemediationAction{
				{
					ConnectorName: "mixed-connector",
					Kind:          RestartTask,
					TaskID:        1,
				},
				{
					ConnectorName: "mixed-connector",
					Kind:          RestartTask,
					TaskID:        2,
				},
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
			expected:     nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := DetermineActionsForConnector(test.connector, test.restartTasks); !reflect.DeepEqual(got, test.expected) {
				t.Fatalf("DetermineActionsForConnector() = %#v, want %#v", got, test.expected)
			}
		})
	}
}

func TestTakeAction(t *testing.T) {
	type expectedRequest struct {
		path       string
		rawQuery   string
		statusCode int
	}

	tests := []struct {
		name             string
		action           RemediationAction
		expectedRequests []expectedRequest
		auth             environment.AuthConfiguration
		expectedAuth     bool
		wantError        bool
	}{
		{
			name:   "OK connector restart is accepted",
			action: RemediationAction{ConnectorName: "source-connector", Kind: RestartConnector},
			expectedRequests: []expectedRequest{{
				path:       "/connectors/source-connector/restart",
				rawQuery:   "includeTasks=true&onlyFailed=true",
				statusCode: http.StatusOK,
			}},
		},
		{
			name:   "accepted task restart is accepted",
			action: RemediationAction{ConnectorName: "sink-connector", Kind: RestartTask, TaskID: 1},
			expectedRequests: []expectedRequest{{
				path:       "/connectors/sink-connector/tasks/1/restart",
				statusCode: http.StatusAccepted,
			}},
		},
		{
			name:      "unknown action kind returns an error",
			action:    RemediationAction{ConnectorName: "source-connector", Kind: ActionKind("unknown")},
			wantError: true,
		},
		{
			name:   "Basic Auth is sent",
			action: RemediationAction{ConnectorName: "source-connector", Kind: RestartConnector},
			expectedRequests: []expectedRequest{{
				path:       "/connectors/source-connector/restart",
				rawQuery:   "includeTasks=true&onlyFailed=true",
				statusCode: http.StatusAccepted,
			}},
			auth: environment.AuthConfiguration{
				Enabled:  true,
				Username: "connect-user",
				Password: "connect-password",
			},
			expectedAuth: true,
		},
		{
			name:   "unauthorized returns an error",
			action: RemediationAction{ConnectorName: "source-connector", Kind: RestartConnector},
			expectedRequests: []expectedRequest{{
				path:       "/connectors/source-connector/restart",
				rawQuery:   "includeTasks=true&onlyFailed=true",
				statusCode: http.StatusUnauthorized,
			}},
			wantError: true,
		},
		{
			name:   "rebalance conflict returns an error",
			action: RemediationAction{ConnectorName: "source-connector", Kind: RestartConnector},
			expectedRequests: []expectedRequest{{
				path:       "/connectors/source-connector/restart",
				rawQuery:   "includeTasks=true&onlyFailed=true",
				statusCode: http.StatusConflict,
			}},
			wantError: true,
		},
		{
			name:   "server error returns an error",
			action: RemediationAction{ConnectorName: "source-connector", Kind: RestartConnector},
			expectedRequests: []expectedRequest{{
				path:       "/connectors/source-connector/restart",
				rawQuery:   "includeTasks=true&onlyFailed=true",
				statusCode: http.StatusInternalServerError,
			}},
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requestIndex := 0
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if requestIndex >= len(test.expectedRequests) {
					t.Errorf("received unexpected request %s", request.URL)
					response.WriteHeader(http.StatusInternalServerError)
					return
				}

				expected := test.expectedRequests[requestIndex]
				requestIndex++

				if request.Method != http.MethodPost {
					t.Errorf("request method = %s, want %s", request.Method, http.MethodPost)
				}
				if request.URL.Path != expected.path {
					t.Errorf("request path = %s, want %s", request.URL.Path, expected.path)
				}
				if request.URL.RawQuery != expected.rawQuery {
					t.Errorf("request query = %s, want %s", request.URL.RawQuery, expected.rawQuery)
				}

				username, password, ok := request.BasicAuth()
				if ok != test.expectedAuth {
					t.Errorf("request.BasicAuth() found credentials = %t, want %t", ok, test.expectedAuth)
				}
				if test.expectedAuth && (username != test.auth.Username || password != test.auth.Password) {
					t.Errorf("request.BasicAuth() = (%q, %q), want (%q, %q)", username, password, test.auth.Username, test.auth.Password)
				}

				response.WriteHeader(expected.statusCode)
			}))
			defer server.Close()

			connect := requests.ConnectAPI{
				HTTPClient: server.Client(),
				BaseURL:    server.URL,
				Auth:       test.auth,
			}
			err := TakeAction(context.Background(), test.action, connect)
			if (err != nil) != test.wantError {
				t.Fatalf("TakeAction() error = %v, want error = %t", err, test.wantError)
			}
			if requestIndex != len(test.expectedRequests) {
				t.Fatalf("received %d requests, want %d", requestIndex, len(test.expectedRequests))
			}
		})
	}
}
