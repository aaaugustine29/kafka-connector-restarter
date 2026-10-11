package actions

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/requests"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/status"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func TestTakeActionLogsRestartRequests(t *testing.T) {
	for _, test := range []struct {
		kind       ActionKind
		statusCode int
	}{
		{RestartConnector, http.StatusAccepted},
		{RestartTask, http.StatusNoContent},
		{RestartTask, http.StatusInternalServerError},
	} {
		t.Run(string(test.kind)+" "+http.StatusText(test.statusCode), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(test.statusCode)
			}))
			defer server.Close()
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, nil)).With("connect_cluster", "test-cluster", "endpoint", server.URL)
			_, err := TakeAction(t.Context(), RemediationAction{ConnectorName: "test-connector", Kind: test.kind, TaskID: 2},
				requests.ConnectAPI{HTTPClient: server.Client(), BaseURL: server.URL}, logger)
			if (err != nil) != (test.statusCode >= 300) {
				t.Fatalf("error = %v", err)
			}
			lines := strings.Split(strings.TrimSpace(output.String()), "\n")
			wantLines := 2
			if test.statusCode >= 300 {
				wantLines = 1
			}
			if len(lines) != wantLines {
				t.Fatalf("logs = %s", output.String())
			}
			kind := "connector"
			if test.kind == RestartTask {
				kind = "task"
			}
			for index, line := range lines {
				var entry map[string]any
				if err := json.Unmarshal([]byte(line), &entry); err != nil {
					t.Fatal(err)
				}
				wantMessage := "requesting " + kind + " restart"
				if index == 1 {
					wantMessage = kind + " restart request accepted"
				}
				if entry["level"] != "INFO" || entry["msg"] != wantMessage || entry["connector"] != "test-connector" || entry["connect_cluster"] != "test-cluster" || entry["endpoint"] != server.URL || entry["action"] != string(test.kind) {
					t.Fatalf("log entry = %v", entry)
				}
				_, hasTask := entry["task_id"]
				if hasTask != (test.kind == RestartTask) {
					t.Fatalf("task ID presence = %v", hasTask)
				}
				if hasTask && entry["task_id"] != float64(2) {
					t.Fatalf("task ID = %v", entry["task_id"])
				}
			}
		})
	}
}

func (function roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestTakeActionRejectsRedirectAndRecordsAttempt(t *testing.T) {
	var loginRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" {
			loginRequests.Add(1)
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(w, r, "/login", http.StatusFound)
	}))
	defer server.Close()
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	connect := requests.NewConnectAPI(connectcluster.Configuration{
		Host: serverURL.Hostname(), Port: serverURL.Port(),
	}, time.Second)
	result, err := TakeAction(context.Background(), RemediationAction{
		ConnectorName: "source-connector", Kind: RestartConnector,
	}, connect, slog.Default())
	if err == nil {
		t.Fatal("redirected remediation request was reported as accepted")
	}
	if result.StatusCode != http.StatusFound || !result.RequestAttempted || result.AttemptedAt.IsZero() {
		t.Fatalf("result = %#v, want the original 302 and a recorded attempt", result)
	}
	if loginRequests.Load() != 0 {
		t.Fatal("remediation request followed the redirect to the login page")
	}
}

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
			if got := DetermineActionsForConnector(test.connector, test.restartTasks, slog.Default()); !slices.Equal(got, test.expected) {
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
		auth             connectcluster.AuthConfiguration
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
			auth: connectcluster.AuthConfiguration{
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
			result, err := TakeAction(context.Background(), test.action, connect, slog.Default())
			if (err != nil) != test.wantError {
				t.Fatalf("TakeAction() error = %v, want error = %t", err, test.wantError)
			}

			expectedRequestAttempted := len(test.expectedRequests) > 0
			if result.RequestAttempted != expectedRequestAttempted {
				t.Fatalf("TakeAction() RequestAttempted = %t, want %t", result.RequestAttempted, expectedRequestAttempted)
			}

			expectedStatusCode := 0
			if expectedRequestAttempted {
				expectedStatusCode = test.expectedRequests[0].statusCode
			}
			if result.StatusCode != expectedStatusCode {
				t.Fatalf("TakeAction() StatusCode = %d, want %d", result.StatusCode, expectedStatusCode)
			}

			if expectedRequestAttempted && result.AttemptedAt.IsZero() {
				t.Fatal("TakeAction() AttemptedAt is zero after making a request")
			}
			if !expectedRequestAttempted && !result.AttemptedAt.IsZero() {
				t.Fatalf("TakeAction() AttemptedAt = %v, want zero time", result.AttemptedAt)
			}
			if requestIndex != len(test.expectedRequests) {
				t.Fatalf("received %d requests, want %d", requestIndex, len(test.expectedRequests))
			}
		})
	}
}

func TestTakeActionRecordsFailedRequest(t *testing.T) {
	connect := requests.ConnectAPI{
		HTTPClient: &http.Client{
			Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("connection refused")
			}),
		},
		BaseURL: "http://connect.example.test",
	}

	result, err := TakeAction(
		context.Background(),
		RemediationAction{ConnectorName: "source-connector", Kind: RestartConnector},
		connect,
		slog.Default(),
	)
	if err == nil {
		t.Fatal("TakeAction() error = nil, want an error")
	}
	if !result.RequestAttempted {
		t.Fatal("TakeAction() RequestAttempted = false, want true")
	}
	if result.AttemptedAt.IsZero() {
		t.Fatal("TakeAction() AttemptedAt is zero after making a request")
	}
	if result.StatusCode != 0 {
		t.Fatalf("TakeAction() StatusCode = %d, want 0", result.StatusCode)
	}
}

func TestGenerateRemediationActionURLEscapesConnectorName(t *testing.T) {
	for _, test := range []struct {
		kind ActionKind
		path string
	}{
		{RestartConnector, "/connectors/name%2Fwith%20%3F%23%25/restart"},
		{RestartTask, "/connectors/name%2Fwith%20%3F%23%25/tasks/0/restart"},
	} {
		t.Run(string(test.kind), func(t *testing.T) {
			requestURL, err := generateRemediationActionURL(
				requests.ConnectAPI{BaseURL: "http://connect.example.test"},
				RemediationAction{ConnectorName: "name/with ?#%", Kind: test.kind},
			)
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := url.Parse(requestURL)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.EscapedPath() != test.path || parsed.Fragment != "" {
				t.Fatalf("unexpected remediation URL: %s", requestURL)
			}
			if test.kind == RestartConnector && parsed.RawQuery != "includeTasks=true&onlyFailed=true" {
				t.Fatalf("connector name changed the query: %s", requestURL)
			}
			if test.kind == RestartTask && parsed.RawQuery != "" {
				t.Fatalf("connector name added a query: %s", requestURL)
			}
		})
	}
}

func TestRemediationActionLoggerTaskID(t *testing.T) {
	for _, kind := range []ActionKind{RestartConnector, RestartTask} {
		t.Run(string(kind), func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil)).With("connect_cluster", "production")
			RemediationAction{ConnectorName: "connector", Kind: kind, TaskID: 0}.Logger(logger).Info("test")
			var record map[string]jsontext.Value
			if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if string(record["connect_cluster"]) != `"production"` {
				t.Fatal("action logger lost the cluster identity")
			}
			taskID, present := record["task_id"]
			if present != (kind == RestartTask) {
				t.Fatalf("task_id present = %t for action %s", present, kind)
			}
			if present && string(taskID) != "0" {
				t.Fatalf("task_id = %s, want 0", taskID)
			}
		})
	}
}
