package poll

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"testing"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/config"
	"entropicworks.com/kafka-connector-restarter/internal/poll/actions"
	"entropicworks.com/kafka-connector-restarter/internal/poll/backoff"
	"entropicworks.com/kafka-connector-restarter/internal/poll/status"
)

func TestFilterByBackoffs(t *testing.T) {
	recentAttempt := time.Now().Add(-30 * time.Minute)
	backoffConfig := config.BackoffConfiguration{BaseDelay: time.Hour}

	tests := []struct {
		name     string
		statuses map[string]backoff.BackoffStatus
		actions  []actions.RemediationAction
		expected []actions.RemediationAction
	}{
		{
			name: "actions without a prior attempt are retained",
			actions: []actions.RemediationAction{
				{ConnectorName: "source-connector", Kind: actions.RestartConnector},
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 1},
			},
			expected: []actions.RemediationAction{
				{ConnectorName: "source-connector", Kind: actions.RestartConnector},
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 1},
			},
		},
		{
			name: "connector restart in the window is omitted",
			statuses: map[string]backoff.BackoffStatus{
				fmt.Sprintf(backoff.ConnectorKeyFormat, "source-connector"): {
					LastAttemptTime: recentAttempt,
				},
			},
			actions: []actions.RemediationAction{
				{ConnectorName: "source-connector", Kind: actions.RestartConnector},
			},
			expected: nil,
		},
		{
			name: "task restart in the window is omitted",
			statuses: map[string]backoff.BackoffStatus{
				fmt.Sprintf(backoff.TaskKeyFormat, "sink-connector", 1): {
					LastAttemptTime: recentAttempt,
				},
			},
			actions: []actions.RemediationAction{
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 1},
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 2},
			},
			expected: []actions.RemediationAction{
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 2},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filter := backoff.BackoffFilter{
				BackoffConfig:   backoffConfig,
				BackoffStatuses: test.statuses,
			}

			if got := FilterByBackoffs(filter, test.actions); !reflect.DeepEqual(got, test.expected) {
				t.Fatalf("FilterByBackoffs() = %#v, want %#v", got, test.expected)
			}
		})
	}
}

func TestResetBackoffsForHealthyStatuses(t *testing.T) {
	filter := backoff.BackoffFilter{
		BackoffStatuses: map[string]backoff.BackoffStatus{
			"healthy-connector": {Attempts: 1},
			fmt.Sprintf(backoff.TaskKeyFormat, "healthy-connector", 1): {Attempts: 1},
			fmt.Sprintf(backoff.TaskKeyFormat, "healthy-connector", 2): {Attempts: 1},
			"failed-connector": {Attempts: 1},
			fmt.Sprintf(backoff.TaskKeyFormat, "failed-connector", 1): {Attempts: 1},
		},
	}
	connectorStatuses := map[string]status.ConnectorStatus{
		"healthy-connector": {
			Name:      "healthy-connector",
			Connector: status.WorkerStatus{State: "RUNNING"},
			Tasks: []status.TaskStatus{
				{ID: 1, State: "RUNNING"},
				{ID: 2, State: "FAILED"},
			},
		},
		"failed-connector": {
			Name:      "failed-connector",
			Connector: status.WorkerStatus{State: "FAILED"},
			Tasks:     []status.TaskStatus{{ID: 1, State: "FAILED"}},
		},
	}

	resetBackoffsForHealthyStatuses(&filter, connectorStatuses)

	expected := map[string]backoff.BackoffStatus{
		fmt.Sprintf(backoff.TaskKeyFormat, "healthy-connector", 2): {Attempts: 1},
		"failed-connector": {Attempts: 1},
		fmt.Sprintf(backoff.TaskKeyFormat, "failed-connector", 1): {Attempts: 1},
	}
	if !reflect.DeepEqual(filter.BackoffStatuses, expected) {
		t.Fatalf("backoff statuses = %#v, want %#v", filter.BackoffStatuses, expected)
	}
}

func TestPollRemediationAndBackoffLifecycle(t *testing.T) {
	const failedStatuses = `{
		"connector-a": {"status": {"name": "connector-a", "connector": {"state": "FAILED"}, "tasks": [], "type": "source"}},
		"connector-b": {"status": {"name": "connector-b", "connector": {"state": "RUNNING"}, "tasks": [{"id": 2, "state": "FAILED"}], "type": "sink"}}
	}`
	const healthyStatuses = `{
		"connector-a": {"status": {"name": "connector-a", "connector": {"state": "RUNNING"}, "tasks": [], "type": "source"}},
		"connector-b": {"status": {"name": "connector-b", "connector": {"state": "RUNNING"}, "tasks": [{"id": 2, "state": "RUNNING"}], "type": "sink"}}
	}`

	type statusResponse struct {
		code int
		body string
	}
	requests := make(chan string, 16)
	responses := make(chan statusResponse)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RequestURI() == "/connectors?expand=status":
			requests <- "GET"
			select {
			case response := <-responses:
				w.WriteHeader(response.code)
				_, _ = w.Write([]byte(response.body))
			case <-r.Context().Done():
			}
		case r.Method == http.MethodPost:
			requests <- r.URL.RequestURI()
			if r.URL.Path == "/connectors/connector-a/restart" {
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Error("Poll did not stop after cancellation")
		}
		server.Close()
	})

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}

	pollConfig := config.Configuration{
		ConnectConfig: config.ConnectAPIConfiguration{
			Host:  serverURL.Hostname(),
			Port:  serverURL.Port(),
			HTTPS: serverURL.Scheme == "https",
		},
		CommunicationConfig: config.CommunicationConfiguration{RequestTimeout: time.Second},
		PollingBehavior: config.PollingBehavior{
			Interval:           10 * time.Millisecond,
			RestartFailedTasks: true,
			Backoff: config.BackoffConfiguration{
				Enabled:   true,
				BaseDelay: time.Hour,
				MaxDelay:  time.Hour,
			},
		},
	}
	go func() {
		Poll(ctx, pollConfig)
		close(finished)
	}()

	readRequest := func() string {
		t.Helper()
		select {
		case request := <-requests:
			return request
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for a Connect API request")
			return ""
		}
	}
	if request := readRequest(); request != "GET" {
		t.Fatalf("first request = %q, want GET", request)
	}

	checkCycle := func(name string, response statusResponse, expected []string) {
		t.Helper()
		select {
		case responses <- response:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: timed out sending status response", name)
		}

		var got []string
		for {
			request := readRequest()
			if request == "GET" {
				break // The next status request means this poll cycle has finished.
			}
			got = append(got, request)
		}
		sort.Strings(got)
		sort.Strings(expected)
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("%s: restart requests = %v, want %v", name, got, expected)
		}
	}

	restarts := []string{
		"/connectors/connector-a/restart?includeTasks=true&onlyFailed=true",
		"/connectors/connector-b/tasks/2/restart",
	}
	checkCycle("initial failure", statusResponse{http.StatusOK, failedStatuses}, restarts)
	checkCycle("failed statuses remain in backoff", statusResponse{http.StatusOK, failedStatuses}, nil)
	checkCycle("healthy statuses clear backoff", statusResponse{http.StatusOK, healthyStatuses}, nil)
	checkCycle("new failures are restarted", statusResponse{http.StatusOK, failedStatuses}, restarts)
	checkCycle("status retrieval error does not reset backoff", statusResponse{http.StatusInternalServerError, ""}, nil)
	checkCycle("failures after retrieval error remain in backoff", statusResponse{http.StatusOK, failedStatuses}, nil)
}
