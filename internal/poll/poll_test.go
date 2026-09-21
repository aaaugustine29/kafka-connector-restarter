package poll

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
	"entropicworks.com/kafka-connector-restarter/internal/poll/actions"
	"entropicworks.com/kafka-connector-restarter/internal/poll/backoff"
	"entropicworks.com/kafka-connector-restarter/internal/poll/status"
)

func TestFilterByBackoffs(t *testing.T) {
	recentAttempt := time.Now().Add(-30 * time.Minute)
	config := environment.BackoffConfiguration{BaseDelay: time.Hour}

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
				BackoffConfig:   config,
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
