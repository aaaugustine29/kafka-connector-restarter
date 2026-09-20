package backoff

import (
	"reflect"
	"testing"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
	"entropicworks.com/kafka-connector-restarter/internal/poll/actions"
)

func TestDetermineNextActionTime(t *testing.T) {
	lastRestartTime := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)
	baseDelay := time.Second

	tests := []struct {
		name        string
		attempts    int
		exponential bool
		expected    time.Time
	}{
		{
			name:     "linear backoff uses the base delay",
			attempts: 3,
			expected: lastRestartTime.Add(baseDelay),
		},
		{
			name:        "first exponential backoff uses the base delay",
			attempts:    0,
			exponential: true,
			expected:    lastRestartTime.Add(baseDelay),
		},
		{
			name:        "second exponential backoff doubles the base delay",
			attempts:    1,
			exponential: true,
			expected:    lastRestartTime.Add(2 * baseDelay),
		},
		{
			name:        "third exponential backoff quadruples the base delay",
			attempts:    2,
			exponential: true,
			expected:    lastRestartTime.Add(4 * baseDelay),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := determineNextActionTime(lastRestartTime, baseDelay, test.attempts, test.exponential); !got.Equal(test.expected) {
				t.Fatalf("determineNextActionTime() = %v, want %v", got, test.expected)
			}
		})
	}
}

func TestIsConnectorInBackoffWindow(t *testing.T) {
	backoffConfig := environment.BackoffConfiguration{
		BaseDelay: time.Hour,
	}

	tests := []struct {
		name     string
		config   environment.BackoffConfiguration
		status   ConnectorBackoffStatus
		expected bool
	}{
		{
			name:     "no prior restart permits a restart",
			config:   backoffConfig,
			status:   ConnectorBackoffStatus{},
			expected: false,
		},
		{
			name:   "recent restart remains in the backoff window",
			config: backoffConfig,
			status: ConnectorBackoffStatus{
				LastConnectorRestartAttemptTime: time.Now().Add(-30 * time.Minute),
			},
			expected: true,
		},
		{
			name:   "expired backoff window permits a restart",
			config: backoffConfig,
			status: ConnectorBackoffStatus{
				LastConnectorRestartAttemptTime: time.Now().Add(-2 * time.Hour),
			},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isConnectorInBackoffWindow(test.config, test.status); got != test.expected {
				t.Fatalf("isConnectorInBackoffWindow() = %t, want %t", got, test.expected)
			}
		})
	}
}

func TestIsTaskInBackoffWindow(t *testing.T) {
	backoffConfig := environment.BackoffConfiguration{
		BaseDelay: time.Hour,
	}

	tests := []struct {
		name     string
		config   environment.BackoffConfiguration
		status   taskBackoffStatus
		expected bool
	}{
		{
			name:     "no prior task restart permits a restart",
			config:   backoffConfig,
			status:   taskBackoffStatus{},
			expected: false,
		},
		{
			name:   "recent task restart remains in the backoff window",
			config: backoffConfig,
			status: taskBackoffStatus{
				LastTaskRestartAttemptTime: time.Now().Add(-30 * time.Minute),
			},
			expected: true,
		},
		{
			name:   "expired task backoff window permits a restart",
			config: backoffConfig,
			status: taskBackoffStatus{
				LastTaskRestartAttemptTime: time.Now().Add(-2 * time.Hour),
			},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isTaskInBackoffWindow(test.config, test.status); got != test.expected {
				t.Fatalf("isTaskInBackoffWindow() = %t, want %t", got, test.expected)
			}
		})
	}
}

func TestFilterByBackoffs(t *testing.T) {
	config := environment.BackoffConfiguration{
		BaseDelay: time.Hour,
	}
	recentAttempt := time.Now().Add(-30 * time.Minute)

	tests := []struct {
		name     string
		statuses map[string]ConnectorBackoffStatus
		actions  []actions.RemediationAction
		expected []actions.RemediationAction
	}{
		{
			name: "actions without a prior attempt are retained",
			actions: []actions.RemediationAction{
				{ConnectorName: "source-connector", Kind: actions.RestartConnector},
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 1},
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 2},
			},
			expected: []actions.RemediationAction{
				{ConnectorName: "source-connector", Kind: actions.RestartConnector},
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 1},
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 2},
			},
		},
		{
			name: "connector restart in the window is omitted",
			statuses: map[string]ConnectorBackoffStatus{
				"source-connector": {
					LastConnectorRestartAttemptTime: recentAttempt,
				},
			},
			actions: []actions.RemediationAction{
				{ConnectorName: "source-connector", Kind: actions.RestartConnector},
			},
			expected: nil,
		},
		{
			name: "only tasks outside their backoff window are retained",
			statuses: map[string]ConnectorBackoffStatus{
				"sink-connector": {
					TaskBackoffStatuses: map[int]taskBackoffStatus{
						1: {LastTaskRestartAttemptTime: recentAttempt},
					},
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
		{
			name: "action is omitted when all failed tasks are in their backoff windows",
			statuses: map[string]ConnectorBackoffStatus{
				"sink-connector": {
					TaskBackoffStatuses: map[int]taskBackoffStatus{
						1: {LastTaskRestartAttemptTime: recentAttempt},
						2: {LastTaskRestartAttemptTime: recentAttempt},
					},
				},
			},
			actions: []actions.RemediationAction{
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 1},
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 2},
			},
			expected: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filterer := BackoffFilter{
				BackoffConfig:            config,
				ConnectorBackoffStatuses: test.statuses,
			}

			if got := filterer.FilterByBackoffs(test.actions); !reflect.DeepEqual(got, test.expected) {
				t.Fatalf("FilterByBackoffs() = %#v, want %#v", got, test.expected)
			}
		})
	}
}
