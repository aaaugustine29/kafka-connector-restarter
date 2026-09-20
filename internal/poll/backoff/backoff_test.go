package backoff

import (
	"testing"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
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

func TestGetBackoffStatus(t *testing.T) {
	taskID := 3
	connectorStatus := BackoffStatus{Attempts: 1}
	taskStatus := BackoffStatus{Attempts: 2}
	filter := BackoffFilter{
		BackoffStatuses: map[string]BackoffStatus{
			getKey("source-connector", nil):   connectorStatus,
			getKey("sink-connector", &taskID): taskStatus,
		},
	}

	if got := filter.GetBackoffStatus("source-connector", nil); got != connectorStatus {
		t.Fatalf("GetBackoffStatus() = %#v, want %#v", got, connectorStatus)
	}
	if got := filter.GetBackoffStatus("sink-connector", &taskID); got != taskStatus {
		t.Fatalf("GetBackoffStatus() = %#v, want %#v", got, taskStatus)
	}
}

func TestIsInBackoffWindow(t *testing.T) {
	backoffConfig := environment.BackoffConfiguration{
		BaseDelay: time.Hour,
	}

	tests := []struct {
		name     string
		isTask   bool
		status   BackoffStatus
		expected bool
	}{
		{
			name:     "no prior restart permits an action",
			expected: false,
		},
		{
			name:     "recent connector restart remains in the backoff window",
			status:   BackoffStatus{LastAttemptTime: time.Now().Add(-30 * time.Minute)},
			expected: true,
		},
		{
			name:     "expired connector backoff window permits an action",
			status:   BackoffStatus{LastAttemptTime: time.Now().Add(-2 * time.Hour)},
			expected: false,
		},
		{
			name:     "recent task restart remains in the backoff window",
			isTask:   true,
			status:   BackoffStatus{LastAttemptTime: time.Now().Add(-30 * time.Minute)},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var taskID *int
			if test.isTask {
				id := 1
				taskID = &id
			}

			filter := BackoffFilter{
				BackoffConfig: backoffConfig,
				BackoffStatuses: map[string]BackoffStatus{
					getKey("connector", taskID): test.status,
				},
			}
			if got := filter.IsInBackoffWindow("connector", taskID); got != test.expected {
				t.Fatalf("IsInBackoffWindow() = %t, want %t", got, test.expected)
			}
		})
	}
}

func TestGetKey(t *testing.T) {
	taskID := 3

	if got := getKey("source-connector", nil); got != "source-connector" {
		t.Fatalf("getKey() = %q, want source-connector", got)
	}
	if got := getKey("sink-connector", &taskID); got != "ConnectorName-sink-connector_TaskID-3" {
		t.Fatalf("getKey() = %q, want ConnectorName-sink-connector_TaskID-3", got)
	}
}
