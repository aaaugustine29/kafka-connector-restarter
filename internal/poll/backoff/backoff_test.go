package backoff

import (
	"testing"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
)

func TestDetermineNextActionTime(t *testing.T) {
	lastAttemptTime := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)
	baseDelay := time.Second
	maxDelay := 5 * time.Second

	tests := []struct {
		name        string
		attempts    int
		exponential bool
		expected    time.Time
	}{
		{
			name:     "linear backoff uses the base delay",
			attempts: 3,
			expected: lastAttemptTime.Add(baseDelay),
		},
		{
			name:        "first recorded attempt uses the base delay",
			attempts:    1,
			exponential: true,
			expected:    lastAttemptTime.Add(baseDelay),
		},
		{
			name:        "second recorded attempt doubles the base delay",
			attempts:    2,
			exponential: true,
			expected:    lastAttemptTime.Add(2 * baseDelay),
		},
		{
			name:        "third recorded attempt quadruples the base delay",
			attempts:    3,
			exponential: true,
			expected:    lastAttemptTime.Add(4 * baseDelay),
		},
		{
			name:        "exponential backoff does not exceed the maximum delay",
			attempts:    4,
			exponential: true,
			expected:    lastAttemptTime.Add(maxDelay),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := determineNextActionTime(lastAttemptTime, baseDelay, maxDelay, test.attempts, test.exponential); !got.Equal(test.expected) {
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

	if got := filter.getBackoffStatus("source-connector", nil); got != connectorStatus {
		t.Fatalf("getBackoffStatus() = %#v, want %#v", got, connectorStatus)
	}
	if got := filter.getBackoffStatus("sink-connector", &taskID); got != taskStatus {
		t.Fatalf("getBackoffStatus() = %#v, want %#v", got, taskStatus)
	}
}

func TestUpdateBackoffStatus(t *testing.T) {
	firstAttempt := time.Date(2026, time.September, 20, 12, 0, 0, 0, time.UTC)
	secondAttempt := firstAttempt.Add(time.Minute)
	taskID := 3
	filter := BackoffFilter{
		BackoffStatuses: map[string]BackoffStatus{},
	}

	filter.UpdateBackoffStatus(firstAttempt, "source-connector", nil)
	filter.UpdateBackoffStatus(secondAttempt, "source-connector", nil)
	filter.UpdateBackoffStatus(firstAttempt, "sink-connector", &taskID)

	if got, want := filter.getBackoffStatus("source-connector", nil), (BackoffStatus{
		LastAttemptTime: secondAttempt,
		Attempts:        2,
	}); got != want {
		t.Fatalf("connector backoff status = %#v, want %#v", got, want)
	}

	if got, want := filter.getBackoffStatus("sink-connector", &taskID), (BackoffStatus{
		LastAttemptTime: firstAttempt,
		Attempts:        1,
	}); got != want {
		t.Fatalf("task backoff status = %#v, want %#v", got, want)
	}
}

func TestIsInBackoffWindow(t *testing.T) {
	backoffConfig := environment.BackoffConfiguration{
		BaseDelay: time.Hour,
		MaxDelay:  time.Hour,
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
