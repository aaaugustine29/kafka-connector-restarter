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

func TestIsConnectorInBackoffWindow(t *testing.T) {
	backoffConfig := environment.BackoffConfiguration{
		Enabled:     true,
		BaseDelay:   time.Hour,
		Exponential: false,
	}

	tests := []struct {
		name     string
		config   environment.BackoffConfiguration
		status   ConnectorBackoffStatus
		expected bool
	}{
		{
			name:   "disabled backoff permits a restart",
			config: environment.BackoffConfiguration{Enabled: false, BaseDelay: time.Hour},
			status: ConnectorBackoffStatus{
				LastConnectorRestartAttemptTime: time.Now(),
			},
			expected: false,
		},
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
			if got := IsConnectorInBackoffWindow(test.config, test.status); got != test.expected {
				t.Fatalf("IsConnectorInBackoffWindow() = %t, want %t", got, test.expected)
			}
		})
	}
}

func TestIsTaskInBackoffWindow(t *testing.T) {
	backoffConfig := environment.BackoffConfiguration{
		Enabled:     true,
		BaseDelay:   time.Hour,
		Exponential: false,
	}

	tests := []struct {
		name     string
		config   environment.BackoffConfiguration
		status   TaskBackoffStatus
		expected bool
	}{
		{
			name:   "disabled backoff permits a task restart",
			config: environment.BackoffConfiguration{Enabled: false, BaseDelay: time.Hour},
			status: TaskBackoffStatus{
				LastTaskRestartAttemptTime: time.Now(),
			},
			expected: false,
		},
		{
			name:     "no prior task restart permits a restart",
			config:   backoffConfig,
			status:   TaskBackoffStatus{},
			expected: false,
		},
		{
			name:   "recent task restart remains in the backoff window",
			config: backoffConfig,
			status: TaskBackoffStatus{
				LastTaskRestartAttemptTime: time.Now().Add(-30 * time.Minute),
			},
			expected: true,
		},
		{
			name:   "expired task backoff window permits a restart",
			config: backoffConfig,
			status: TaskBackoffStatus{
				LastTaskRestartAttemptTime: time.Now().Add(-2 * time.Hour),
			},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsTaskInBackoffWindow(test.config, test.status); got != test.expected {
				t.Fatalf("isTaskInBackoffWindow() = %t, want %t", got, test.expected)
			}
		})
	}
}
