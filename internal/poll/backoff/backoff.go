package backoff

import (
	"fmt"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
)

const (
	ConnectorKeyFormat string = "%s"
	TaskKeyFormat      string = "ConnectorName-%s_TaskID-%d"
)

type BackoffStatus struct {
	LastAttemptTime time.Time
	Attempts        int
}

type BackoffFilter struct {
	BackoffConfig   environment.BackoffConfiguration
	BackoffStatuses map[string]BackoffStatus
}

func (backoffFilter *BackoffFilter) UpdateBackoffStatus(attemptTime time.Time, connectorName string, taskID *int) {
	key := getKey(connectorName, taskID)
	status := backoffFilter.BackoffStatuses[key]
	status.LastAttemptTime = attemptTime
	status.Attempts++
	backoffFilter.BackoffStatuses[key] = status
}

func (backoffFilter *BackoffFilter) IsInBackoffWindow(connectorName string, taskID *int) bool {
	backoffStatus := backoffFilter.getBackoffStatus(connectorName, taskID)

	nextBackoffTime := determineNextActionTime(
		backoffStatus.LastAttemptTime,
		backoffFilter.BackoffConfig.BaseDelay,
		backoffFilter.BackoffConfig.MaxDelay,
		backoffStatus.Attempts,
		backoffFilter.BackoffConfig.Exponential,
	)
	if backoffStatus.LastAttemptTime.IsZero() {
		return false
	} else if time.Now().Before(nextBackoffTime) {
		return true
	} else {
		return false
	}
}

func (backoffFilter *BackoffFilter) getBackoffStatus(
	connectorName string, taskID *int,
) BackoffStatus {
	return backoffFilter.BackoffStatuses[getKey(connectorName, taskID)]
}

func determineNextActionTime(
	lastAttemptTime time.Time,
	baseDelay time.Duration,
	maxDelay time.Duration,
	attempts int,
	exponential bool,
) time.Time {
	delay := baseDelay

	if maxDelay > 0 && delay > maxDelay {
		delay = maxDelay
	}

	if exponential {
		for attempt := 1; attempt < attempts && delay < maxDelay; attempt++ {
			if delay > maxDelay/2 {
				delay = maxDelay
				break
			}
			delay *= 2
		}
	}

	return lastAttemptTime.Add(delay)
}

func getKey(connectorName string, taskID *int) string {
	if taskID == nil {
		return fmt.Sprintf(ConnectorKeyFormat, connectorName)
	} else {
		return fmt.Sprintf(TaskKeyFormat, connectorName, *taskID)
	}
}
