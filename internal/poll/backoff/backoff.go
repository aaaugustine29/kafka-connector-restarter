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

func (backoffFilter *BackoffFilter) IsInBackoffWindow(connectorName string, taskID *int) bool {
	backoffStatus := backoffFilter.GetBackoffStatus(connectorName, taskID)

	nextBackoffTime := determineNextActionTime(
		backoffStatus.LastAttemptTime,
		backoffFilter.BackoffConfig.BaseDelay,
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

func (backoffFilter *BackoffFilter) GetBackoffStatus(
	connectorName string, taskID *int,
) BackoffStatus {
	return backoffFilter.BackoffStatuses[getKey(connectorName, taskID)]
}

func determineNextActionTime(
	lastRestartTime time.Time,
	baseDelay time.Duration,
	attempts int,
	exponential bool,
) time.Time {
	nextActionTime := lastRestartTime
	if exponential {
		return nextActionTime.Add(baseDelay * time.Duration(1<<(attempts)))
	} else {
		return nextActionTime.Add(baseDelay)
	}
}

func getKey(connectorName string, taskID *int) string {
	if taskID == nil {
		return fmt.Sprintf(ConnectorKeyFormat, connectorName)
	} else {
		return fmt.Sprintf(TaskKeyFormat, connectorName, *taskID)
	}
}
