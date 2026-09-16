package backoff

import (
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
)

type TaskBackoffStatus struct {
	LastTaskRestartAttemptTime time.Time
	TaskRestartAttempts        int
}

type ConnectorBackoffStatus struct {
	LastConnectorRestartAttemptTime time.Time
	ConnectorRestartAttempts        int
	LastTaskRestartAttemptTime      map[int]time.Time
}

func IsConnectorInBackoffWindow(
	backoffConfig environment.BackoffConfiguration,
	backoffStatus ConnectorBackoffStatus,
) bool {
	nextBackoffTime := determineNextActionTime(
		backoffStatus.LastConnectorRestartAttemptTime,
		backoffConfig.BaseDelay,
		backoffStatus.ConnectorRestartAttempts,
		backoffConfig.Exponential,
	)
	if !backoffConfig.Enabled {
		return false
	} else {
		if backoffStatus.LastConnectorRestartAttemptTime.IsZero() {
			return false
		} else if time.Now().Before(nextBackoffTime) {
			return true
		} else {
			return false
		}
	}
}

func IsTaskInBackoffWindow(
	backoffConfig environment.BackoffConfiguration,
	backoffStatus TaskBackoffStatus,
) bool {
	nextBackoffTime := determineNextActionTime(
		backoffStatus.LastTaskRestartAttemptTime,
		backoffConfig.BaseDelay,
		backoffStatus.TaskRestartAttempts,
		backoffConfig.Exponential,
	)
	if !backoffConfig.Enabled {
		return false
	} else {
		if backoffStatus.LastTaskRestartAttemptTime.IsZero() {
			return false
		} else if time.Now().Before(nextBackoffTime) {
			return true
		} else {
			return false
		}
	}
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
