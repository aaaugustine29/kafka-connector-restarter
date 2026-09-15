package backoff

import (
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
)

type TaskBackoffHistory struct {
	LastTaskRestartAttemptTime time.Time
	ConnectorRestartAttempts   int
}

type ConnectorBackoffHistory struct {
	LastConnectorRestartAttemptTime time.Time
	ConnectorRestartAttempts        int
	LastTaskRestartAttemptTime      map[int]time.Time
}

func IsConnectorOutsideBackoffWindow(
	backoffConfig environment.BackoffConfiguration,
	backoffHistory ConnectorBackoffHistory,
) bool {
	nextBackoffTime := determineNextActionTime(
		backoffHistory.LastConnectorRestartAttemptTime,
		backoffConfig.BaseDelay,
		backoffHistory.ConnectorRestartAttempts,
		backoffConfig.Exponential,
	)
	if !backoffConfig.Enabled {
		return false
	} else {
		if backoffHistory.LastConnectorRestartAttemptTime.IsZero() {
			return false
		} else if nextBackoffTime.Before(time.Now()) {
			return false
		} else {
			return true
		}
	}
}

func isTaskOutsideBackoffWindow(
	backoffConfig environment.BackoffConfiguration,
	backoffHistory TaskBackoffHistory,
) bool {
	nextBackoffTime := determineNextActionTime(
		backoffHistory.LastTaskRestartAttemptTime,
		backoffConfig.BaseDelay,
		backoffHistory.ConnectorRestartAttempts,
		backoffConfig.Exponential,
	)
	if !backoffConfig.Enabled {
		return false
	} else {
		if backoffHistory.LastTaskRestartAttemptTime.IsZero() {
			return false
		} else if nextBackoffTime.Before(time.Now()) {
			return false
		} else {
			return true
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
		return nextActionTime.Add(baseDelay * time.Duration(2<<attempts))
	} else {
		return nextActionTime.Add(baseDelay)
	}
}
