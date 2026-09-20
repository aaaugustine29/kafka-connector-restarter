package backoff

import (
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
	"entropicworks.com/kafka-connector-restarter/internal/poll/actions"
)

type taskBackoffStatus struct {
	LastTaskRestartAttemptTime time.Time
	TaskRestartAttempts        int
}

type ConnectorBackoffStatus struct {
	LastConnectorRestartAttemptTime time.Time
	ConnectorRestartAttempts        int
	TaskBackoffStatuses             map[int]taskBackoffStatus
}

type BackoffFilterer struct {
	BackoffConfig            environment.BackoffConfiguration
	ConnectorBackoffStatuses map[string]ConnectorBackoffStatus
}

func (backoffFilterer BackoffFilterer) FilterByBackoffs(originalRemediationActions []actions.RemediationAction) []actions.RemediationAction {
	var filteredActions []actions.RemediationAction
	for _, originalRemediationAction := range originalRemediationActions {
		connectorBackoffStatus := backoffFilterer.ConnectorBackoffStatuses[originalRemediationAction.ConnectorName]
		if originalRemediationAction.Restart {
			if !isConnectorInBackoffWindow(backoffFilterer.BackoffConfig, connectorBackoffStatus) {
				filteredActions = append(filteredActions, originalRemediationAction)
			}
		} else {
			var filteredTaskIDsToBeRestarted []int
			for _, originalTaskToBeRestarted := range originalRemediationAction.TaskIDsToBeRestarted {
				taskBackoffStatus := backoffFilterer.ConnectorBackoffStatuses[originalRemediationAction.ConnectorName].TaskBackoffStatuses[originalTaskToBeRestarted]
				if !isTaskInBackoffWindow(backoffFilterer.BackoffConfig, taskBackoffStatus) {
					filteredTaskIDsToBeRestarted = append(filteredTaskIDsToBeRestarted, originalTaskToBeRestarted)
				}
			}
			if len(filteredTaskIDsToBeRestarted) > 0 {
				originalRemediationAction.TaskIDsToBeRestarted = filteredTaskIDsToBeRestarted
				filteredActions = append(filteredActions, originalRemediationAction)
			}
		}
	}
	return filteredActions
}

func isConnectorInBackoffWindow(
	backoffConfig environment.BackoffConfiguration,
	backoffStatus ConnectorBackoffStatus,
) bool {
	nextBackoffTime := determineNextActionTime(
		backoffStatus.LastConnectorRestartAttemptTime,
		backoffConfig.BaseDelay,
		backoffStatus.ConnectorRestartAttempts,
		backoffConfig.Exponential,
	)
	if backoffStatus.LastConnectorRestartAttemptTime.IsZero() {
		return false
	} else if time.Now().Before(nextBackoffTime) {
		return true
	} else {
		return false
	}
}

func isTaskInBackoffWindow(
	backoffConfig environment.BackoffConfiguration,
	backoffStatus taskBackoffStatus,
) bool {
	nextBackoffTime := determineNextActionTime(
		backoffStatus.LastTaskRestartAttemptTime,
		backoffConfig.BaseDelay,
		backoffStatus.TaskRestartAttempts,
		backoffConfig.Exponential,
	)
	if backoffStatus.LastTaskRestartAttemptTime.IsZero() {
		return false
	} else if time.Now().Before(nextBackoffTime) {
		return true
	} else {
		return false
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
