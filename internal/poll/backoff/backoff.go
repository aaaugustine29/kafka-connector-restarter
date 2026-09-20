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

type BackoffFilter struct {
	BackoffConfig            environment.BackoffConfiguration
	ConnectorBackoffStatuses map[string]ConnectorBackoffStatus
}

func (backoffFilter BackoffFilter) FilterByBackoffs(originalRemediationActions []actions.RemediationAction) []actions.RemediationAction {
	var filteredActions []actions.RemediationAction
	for _, originalRemediationAction := range originalRemediationActions {
		connectorBackoffStatus := backoffFilter.ConnectorBackoffStatuses[originalRemediationAction.ConnectorName]
		if originalRemediationAction.Kind == actions.RestartConnector {
			if !isConnectorInBackoffWindow(backoffFilter.BackoffConfig, connectorBackoffStatus) {
				filteredActions = append(filteredActions, originalRemediationAction)
			}
		} else if originalRemediationAction.Kind == actions.RestartTask {
			taskBackoffStatus := backoffFilter.ConnectorBackoffStatuses[originalRemediationAction.ConnectorName].TaskBackoffStatuses[originalRemediationAction.TaskID]
			if !isTaskInBackoffWindow(backoffFilter.BackoffConfig, taskBackoffStatus) {
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
