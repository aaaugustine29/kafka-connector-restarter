package poll

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/config"
	"entropicworks.com/kafka-connector-restarter/internal/poll/actions"
	"entropicworks.com/kafka-connector-restarter/internal/poll/backoff"
	"entropicworks.com/kafka-connector-restarter/internal/poll/status"
	"entropicworks.com/kafka-connector-restarter/internal/poll/utils/requests"
)

func Poll(ctx context.Context, configuration config.Configuration) {
	var connectHTTPClient = &http.Client{
		Timeout: configuration.CommunicationConfig.RequestTimeout,
	}
	connect := requests.NewConnectAPI(connectHTTPClient, configuration.ConnectConfig)
	backoffFilter := backoff.BackoffFilter{
		BackoffConfig:   configuration.PollingBehavior.Backoff,
		BackoffStatuses: map[string]backoff.BackoffStatus{},
	}

	ticker := time.NewTicker(configuration.PollingBehavior.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("polling stopped")
			return
		case <-ticker.C:
			connectorStatuses, err := status.FindConnectorStatuses(ctx, connect)
			if err != nil {
				slog.Error("poll cycle failed while retrieving connector statuses", "error", err)
				continue
			}

			slog.Debug("connector statuses retrieved", "count", len(connectorStatuses))
			resetBackoffsForHealthyStatuses(&backoffFilter, connectorStatuses)
			connectorRemediationActions := actions.GenerateActionsFromStatuses(connectorStatuses, configuration.PollingBehavior.RestartFailedTasks)
			if backoffFilter.BackoffConfig.Enabled {
				connectorRemediationActions = FilterByBackoffs(backoffFilter, connectorRemediationActions)
			}
			for _, remediationAction := range connectorRemediationActions {
				result, err := actions.TakeAction(ctx, remediationAction, connect)
				if err != nil {
					slog.Error("remediation action failed", "action", remediationAction.Kind, "connector", remediationAction.ConnectorName, "task_id", remediationAction.TaskID, "error", err)
				}
				if result.RequestMade {
					switch remediationAction.Kind {
					case actions.RestartConnector:
						backoffFilter.UpdateBackoffStatus(result.AttemptedAt, remediationAction.ConnectorName, nil)
					case actions.RestartTask:
						backoffFilter.UpdateBackoffStatus(result.AttemptedAt, remediationAction.ConnectorName, &remediationAction.TaskID)
					default:
						slog.Error("unsupported remediation action", "action", remediationAction.Kind, "connector", remediationAction.ConnectorName, "task_id", remediationAction.TaskID)
					}
				}
			}
		}
	}
}

func resetBackoffsForHealthyStatuses(backoffFilter *backoff.BackoffFilter, connectorStatuses map[string]status.ConnectorStatus) {
	for _, connectorStatus := range connectorStatuses {
		if strings.EqualFold(connectorStatus.Connector.State, "RUNNING") {
			backoffFilter.ResetBackoffStatus(connectorStatus.Name, nil)
		}

		for _, taskStatus := range connectorStatus.Tasks {
			if strings.EqualFold(taskStatus.State, "RUNNING") {
				backoffFilter.ResetBackoffStatus(connectorStatus.Name, &taskStatus.ID)
			}
		}
	}
}

func FilterByBackoffs(backoffFilter backoff.BackoffFilter, originalActions []actions.RemediationAction) []actions.RemediationAction {
	var filteredActions []actions.RemediationAction
	for _, action := range originalActions {
		switch action.Kind {
		case actions.RestartConnector:
			if !backoffFilter.IsInBackoffWindow(action.ConnectorName, nil) {
				filteredActions = append(filteredActions, action)
			} else {
				slog.Debug("connector restart skipped because it is in the backoff window", "connector", action.ConnectorName)
			}

		case actions.RestartTask:
			if !backoffFilter.IsInBackoffWindow(action.ConnectorName, &action.TaskID) {
				filteredActions = append(filteredActions, action)
			} else {
				slog.Debug("task restart skipped because it is in the backoff window", "connector", action.ConnectorName, "task_id", action.TaskID)
			}
		}
	}
	return filteredActions
}
