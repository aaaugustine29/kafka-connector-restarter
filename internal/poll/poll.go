package poll

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
	"entropicworks.com/kafka-connector-restarter/internal/poll/actions"
	"entropicworks.com/kafka-connector-restarter/internal/poll/backoff"
	"entropicworks.com/kafka-connector-restarter/internal/poll/status"
	"entropicworks.com/kafka-connector-restarter/internal/poll/utils/requests"
)

func Poll(ctx context.Context, config environment.Configuration) {
	var connectHTTPClient = &http.Client{
		Timeout: config.CommunicationConfig.RequestTimeout,
	}
	connect := requests.ConnectAPI{
		HTTPClient: connectHTTPClient,
		BaseURL:    config.ConnectConfig.URL,
		Auth:       config.ConnectConfig.AuthConfig,
	}
	backoffFilter := backoff.BackoffFilter{
		BackoffConfig:   config.PollingBehavior.Backoff,
		BackoffStatuses: map[string]backoff.BackoffStatus{},
	}

	ticker := time.NewTicker(config.PollingBehavior.Interval)
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
			connectorRemediationActions := actions.GenerateActionsFromStatuses(connectorStatuses, config.PollingBehavior.RestartFailedTasks)
			if backoffFilter.BackoffConfig.Enabled {
				connectorRemediationActions = FilterByBackoffs(backoffFilter, connectorRemediationActions)
			}
			for _, remediationAction := range connectorRemediationActions {
				if err := actions.TakeAction(ctx, remediationAction, connect); err != nil {
					slog.Error("remediation action failed", "action", remediationAction.Kind, "connector", remediationAction.ConnectorName, "task_id", remediationAction.TaskID, "error", err)
				}
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
