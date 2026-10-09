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

func Poll(ctx context.Context, configManager *config.Manager) {
	configuration, updateChannel := configManager.ConfigurationSnapshot()
	connectHTTPClient := &http.Client{
		Timeout: configuration.CommunicationConfig.RequestTimeout.Duration(),
	}
	connect := requests.NewConnectAPI(connectHTTPClient, configuration.ConnectConfig)
	backoffFilter := backoff.BackoffFilter{
		BackoffConfig:   configuration.PollingBehavior.Backoff,
		BackoffStatuses: map[string]backoff.BackoffStatus{},
	}

	ticker := time.NewTicker(configuration.PollingBehavior.Interval.Duration())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("polling stopped")
			return
		case <-updateChannel:
			var newConfiguration config.Configuration
			newConfiguration, updateChannel = configManager.ConfigurationSnapshot()
			if newConfiguration.CommunicationConfig != configuration.CommunicationConfig ||
				newConfiguration.ConnectConfig != configuration.ConnectConfig {
				if newConfiguration.CommunicationConfig != configuration.CommunicationConfig {
					connectHTTPClient = &http.Client{
						Timeout: newConfiguration.CommunicationConfig.RequestTimeout.Duration(),
					}
				}
				if newConfiguration.ConnectConfig.Host != configuration.ConnectConfig.Host ||
					newConfiguration.ConnectConfig.Port != configuration.ConnectConfig.Port ||
					newConfiguration.ConnectConfig.HTTPS != configuration.ConnectConfig.HTTPS {
					backoffFilter.BackoffStatuses = map[string]backoff.BackoffStatus{}
				}
				connect = requests.NewConnectAPI(connectHTTPClient, newConfiguration.ConnectConfig)
			}
			if newConfiguration.PollingBehavior.Backoff != configuration.PollingBehavior.Backoff {
				backoffFilter.BackoffConfig = newConfiguration.PollingBehavior.Backoff
			}
			if newConfiguration.PollingBehavior.Interval != configuration.PollingBehavior.Interval {
				ticker.Reset(newConfiguration.PollingBehavior.Interval.Duration())
			}

			configuration = newConfiguration

		case <-ticker.C:
			connectorStatuses, err := status.FindConnectorStatuses(ctx, connect)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
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
				if ctx.Err() != nil {
					return
				}
				result, err := actions.TakeAction(ctx, remediationAction, connect)
				if err != nil && ctx.Err() == nil {
					remediationAction.Logger().Error("remediation request failed", "request_made", result.RequestMade, "status_code", result.StatusCode, "error", err)
				}
				if result.RequestMade {
					switch remediationAction.Kind {
					case actions.RestartConnector:
						backoffFilter.UpdateBackoffStatus(result.AttemptedAt, remediationAction.ConnectorName, nil)
					case actions.RestartTask:
						backoffFilter.UpdateBackoffStatus(result.AttemptedAt, remediationAction.ConnectorName, &remediationAction.TaskID)
					default:
						remediationAction.Logger().Error("unsupported remediation action")
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
		default:
			action.Logger().Error("cannot filter unsupported remediation action")
		}
	}
	return filteredActions
}
