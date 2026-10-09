package poll

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/config"
	"entropicworks.com/kafka-connector-restarter/internal/connectcluster"
	"entropicworks.com/kafka-connector-restarter/internal/poll/actions"
	"entropicworks.com/kafka-connector-restarter/internal/poll/backoff"
	"entropicworks.com/kafka-connector-restarter/internal/poll/status"
	"entropicworks.com/kafka-connector-restarter/internal/poll/utils/requests"
)

// ConnectClusterPoller owns the client and backoff state for one cluster.
// Its mutable state is used only by its polling goroutine.
type ConnectClusterPoller struct {
	name                           string
	connectHTTPClient              *http.Client
	backoffFilter                  backoff.BackoffFilter
	connectClusterAPIConfiguration connectcluster.ConnectClusterAPIConfiguration
}

func NewConnectClusterPoller(name string, configuration connectcluster.ConnectClusterAPIConfiguration) *ConnectClusterPoller {
	return &ConnectClusterPoller{name: name, connectClusterAPIConfiguration: configuration}
}

func (poller *ConnectClusterPoller) Poll(ctx context.Context, configManager *config.Manager) {
	configuration, updateChannel := configManager.ConfigurationSnapshot()
	poller.connectHTTPClient = &http.Client{
		Timeout: configuration.CommunicationConfig.RequestTimeout.Duration(),
	}
	connect := requests.NewConnectAPI(poller.connectHTTPClient, poller.connectClusterAPIConfiguration)
	poller.backoffFilter = backoff.BackoffFilter{
		BackoffConfig:   configuration.PollingBehavior.Backoff,
		BackoffStatuses: map[string]backoff.BackoffStatus{},
	}
	logger := slog.With("connect_cluster", poller.name)

	ticker := time.NewTicker(configuration.PollingBehavior.Interval.Duration())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("polling stopped")
			return
		case <-updateChannel:
			var newConfiguration config.ApplicationConfiguration
			newConfiguration, updateChannel = configManager.ConfigurationSnapshot()
			if newConfiguration.CommunicationConfig != configuration.CommunicationConfig {
				poller.connectHTTPClient = &http.Client{
					Timeout: newConfiguration.CommunicationConfig.RequestTimeout.Duration(),
				}
				connect = requests.NewConnectAPI(poller.connectHTTPClient, poller.connectClusterAPIConfiguration)
			}
			if newConfiguration.PollingBehavior.Backoff != configuration.PollingBehavior.Backoff {
				poller.backoffFilter.BackoffConfig = newConfiguration.PollingBehavior.Backoff
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
				logger.Error("poll cycle failed while retrieving connector statuses", "error", err)
				continue
			}

			logger.Debug("connector statuses retrieved", "count", len(connectorStatuses))
			resetBackoffsForHealthyStatuses(&poller.backoffFilter, connectorStatuses)
			connectorRemediationActions := actions.GenerateActionsFromStatuses(connectorStatuses, configuration.PollingBehavior.RestartFailedTasks)
			if poller.backoffFilter.BackoffConfig.Enabled {
				connectorRemediationActions = FilterByBackoffs(poller.backoffFilter, connectorRemediationActions)
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
						poller.backoffFilter.UpdateBackoffStatus(result.AttemptedAt, remediationAction.ConnectorName, nil)
					case actions.RestartTask:
						poller.backoffFilter.UpdateBackoffStatus(result.AttemptedAt, remediationAction.ConnectorName, &remediationAction.TaskID)
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
