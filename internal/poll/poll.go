package poll

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"time"

	"entropicworks.com/kafka-connect-healer/internal/config"
	"entropicworks.com/kafka-connect-healer/internal/connectcluster"
	"entropicworks.com/kafka-connect-healer/internal/poll/actions"
	"entropicworks.com/kafka-connect-healer/internal/poll/backoff"
	"entropicworks.com/kafka-connect-healer/internal/poll/requests"
	"entropicworks.com/kafka-connect-healer/internal/poll/status"
)

// ConnectClusterPoller owns the client and backoff state for one cluster.
// Its mutable state is used only by its polling goroutine.
type ConnectClusterPoller struct {
	name                 string
	connectClient        requests.ConnectAPI
	backoffFilter        backoff.Filter
	clusterConfiguration connectcluster.Configuration
}

func NewConnectClusterPoller(name string, configuration connectcluster.Configuration) *ConnectClusterPoller {
	return &ConnectClusterPoller{name: name, clusterConfiguration: configuration}
}

func (poller *ConnectClusterPoller) Poll(ctx context.Context, configurationManager *config.Manager) {
	configuration, configurationUpdateChannel := configurationManager.ConfigurationSnapshot()
	poller.connectClient = requests.NewConnectAPI(poller.clusterConfiguration, configuration.CommunicationConfig.RequestTimeout.Duration())
	poller.backoffFilter = backoff.Filter{
		BackoffConfig:   configuration.PollingBehavior.Backoff,
		BackoffStatuses: map[string]backoff.Status{},
	}
	logger := slog.With("connect_cluster", poller.name, "endpoint", poller.connectClient.BaseURL)
	logPollingSettings(logger, "polling started", configuration)
	defer logger.Info("polling stopped")

	ticker := time.NewTicker(configuration.PollingBehavior.Interval.Duration())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-configurationUpdateChannel:
			var newConfiguration config.ApplicationConfiguration
			newConfiguration, configurationUpdateChannel = configurationManager.ConfigurationSnapshot()
			if newConfiguration.CommunicationConfig != configuration.CommunicationConfig {
				// Requests run synchronously in this goroutine; update only between cycles.
				poller.connectClient.HTTPClient.Timeout = newConfiguration.CommunicationConfig.RequestTimeout.Duration()
			}
			if newConfiguration.PollingBehavior.Backoff != configuration.PollingBehavior.Backoff {
				poller.backoffFilter.BackoffConfig = newConfiguration.PollingBehavior.Backoff
			}
			if newConfiguration.PollingBehavior.Interval != configuration.PollingBehavior.Interval {
				ticker.Reset(newConfiguration.PollingBehavior.Interval.Duration())
			}

			if newConfiguration.PollingBehavior != configuration.PollingBehavior ||
				newConfiguration.CommunicationConfig != configuration.CommunicationConfig {
				logPollingSettings(logger, "polling settings updated", newConfiguration)
			}
			configuration = newConfiguration

		case <-ticker.C:
			cycleStarted := time.Now()
			connectorStatuses, err := status.FindConnectorStatuses(ctx, poller.connectClient)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Error("poll cycle failed while retrieving connector statuses", "error", err, "duration", time.Since(cycleStarted))
				continue
			}

			logger.Debug("connector statuses retrieved", "count", len(connectorStatuses))
			pruneBackoffsForMissingStatuses(&poller.backoffFilter, connectorStatuses)
			resetBackoffsForHealthyStatuses(&poller.backoffFilter, connectorStatuses)
			connectorRemediationActions := actions.GenerateActionsFromStatuses(connectorStatuses, configuration.PollingBehavior.RestartFailedTasks, logger)
			if poller.backoffFilter.BackoffConfig.Enabled {
				connectorRemediationActions = FilterByBackoffs(poller.backoffFilter, connectorRemediationActions, logger)
			}
			restartAttempts := 0
			for _, remediationAction := range connectorRemediationActions {
				if ctx.Err() != nil {
					return
				}
				result, err := actions.TakeAction(ctx, remediationAction, poller.connectClient, logger)
				if err != nil && ctx.Err() == nil {
					remediationAction.Logger(logger).Error("remediation request failed", "request_attempted", result.RequestAttempted, "status_code", result.StatusCode, "error", err)
				}
				if result.RequestAttempted {
					restartAttempts++
					switch remediationAction.Kind {
					case actions.RestartConnector:
						poller.backoffFilter.UpdateBackoffStatus(result.AttemptedAt, remediationAction.ConnectorName, nil)
					case actions.RestartTask:
						poller.backoffFilter.UpdateBackoffStatus(result.AttemptedAt, remediationAction.ConnectorName, &remediationAction.TaskID)
					default:
						remediationAction.Logger(logger).Error("unsupported remediation action")
					}
				}
			}
			logger.Debug("poll cycle completed", "connector_count", len(connectorStatuses), "restart_attempts", restartAttempts, "duration", time.Since(cycleStarted))
		}
	}
}

func logPollingSettings(logger *slog.Logger, message string, configuration config.ApplicationConfiguration) {
	logger.Info(message,
		"poll_interval", configuration.PollingBehavior.Interval.String(),
		"request_timeout", configuration.CommunicationConfig.RequestTimeout.String(),
		"restart_failed_tasks", configuration.PollingBehavior.RestartFailedTasks,
		"backoff_enabled", configuration.PollingBehavior.Backoff.Enabled,
		"backoff_base_delay", configuration.PollingBehavior.Backoff.BaseDelay.String(),
		"backoff_max_delay", configuration.PollingBehavior.Backoff.MaxDelay.String(),
		"backoff_exponential", configuration.PollingBehavior.Backoff.Exponential,
	)
}

// Call only after successfully retrieving a complete status snapshot.
func pruneBackoffsForMissingStatuses(backoffFilter *backoff.Filter, connectorStatuses map[string]status.ConnectorStatus) {
	if len(backoffFilter.BackoffStatuses) == 0 {
		return
	}
	missingStatuses := maps.Clone(backoffFilter.BackoffStatuses)
	for _, connectorStatus := range connectorStatuses {
		delete(missingStatuses, connectorStatus.Name)
		for _, taskStatus := range connectorStatus.Tasks {
			delete(missingStatuses, fmt.Sprintf(backoff.TaskKeyFormat, connectorStatus.Name, taskStatus.ID))
		}
	}
	for key := range missingStatuses {
		delete(backoffFilter.BackoffStatuses, key)
	}
}

func resetBackoffsForHealthyStatuses(backoffFilter *backoff.Filter, connectorStatuses map[string]status.ConnectorStatus) {
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

func FilterByBackoffs(backoffFilter backoff.Filter, originalActions []actions.RemediationAction, logger *slog.Logger) []actions.RemediationAction {
	var filteredActions []actions.RemediationAction
	for _, action := range originalActions {
		switch action.Kind {
		case actions.RestartConnector:
			if !backoffFilter.IsInBackoffWindow(action.ConnectorName, nil) {
				filteredActions = append(filteredActions, action)
			} else {
				action.Logger(logger).Debug("connector restart skipped because it is in the backoff window")
			}

		case actions.RestartTask:
			if !backoffFilter.IsInBackoffWindow(action.ConnectorName, &action.TaskID) {
				filteredActions = append(filteredActions, action)
			} else {
				action.Logger(logger).Debug("task restart skipped because it is in the backoff window")
			}
		default:
			action.Logger(logger).Error("cannot filter unsupported remediation action")
		}
	}
	return filteredActions
}
