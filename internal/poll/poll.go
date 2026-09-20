package poll

import (
	"context"
	"log"
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
			return
		case <-ticker.C:
			connectorStatuses, err := status.FindConnectorStatuses(ctx, connect)
			if err != nil {
				log.Printf("Error during poll cycle: %v", err)
				continue
			}
			connectorRemediationActions := actions.GenerateActionsFromStatuses(connectorStatuses, config.PollingBehavior.RestartFailedTasks)
			if backoffFilter.BackoffConfig.Enabled {
				connectorRemediationActions = FilterByBackoffs(backoffFilter, connectorRemediationActions)
			}
			for _, remediationAction := range connectorRemediationActions {
				if err := actions.TakeAction(ctx, remediationAction, connect); err != nil {
					// log.Printf("Error taking remediation action %s: %v", actionURL, err)
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
			}

		case actions.RestartTask:
			if !backoffFilter.IsInBackoffWindow(action.ConnectorName, &action.TaskID) {
				filteredActions = append(filteredActions, action)
			}
		}
	}
	return filteredActions
}
