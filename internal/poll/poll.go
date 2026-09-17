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
	backoffFilterer := backoff.BackoffFilterer{
		BackoffConfig:            config.PollingBehavior.Backoff,
		ConnectorBackoffStatuses: map[string]backoff.ConnectorBackoffStatus{},
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
			if backoffFilterer.BackoffConfig.Enabled {
				connectorRemediationActions = backoffFilterer.FilterByBackoffs(connectorRemediationActions)
			}
			for _, remediationAction := range connectorRemediationActions {
				if err := actions.TakeAction(ctx, remediationAction, connect); err != nil {
					// log.Printf("Error taking remediation action %s: %v", actionURL, err)
				}
			}
		}
	}
}
