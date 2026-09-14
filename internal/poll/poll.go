package poll

import (
	"context"
	"log"
	"net/http"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
	"entropicworks.com/kafka-connector-restarter/internal/poll/actions"
	"entropicworks.com/kafka-connector-restarter/internal/poll/status"
)

func Poll(ctx context.Context, config environment.Configuration) {
	var connectHTTPClient = &http.Client{
		Timeout: config.CommunicationConfig.RequestTimeout,
	}
	connect := status.ConnectAPI{
		HTTPClient: connectHTTPClient,
		BaseURL:    config.ConnectConfig.URL,
		Auth:       config.ConnectConfig.AuthConfig,
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
			connectorRemediationActions := actions.MapConnectorStatusesToActions(connectorStatuses, config.PollingBehavior.RestartFailedTasks)
			actionURLs := actions.GenerateRemediationActionURLs(connectorRemediationActions, connect)
			for _, actionURL := range actionURLs {
				if err := actions.TakeAction(ctx, actionURL, connect); err != nil {
					log.Printf("Error taking remediation action %s: %v", actionURL, err)
				}
			}
		}
	}
}
