package poll

import (
	"context"
	"log"
	"net/http"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
)

type connectAPI struct {
	client  *http.Client
	baseURL string
	auth    environment.AuthConfiguration
}

func Poll(ctx context.Context, config environment.Configuration) {
	var connectHTTPClient = &http.Client{
		Timeout: config.CommunicationConfig.RequestTimeout,
	}
	connect := connectAPI{
		client:  connectHTTPClient,
		baseURL: config.ConnectConfig.URL,
		auth:    config.ConnectConfig.AuthConfig,
	}

	ticker := time.NewTicker(config.PollingBehavior.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			connectorStatuses, err := FindConnectorStatuses(ctx, connect)
			if err != nil {
				log.Printf("Error during poll cycle: %v", err)
				continue
			}
			connectorRemediationActions := mapConnectorStatusesToActions(connectorStatuses, config.PollingBehavior.RestartFailedTasks)
			actionURLs := generateRemediationActionURLs(connectorRemediationActions, connect)
			for _, actionURL := range actionURLs {
				if err := takeAction(ctx, actionURL, connect); err != nil {
					log.Printf("Error taking remediation action %s: %v", actionURL, err)
				}
			}
		}
	}
}
