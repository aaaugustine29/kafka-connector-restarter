package status

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
)

type TaskStatus struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
}

type ConnectorStatus struct {
	Name      string       `json:"name"`
	Connector WorkerStatus `json:"connector"`
	Tasks     []TaskStatus `json:"tasks"`
	Type      string       `json:"type"`
}

type WorkerStatus struct {
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
}

type ConnectAPI struct {
	HTTPClient *http.Client
	BaseURL    string
	Auth       environment.AuthConfiguration
}

func FindConnectorStatuses(
	ctx context.Context,
	connect ConnectAPI,
) (map[string]ConnectorStatus, error) {
	requestURL := connect.BaseURL + defaultConnectorStatusPath

	request, err := connect.NewRequest(
		ctx,
		http.MethodGet,
		requestURL,
	)

	if err != nil {
		return nil, fmt.Errorf("Error creating status request: %w", err)
	}

	response, err := connect.HTTPClient.Do(request)

	if err != nil {
		return nil, fmt.Errorf("Error getting connector statuses: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf(
			"Unexpected HTTP status code of %d with HTTP status of %s",
			response.StatusCode, response.Status)
	}

	connectorStatuses, err := mapConnectorStatusResponse(response)
	if err != nil {
		return nil, err
	}

	return connectorStatuses, nil
}

func (connect ConnectAPI) NewRequest(ctx context.Context, method string, requestURL string) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, method, requestURL, nil)
	if err != nil {
		return nil, err
	}

	if connect.Auth.Enabled {
		request.SetBasicAuth(connect.Auth.Username, connect.Auth.Password)
	}

	return request, nil
}

func mapConnectorStatusResponse(response *http.Response) (map[string]ConnectorStatus, error) {
	var responseStatuses map[string]struct {
		Status ConnectorStatus `json:"status"`
	}
	if err := json.NewDecoder(response.Body).Decode(&responseStatuses); err != nil {
		return nil, fmt.Errorf("decode connector statuses: %w", err)
	}

	connectorStatuses := make(map[string]ConnectorStatus, len(responseStatuses))
	for name, responseStatus := range responseStatuses {
		connectorStatuses[name] = responseStatus.Status
	}

	return connectorStatuses, nil
}
