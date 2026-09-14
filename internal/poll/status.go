package poll

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type taskStatus struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
}

type connectorStatus struct {
	Name      string       `json:"name"`
	Connector workerStatus `json:"connector"`
	Tasks     []taskStatus `json:"tasks"`
	Type      string       `json:"type"`
}

type workerStatus struct {
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
}

func FindConnectorStatuses(
	ctx context.Context,
	connect connectAPI,
) (map[string]connectorStatus, error) {
	requestURL := connect.baseURL + defaultConnectorStatusPath

	request, err := connect.newRequest(
		ctx,
		http.MethodGet,
		requestURL,
	)

	if err != nil {
		return nil, fmt.Errorf("Error creating status request: %w", err)
	}

	response, err := connect.client.Do(request)

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

func (connect connectAPI) newRequest(ctx context.Context, method string, requestURL string) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, method, requestURL, nil)
	if err != nil {
		return nil, err
	}

	if connect.auth.Enabled {
		request.SetBasicAuth(connect.auth.Username, connect.auth.Password)
	}

	return request, nil
}

func mapConnectorStatusResponse(response *http.Response) (map[string]connectorStatus, error) {
	var responseStatuses map[string]struct {
		Status connectorStatus `json:"status"`
	}
	if err := json.NewDecoder(response.Body).Decode(&responseStatuses); err != nil {
		return nil, fmt.Errorf("decode connector statuses: %w", err)
	}

	connectorStatuses := make(map[string]connectorStatus, len(responseStatuses))
	for name, responseStatus := range responseStatuses {
		connectorStatuses[name] = responseStatus.Status
	}

	return connectorStatuses, nil
}
