package status

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net/http"

	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/requests"
)

const defaultConnectorStatusPath = "/connectors?expand=status"

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

func FindConnectorStatuses(
	ctx context.Context,
	connect requests.ConnectAPI,
) (map[string]ConnectorStatus, error) {
	requestURL := connect.BaseURL + defaultConnectorStatusPath

	request, err := connect.NewRequest(
		ctx,
		http.MethodGet,
		requestURL,
	)

	if err != nil {
		return nil, fmt.Errorf("create connector status request: %w", err)
	}

	response, err := connect.HTTPClient.Do(request)

	if err != nil {
		return nil, fmt.Errorf("retrieve connector statuses: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("retrieve connector statuses: unexpected HTTP status: %s", response.Status)
	}

	connectorStatuses, err := mapConnectorStatusResponse(response)
	if err != nil {
		return nil, err
	}

	return connectorStatuses, nil
}

func mapConnectorStatusResponse(response *http.Response) (map[string]ConnectorStatus, error) {
	var responseStatuses map[string]struct {
		Status ConnectorStatus `json:"status"`
	}
	if err := json.UnmarshalRead(response.Body, &responseStatuses); err != nil {
		return nil, fmt.Errorf("decode connector statuses: %w", err)
	}
	if responseStatuses == nil {
		return nil, fmt.Errorf("decode connector statuses: expected an object, got null")
	}

	connectorStatuses := make(map[string]ConnectorStatus, len(responseStatuses))
	for name, responseStatus := range responseStatuses {
		if responseStatus.Status.Name != name || responseStatus.Status.Connector.State == "" {
			return nil, fmt.Errorf("decode connector statuses: missing or inconsistent status for connector %q", name)
		}
		for _, task := range responseStatus.Status.Tasks {
			if task.ID < 0 || task.State == "" {
				return nil, fmt.Errorf("decode connector statuses: invalid task status for connector %q", name)
			}
		}
		connectorStatuses[name] = responseStatus.Status
	}

	return connectorStatuses, nil
}
