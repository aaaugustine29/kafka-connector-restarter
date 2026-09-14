package poll

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
)

type connectorRemediationActions struct {
	ConnectorName        string
	Restart              bool
	TaskIDsToBeRestarted []int
}

func determineAction(connector connectorStatus, restartTasks bool) connectorRemediationActions {
	var actions connectorRemediationActions

	actions.ConnectorName = connector.Name
	if strings.EqualFold(connector.Connector.State, "RUNNING") {
		if restartTasks {
			for _, task := range connector.Tasks {
				if strings.EqualFold(task.State, "FAILED") {
					actions.TaskIDsToBeRestarted = append(actions.TaskIDsToBeRestarted, task.ID)
					log.Println("Task", task.ID, "of connector", connector.Name, "appears to be unhealthy")
				}
			}
		}
	} else if strings.EqualFold(connector.Connector.State, "FAILED") {
		actions.Restart = true
		log.Println("Connector", connector.Name, "is in a failed state.")
	} else {
		log.Println("Connector", connector.Name, "is in state", connector.Connector.State, ". No action being taken.")
	}
	return actions
}

func mapConnectorStatusesToActions(statuses map[string]connectorStatus, restartTasks bool) []connectorRemediationActions {
	var connectorActions []connectorRemediationActions
	for _, status := range statuses {
		connectorActions = append(connectorActions, determineAction(status, restartTasks))
	}
	return connectorActions
}

func generateRemediationActionURLs(actions []connectorRemediationActions, connect connectAPI) []string {
	var restartURLs []string
	for _, action := range actions {
		if action.Restart {
			restartURLs = append(restartURLs, connect.baseURL+fmt.Sprintf(defaultConnectorRestartPath, action.ConnectorName))
		} else {
			for _, taskID := range action.TaskIDsToBeRestarted {
				restartURLs = append(restartURLs, connect.baseURL+fmt.Sprintf(defaultTaskRestartPath, action.ConnectorName, taskID))
			}
		}
	}
	return restartURLs
}

func takeAction(ctx context.Context, actionURL string, connect connectAPI) error {
	request, err := connect.newRequest(
		ctx,
		http.MethodPost,
		actionURL,
	)

	if err != nil {
		return fmt.Errorf("Error creating remediation request: %w", err)
	}

	response, err := connect.client.Do(request)

	if err != nil {
		return fmt.Errorf("Error taking action: %w", err)
	}

	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("Unexpected HTTP status code of %d with HTTP status of %s", response.StatusCode, response.Status)
	}

	return nil
}
