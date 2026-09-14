package actions

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

	"entropicworks.com/kafka-connector-restarter/internal/poll/status"
)

type RemediationAction struct {
	ConnectorName        string
	Restart              bool
	TaskIDsToBeRestarted []int
}

func DetermineAction(connector status.ConnectorStatus, restartTasks bool) RemediationAction {
	var actions RemediationAction

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

func MapConnectorStatusesToActions(statuses map[string]status.ConnectorStatus, restartTasks bool) []RemediationAction {
	var connectorActions []RemediationAction
	for _, status := range statuses {
		connectorActions = append(connectorActions, DetermineAction(status, restartTasks))
	}
	return connectorActions
}

func GenerateRemediationActionURLs(actions []RemediationAction, connect status.ConnectAPI) []string {
	var restartURLs []string
	for _, action := range actions {
		if action.Restart {
			restartURLs = append(restartURLs, connect.BaseURL+fmt.Sprintf(defaultConnectorRestartPath, action.ConnectorName))
		} else {
			for _, taskID := range action.TaskIDsToBeRestarted {
				restartURLs = append(restartURLs, connect.BaseURL+fmt.Sprintf(defaultTaskRestartPath, action.ConnectorName, taskID))
			}
		}
	}
	return restartURLs
}

func TakeAction(ctx context.Context, actionURL string, connect status.ConnectAPI) error {
	request, err := connect.NewRequest(
		ctx,
		http.MethodPost,
		actionURL,
	)

	if err != nil {
		return fmt.Errorf("Error creating remediation request: %w", err)
	}

	response, err := connect.HTTPClient.Do(request)

	if err != nil {
		return fmt.Errorf("Error taking action: %w", err)
	}

	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("Unexpected HTTP status code of %d with HTTP status of %s", response.StatusCode, response.Status)
	}

	return nil
}
