package actions

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"

	"entropicworks.com/kafka-connector-restarter/internal/poll/status"
	"entropicworks.com/kafka-connector-restarter/internal/poll/utils/requests"
)

type RemediationAction struct {
	ConnectorName string
	Kind          ActionKind
	TaskID        int
}

type ActionKind string

const (
	RestartConnector ActionKind = "restart_connector"
	RestartTask      ActionKind = "restart_task"
)

func DetermineActionsForConnector(
	status status.ConnectorStatus,
	restartTasks bool,
) []RemediationAction {
	var actions []RemediationAction
	if strings.EqualFold(status.Connector.State, "RUNNING") {
		if restartTasks {
			for _, task := range status.Tasks {
				if strings.EqualFold(task.State, "FAILED") {
					actions = append(actions, RemediationAction{
						ConnectorName: status.Name,
						Kind:          RestartTask,
						TaskID:        task.ID,
					})
					log.Println("Task", task.ID, "of connector", status.Name, "appears to be unhealthy")
				}
			}
		}
	} else if strings.EqualFold(status.Connector.State, "FAILED") {
		actions = append(actions, RemediationAction{
			ConnectorName: status.Name,
			Kind:          RestartConnector,
			TaskID:        0,
		})
		log.Println("Connector", status.Name, "is in a failed state.")
	} else {
		log.Println("Connector", status.Name, "is in state", status.Connector.State, ". No action being taken.")
	}
	return actions
}

func GenerateActionsFromStatuses(statuses map[string]status.ConnectorStatus, restartTasks bool) []RemediationAction {
	var connectorActions []RemediationAction
	for _, status := range statuses {
		connectorActions = append(connectorActions, DetermineActionsForConnector(status, restartTasks)...)
	}
	return connectorActions
}

func generateRemediationActionURL(connect requests.ConnectAPI, action RemediationAction) (string, error) {
	if action.Kind == RestartConnector {
		return connect.BaseURL + fmt.Sprintf(defaultConnectorRestartPath, action.ConnectorName), nil
	} else if action.Kind == RestartTask {
		return connect.BaseURL + fmt.Sprintf(defaultTaskRestartPath, action.ConnectorName, action.TaskID), nil
	} else {
		return "", errors.New(fmt.Sprintf("While taking action: Unknown action kind: %s for connector %s or task %d\n",
			action.Kind, action.ConnectorName, action.TaskID))
	}
}

func TakeAction(ctx context.Context, remediationAction RemediationAction, connect requests.ConnectAPI) error {
	requestURL, err := generateRemediationActionURL(connect, remediationAction)
	if err != nil {
		return err
	}

	return makeActionRequest(ctx, connect, requestURL)
}

func makeActionRequest(ctx context.Context, connect requests.ConnectAPI, requestURL string) error {
	request, err := connect.NewRequest(
		ctx,
		http.MethodPost,
		requestURL,
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
