package actions

import (
	"context"
	"fmt"
	"log/slog"
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

const (
	defaultTaskRestartPath      = "/connectors/%s/tasks/%d/restart"
	defaultConnectorRestartPath = "/connectors/%s/restart?includeTasks=true&onlyFailed=true"
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
					slog.Warn("failed task detected; scheduling restart", "connector", status.Name, "task_id", task.ID)
				}
			}
		}
	} else if strings.EqualFold(status.Connector.State, "FAILED") {
		actions = append(actions, RemediationAction{
			ConnectorName: status.Name,
			Kind:          RestartConnector,
			TaskID:        0,
		})
		slog.Warn("failed connector detected; scheduling restart", "connector", status.Name)
	} else {
		slog.Debug("connector is not eligible for remediation", "connector", status.Name, "state", status.Connector.State)
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
	switch action.Kind {
	case RestartConnector:
		return connect.BaseURL + fmt.Sprintf(defaultConnectorRestartPath, action.ConnectorName), nil
	case RestartTask:
		return connect.BaseURL + fmt.Sprintf(defaultTaskRestartPath, action.ConnectorName, action.TaskID), nil
	default:
		return "", fmt.Errorf("unsupported remediation action %q for connector %q and task %d",
			action.Kind, action.ConnectorName, action.TaskID)
	}
}

func TakeAction(ctx context.Context, remediationAction RemediationAction, connect requests.ConnectAPI) error {
	requestURL, err := generateRemediationActionURL(connect, remediationAction)
	if err != nil {
		return err
	}

	slog.Info("taking remediation action", "action", remediationAction.Kind, "connector", remediationAction.ConnectorName, "task_id", remediationAction.TaskID)
	if err := makeActionRequest(ctx, connect, requestURL); err != nil {
		return err
	}

	slog.Info("remediation action completed", "action", remediationAction.Kind, "connector", remediationAction.ConnectorName, "task_id", remediationAction.TaskID)
	return nil
}

func makeActionRequest(ctx context.Context, connect requests.ConnectAPI, requestURL string) error {
	request, err := connect.NewRequest(
		ctx,
		http.MethodPost,
		requestURL,
	)

	if err != nil {
		return fmt.Errorf("create remediation request for %q: %w", requestURL, err)
	}

	response, err := connect.HTTPClient.Do(request)

	if err != nil {
		return fmt.Errorf("send remediation request to %q: %w", requestURL, err)
	}

	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("remediation request to %q returned unexpected HTTP status: %s", requestURL, response.Status)
	}
	return nil
}
