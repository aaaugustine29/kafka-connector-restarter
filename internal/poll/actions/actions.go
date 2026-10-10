package actions

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"entropicworks.com/kafka-connect-healer/internal/poll/status"
	"entropicworks.com/kafka-connect-healer/internal/poll/utils/requests"
)

type RemediationAction struct {
	ConnectorName string
	Kind          ActionKind
	TaskID        int
}

func (action RemediationAction) Logger(parent *slog.Logger) *slog.Logger {
	logger := parent.With("action", action.Kind, "connector", action.ConnectorName)
	if action.Kind == RestartTask {
		logger = logger.With("task_id", action.TaskID)
	}
	return logger
}

type RemediationActionResult struct {
	RequestMade bool
	AttemptedAt time.Time
	StatusCode  int
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
	logger *slog.Logger,
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
					logger.Debug("failed task detected", "connector", status.Name, "task_id", task.ID)
				}
			}
		}
	} else if strings.EqualFold(status.Connector.State, "FAILED") {
		actions = append(actions, RemediationAction{
			ConnectorName: status.Name,
			Kind:          RestartConnector,
			TaskID:        0,
		})
		logger.Debug("failed connector detected", "connector", status.Name)
	} else {
		logger.Debug("connector is not eligible for remediation", "connector", status.Name, "state", status.Connector.State)
	}
	return actions
}

func GenerateActionsFromStatuses(statuses map[string]status.ConnectorStatus, restartTasks bool, logger *slog.Logger) []RemediationAction {
	var connectorActions []RemediationAction
	for _, status := range statuses {
		connectorActions = append(connectorActions, DetermineActionsForConnector(status, restartTasks, logger)...)
	}
	return connectorActions
}

func generateRemediationActionURL(connect requests.ConnectAPI, action RemediationAction) (string, error) {
	switch action.Kind {
	case RestartConnector:
		return connect.BaseURL + fmt.Sprintf(defaultConnectorRestartPath, url.PathEscape(action.ConnectorName)), nil
	case RestartTask:
		return connect.BaseURL + fmt.Sprintf(defaultTaskRestartPath, url.PathEscape(action.ConnectorName), action.TaskID), nil
	default:
		return "", fmt.Errorf("unsupported remediation action %q for connector %q and task %d",
			action.Kind, action.ConnectorName, action.TaskID)
	}
}

func TakeAction(ctx context.Context, remediationAction RemediationAction, connect requests.ConnectAPI, logger *slog.Logger) (RemediationActionResult, error) {
	requestURL, err := generateRemediationActionURL(connect, remediationAction)
	if err != nil {
		return RemediationActionResult{
			RequestMade: false,
		}, err
	}

	logger = remediationAction.Logger(logger)
	logger.Info("sending remediation request")
	result, err := makeActionRequest(ctx, connect, requestURL)
	if err != nil {
		return result, err
	}

	logger.Info("remediation request accepted", "status_code", result.StatusCode)
	return result, nil
}

func makeActionRequest(ctx context.Context, connect requests.ConnectAPI, requestURL string) (RemediationActionResult, error) {
	result := RemediationActionResult{}

	request, err := connect.NewRequest(
		ctx,
		http.MethodPost,
		requestURL,
	)

	if err != nil {
		result.RequestMade = false
		return result, fmt.Errorf("create remediation request for %q: %w", requestURL, err)
	}

	result.RequestMade = true
	result.AttemptedAt = time.Now()
	response, err := connect.HTTPClient.Do(request)

	if err != nil {
		return result, fmt.Errorf("send remediation request to %q: %w", requestURL, err)
	}

	defer response.Body.Close()

	result.StatusCode = response.StatusCode

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return result, fmt.Errorf("remediation request to %q returned unexpected HTTP status: %s", requestURL, response.Status)
	}

	return result, nil
}
