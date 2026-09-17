package status

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestMapConnectorStatusResponse(t *testing.T) {
	response := &http.Response{
		Body: io.NopCloser(strings.NewReader(`{
			"FileStreamSinkConnectorConnector_0": {
				"status": {
					"name": "FileStreamSinkConnectorConnector_0",
					"connector": {"state": "RUNNING", "worker_id": "10.0.0.162:8083"},
					"tasks": [{"id": 0, "state": "RUNNING", "worker_id": "10.0.0.162:8083"}],
					"type": "sink"
				}
			}
		}`)),
	}

	connectorStatuses, err := mapConnectorStatusResponse(response)
	if err != nil {
		t.Fatalf("mapConnectorStatusResponse() error = %v", err)
	}

	status, ok := connectorStatuses["FileStreamSinkConnectorConnector_0"]
	if !ok {
		t.Fatal("connector status was not mapped")
	}

	if status.Connector.State != "RUNNING" || status.Connector.WorkerID != "10.0.0.162:8083" {
		t.Fatalf("connector status = %#v, want a running connector on 10.0.0.162:8083", status.Connector)
	}

	if len(status.Tasks) != 1 || status.Tasks[0].ID != 0 || status.Tasks[0].WorkerID != "10.0.0.162:8083" {
		t.Fatalf("task statuses = %#v, want one running task", status.Tasks)
	}
}
