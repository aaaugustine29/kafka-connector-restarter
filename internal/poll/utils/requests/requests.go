package requests

import (
	"context"
	"net/http"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
)

type ConnectAPI struct {
	HTTPClient *http.Client
	BaseURL    string
	Auth       environment.AuthConfiguration
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
