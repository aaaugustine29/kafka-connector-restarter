package requests

import (
	"context"
	"net"
	"net/http"
	"net/url"

	"entropicworks.com/kafka-connector-restarter/internal/config"
)

type ConnectAPI struct {
	HTTPClient *http.Client
	BaseURL    string
	Auth       config.AuthConfiguration
}

func NewConnectAPI(httpClient *http.Client, configuration config.ConnectAPIConfiguration) ConnectAPI {
	scheme := "http"
	if configuration.HTTPS {
		scheme = "https"
	}

	return ConnectAPI{
		HTTPClient: httpClient,
		BaseURL: (&url.URL{
			Scheme: scheme,
			Host:   net.JoinHostPort(configuration.Host, configuration.Port),
		}).String(),
		Auth: configuration.AuthConfig,
	}
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
