package requests

import (
	"context"
	"net"
	"net/http"
	"net/url"

	"entropicworks.com/kafka-connector-restarter/internal/connectcluster"
)

type ConnectAPI struct {
	HTTPClient *http.Client
	BaseURL    string
	Auth       connectcluster.AuthConfiguration
}

func NewConnectAPI(httpClient *http.Client, configuration connectcluster.ConnectClusterAPIConfiguration) ConnectAPI {
	// Keep the caller's client unchanged while preventing redirected requests.
	client := *httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	scheme := "http"
	if configuration.HTTPS {
		scheme = "https"
	}

	return ConnectAPI{
		HTTPClient: &client,
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
