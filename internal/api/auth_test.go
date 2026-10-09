package api

import (
	"encoding/json/jsontext"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"entropicworks.com/kafka-connector-restarter/internal/config"
)

func TestAPIAuthentication(t *testing.T) {
	for _, route := range []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, "/", http.StatusOK},
		{http.MethodGet, "/config", http.StatusOK},
		{http.MethodPatch, "/config", http.StatusNoContent},
		{http.MethodGet, "/missing", http.StatusNotFound},
		{http.MethodPost, "/config", http.StatusMethodNotAllowed},
	} {
		for _, credentials := range []struct {
			name          string
			authorization string
			username      string
			password      string
			valid         bool
		}{
			{name: "missing"},
			{name: "malformed", authorization: "Basic not-base64"},
			{name: "missing colon", authorization: "Basic dXNlcg=="},
			{name: "empty credentials", authorization: "Basic Og=="},
			{name: "wrong scheme", authorization: "Bearer token"},
			{name: "wrong username", username: "other-user", password: "api-secret"},
			{name: "wrong password", username: "api-user", password: "wrong"},
			{name: "Connect credentials", username: "connect-user", password: "connect-secret"},
			{name: "correct", username: "api-user", password: "api-secret", valid: true},
		} {
			t.Run(route.method+" "+route.path+"/"+credentials.name, func(t *testing.T) {
				before := config.DefaultConfiguration()
				before.APIConfig.AuthConfig = config.AuthConfiguration{
					Enabled: true, Username: "api-user", Password: "api-secret",
				}
				manager := config.NewManager(before)
				_, changes := manager.ConfigurationSnapshot()
				server := NewServer(APIComponents{ConfigManager: manager})
				request := httptest.NewRequest(route.method, route.path, strings.NewReader(`{"pollingBehavior":{"restartFailedTasks":false}}`))
				request.Header.Set("Content-Type", "application/json")
				if credentials.authorization != "" {
					request.Header.Set("Authorization", credentials.authorization)
				} else if credentials.username != "" || credentials.password != "" {
					request.SetBasicAuth(credentials.username, credentials.password)
				}
				response := httptest.NewRecorder()
				server.Handler.ServeHTTP(response, request)
				wantStatus := http.StatusUnauthorized
				if credentials.valid {
					wantStatus = route.status
				}
				if response.Code != wantStatus {
					t.Fatalf("status = %d, want %d", response.Code, wantStatus)
				}
				if strings.Contains(response.Body.String(), "api-secret") || strings.Contains(response.Body.String(), "connect-secret") {
					t.Fatal("response exposed credentials")
				}
				if !credentials.valid {
					if response.Header().Get("WWW-Authenticate") != `Basic realm="kafka-connector-restarter", charset="UTF-8"` {
						t.Fatal("unauthorized response did not include the Basic Auth challenge")
					}
					if response.Header().Get("Cache-Control") != "no-store" {
						t.Fatal("unauthorized response can be cached")
					}
					if manager.GetConfiguration() != before {
						t.Fatal("unauthorized request changed configuration")
					}
					select {
					case <-changes:
						t.Fatal("unauthorized request notified workers")
					default:
					}
				}
				if credentials.valid && route.method == http.MethodPatch && manager.GetConfiguration().PollingBehavior.RestartFailedTasks {
					t.Fatal("authenticated PATCH did not apply its change")
				}
			})
		}
	}
}

func TestAPIAuthenticationDisabledByDefault(t *testing.T) {
	server := NewServer(APIComponents{ConfigManager: config.NewManager(config.DefaultConfiguration())})
	for _, header := range []string{"", "Basic invalid"} {
		request := httptest.NewRequest(http.MethodGet, "/config", nil)
		request.Header.Set("Authorization", header)
		response := httptest.NewRecorder()
		server.Handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Header().Get("WWW-Authenticate") != "" {
			t.Fatal("disabled authentication changed existing API behavior")
		}
	}
}

func TestAPIAuthenticationIdenticalPatchIsAllowed(t *testing.T) {
	before := config.DefaultConfiguration()
	before.APIConfig.AuthConfig = config.AuthConfiguration{
		Enabled: true, Username: "api-user", Password: "api-secret",
	}
	manager := config.NewManager(before)
	_, changes := manager.ConfigurationSnapshot()
	server := NewServer(APIComponents{ConfigManager: manager})
	request := httptest.NewRequest(http.MethodPatch, "/config", strings.NewReader(`{"apiConfig":{"authConfig":{"enabled":true,"username":"api-user"}}}`))
	request.Header.Set("Content-Type", "application/json")
	request.SetBasicAuth("api-user", "api-secret")
	response := httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || manager.GetConfiguration() != before {
		t.Fatal("identical API auth patch should succeed without changing configuration")
	}
	select {
	case <-changes:
		t.Fatal("identical API auth patch notified workers")
	default:
	}
}

func TestAPIAuthenticationCannotBeChangedThroughPatch(t *testing.T) {
	for _, body := range []string{
		`{"apiConfig":{"authConfig":{"enabled":false}}}`,
		`{"apiConfig":{"authConfig":{"username":"another-user"}}}`,
		`{"apiConfig":{"authConfig":{"password":"new-secret"}}}`,
		`{"pollingBehavior":{"restartFailedTasks":false},"apiConfig":{"authConfig":{"enabled":false}}}`,
	} {
		t.Run(body, func(t *testing.T) {
			before := config.DefaultConfiguration()
			before.APIConfig.AuthConfig = config.AuthConfiguration{
				Enabled: true, Username: "api-user", Password: "api-secret",
			}
			manager := config.NewManager(before)
			_, changes := manager.ConfigurationSnapshot()
			server := NewServer(APIComponents{ConfigManager: manager})
			request := httptest.NewRequest(http.MethodPatch, "/config", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.SetBasicAuth("api-user", "api-secret")
			response := httptest.NewRecorder()
			server.Handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "startup-only") {
				t.Fatalf("status = %d, body = %s, want startup-only error", response.Code, response.Body.String())
			}
			if manager.GetConfiguration() != before {
				t.Fatal("rejected auth patch changed configuration")
			}
			select {
			case <-changes:
				t.Fatal("rejected auth patch notified workers")
			default:
			}
			if strings.Contains(response.Body.String(), "new-secret") || strings.Contains(response.Body.String(), "api-secret") {
				t.Fatal("rejected auth patch exposed a password")
			}
			get := httptest.NewRequest(http.MethodGet, "/config", nil)
			get.SetBasicAuth("api-user", "api-secret")
			getResponse := httptest.NewRecorder()
			server.Handler.ServeHTTP(getResponse, get)
			if getResponse.Code != http.StatusOK || !jsontext.Value(getResponse.Body.Bytes()).IsValid() {
				t.Fatal("rejected auth patch broke access with original credentials")
			}
		})
	}
}
