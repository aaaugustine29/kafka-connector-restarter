package config

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/config"
)

func TestGetConfigReturnsLatestConfigurationWithoutPassword(t *testing.T) {
	configuration := config.DefaultConfiguration()
	configuration.ConnectConfig.HTTPS = true
	configuration.ConnectConfig.AuthConfig = config.AuthConfiguration{
		Enabled: true, Username: "user", Password: "secret",
	}
	manager := config.NewManager(configuration)
	handler := NewConfigHandler(manager)
	if err := manager.UpdateConfiguration(func(configuration *config.Configuration) error {
		configuration.PollingBehavior.Interval = config.Duration(250 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatalf("UpdateConfiguration() error = %v", err)
	}
	response := httptest.NewRecorder()
	handler.GetConfig(response, httptest.NewRequest(http.MethodGet, "/config", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var body struct {
		PollingBehavior struct {
			Interval string `json:"interval"`
		} `json:"pollingBehavior"`
		ConnectConfig struct {
			AuthConfig map[string]json.RawMessage `json:"authConfig"`
		} `json:"connectConfig"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode config response: %v", err)
	}
	if body.PollingBehavior.Interval != "250ms" {
		t.Fatalf("interval = %q, want 250ms", body.PollingBehavior.Interval)
	}
	if _, present := body.ConnectConfig.AuthConfig["password"]; present {
		t.Fatal("config response includes the password field")
	}
	if got := manager.GetConfiguration().ConnectConfig.AuthConfig.Password; got != "secret" {
		t.Fatal("GET config modified the stored password")
	}
}
