package config

import "testing"

func TestDefaultConfigurationMatchesComponentDefaults(t *testing.T) {
	got := DefaultConfiguration()

	if got.CommunicationConfig != DefaultCommunicationConfiguration() {
		t.Fatalf("CommunicationConfig = %#v, want %#v", got.CommunicationConfig, DefaultCommunicationConfiguration())
	}
	if got.PollingBehavior != DefaultPollingBehavior() {
		t.Fatalf("PollingBehavior = %#v, want %#v", got.PollingBehavior, DefaultPollingBehavior())
	}
	if got.ConnectConfig != DefaultConnectAPIConfiguration() {
		t.Fatalf("ConnectConfig = %#v, want %#v", got.ConnectConfig, DefaultConnectAPIConfiguration())
	}
	if got.LoggingConfig != DefaultLoggingConfiguration() {
		t.Fatalf("LoggingConfig = %#v, want %#v", got.LoggingConfig, DefaultLoggingConfiguration())
	}
}

func TestDefaultConnectAPIConfigurationBuildsURL(t *testing.T) {
	got := DefaultConnectAPIConfiguration()

	if got.URL != "http://localhost:8083" {
		t.Fatalf("URL = %q, want %q", got.URL, "http://localhost:8083")
	}
}
