package config

import (
	"encoding/json/v2"
	"testing"
	"time"
)

func TestDurationJSONRoundTrip(t *testing.T) {
	for _, test := range []time.Duration{
		0, time.Nanosecond, 250 * time.Millisecond, 1500 * time.Millisecond,
		90 * time.Second, -time.Second, time.Duration(1<<63 - 1), time.Duration(-1 << 63),
	} {
		t.Run(test.String(), func(t *testing.T) {
			data, err := json.Marshal(Duration(test))
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			var value string
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatalf("duration JSON is not a string: %s", data)
			}
			if value != test.String() {
				t.Fatalf("duration JSON = %q, want %q", value, test.String())
			}
			var got Duration
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if got.Duration() != test {
				t.Fatalf("duration after round trip = %v, want %v", got, test)
			}
		})
	}
}

func TestDurationJSONRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{
		`1000000000`, `true`, `null`, `{}`, `[]`, `""`,
		`"1000"`, `"invalid"`, `"9223372036854775808ns"`,
	} {
		t.Run(value, func(t *testing.T) {
			before := Duration(time.Second)
			got := before
			if err := json.Unmarshal([]byte(value), &got); err == nil {
				t.Fatal("Unmarshal() error = nil, want error")
			}
			if got != before {
				t.Fatalf("duration after failed decode = %v, want %v", got, before)
			}
		})
	}
}

func TestConfigurationJSONDurationStrings(t *testing.T) {
	configuration := DefaultConfiguration()
	data, err := json.Marshal(configuration)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var response struct {
		PollingBehavior struct {
			Interval string `json:"interval"`
			Backoff  struct {
				BaseDelay string `json:"baseDelay"`
				MaxDelay  string `json:"maxDelay"`
			} `json:"backoff"`
		} `json:"pollingBehavior"`
		CommunicationConfig struct {
			RequestTimeout string `json:"requestTimeout"`
		} `json:"communicationConfig"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		t.Fatalf("decode configuration duration strings: %v", err)
	}
	if response.PollingBehavior.Interval != "10s" ||
		response.PollingBehavior.Backoff.BaseDelay != "20s" ||
		response.PollingBehavior.Backoff.MaxDelay != "10m0s" ||
		response.CommunicationConfig.RequestTimeout != "10s" {
		t.Fatalf("unexpected configuration durations: %s", data)
	}
	var got ApplicationConfiguration
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got != configuration {
		t.Fatalf("configuration after round trip = %#v, want %#v", got, configuration)
	}
}

func TestManagerUpdateFromJSON(t *testing.T) {
	manager := NewManager(DefaultConfiguration())
	if err := manager.UpdateConfiguration(func(configuration *ApplicationConfiguration) error {
		return json.Unmarshal([]byte(`{
			"pollingBehavior": {"interval": "1m30s", "restartFailedTasks": false},
			"communicationConfig": {"requestTimeout": "1.5s"}
		}`), configuration)
	}); err != nil {
		t.Fatalf("UpdateConfiguration() error = %v", err)
	}
	before, changes := manager.ConfigurationSnapshot()
	if before.PollingBehavior.Interval != Duration(90*time.Second) ||
		before.PollingBehavior.RestartFailedTasks ||
		before.CommunicationConfig.RequestTimeout != Duration(1500*time.Millisecond) {
		t.Fatalf("configuration after JSON update = %#v", before)
	}
	if before.PollingBehavior.Backoff != DefaultBackoffConfiguration() {
		t.Fatal("JSON update changed an omitted backoff configuration")
	}
	for _, value := range []string{`"0s"`, `"-1s"`, `"9223372036854775808ns"`, `1000`} {
		err := manager.UpdateConfiguration(func(configuration *ApplicationConfiguration) error {
			return json.Unmarshal([]byte(`{"pollingBehavior":{"interval":`+value+`}}`), configuration)
		})
		if err == nil {
			t.Fatalf("UpdateConfiguration(%s) error = nil, want error", value)
		}
		if got := manager.GetConfiguration(); got != before {
			t.Fatalf("configuration changed after invalid JSON update %s", value)
		}
		select {
		case <-changes:
			t.Fatal("invalid JSON update sent a change notification")
		default:
		}
	}
}
