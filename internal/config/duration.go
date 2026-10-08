package config

import (
	"encoding/json/v2"
	"fmt"
	"time"

	"go.yaml.in/yaml/v3"
)

// Duration stores a time.Duration and represents it as a duration string in JSON and YAML.
type Duration time.Duration

func ParseDuration(value string) (Duration, error) {
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	return Duration(duration), nil
}

func (duration Duration) Duration() time.Duration {
	return time.Duration(duration)
}

func (duration Duration) String() string {
	return duration.Duration().String()
}

func (duration Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(duration.String())
}

func (duration *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("duration must be a string with units: %w", err)
	}
	next, err := ParseDuration(value)
	if err != nil {
		return err
	}
	*duration = next
	return nil
}

func (duration Duration) MarshalYAML() (any, error) {
	return duration.String(), nil
}

func (duration *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fmt.Errorf("line %d: duration must be a string with units", node.Line)
	}
	next, err := ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("line %d: invalid duration string", node.Line)
	}
	*duration = next
	return nil
}
