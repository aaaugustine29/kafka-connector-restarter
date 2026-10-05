package config

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration stores a time.Duration and represents it as a duration string in JSON.
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
