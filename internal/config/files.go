package config

import (
	"fmt"

	"entropicworks.com/kafka-connector-restarter/internal/yamlconfig"
)

// LoadFiles loads application defaults, an optional base file, and an optional
// Secret overlay. No configuration is returned unless the merged values are valid.
func LoadFiles(basePath, secretPath string) (ApplicationConfiguration, error) {
	configuration := DefaultConfiguration()
	if basePath != "" {
		if err := decodeFile(basePath, &configuration); err != nil {
			return ApplicationConfiguration{}, fmt.Errorf("base configuration: %w", err)
		}
	}
	if secretPath != "" {
		if err := decodeFile(secretPath, &configuration); err != nil {
			return ApplicationConfiguration{}, fmt.Errorf("Secret overlay: %w", err)
		}
	}
	if err := ValidateConfiguration(configuration); err != nil {
		return ApplicationConfiguration{}, fmt.Errorf("invalid merged configuration: %w", err)
	}
	return configuration, nil
}

func decodeFile(path string, configuration *ApplicationConfiguration) error {
	node, err := yamlconfig.ReadFile(path)
	if err != nil {
		return err
	}
	if err := yamlconfig.Decode(node, configuration); err != nil {
		return fmt.Errorf("%q: %w", path, err)
	}
	return nil
}
