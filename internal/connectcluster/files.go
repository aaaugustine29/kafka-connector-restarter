package connectcluster

import (
	"fmt"

	"entropicworks.com/kafka-connector-restarter/internal/yamlconfig"
)

// LoadFiles applies per-cluster defaults, a required base file, then an optional
// Secret overlay. Values are validated only after both files have been merged.
func LoadFiles(basePath, secretPath string) (map[string]ConnectClusterAPIConfiguration, error) {
	clusters := make(map[string]ConnectClusterAPIConfiguration)
	if err := decodeFile(basePath, clusters); err != nil {
		return nil, fmt.Errorf("base cluster configuration: %w", err)
	}
	if secretPath != "" {
		if err := decodeFile(secretPath, clusters); err != nil {
			return nil, fmt.Errorf("cluster Secret overlay: %w", err)
		}
	}
	for name, configuration := range clusters {
		if err := ValidateConfiguration(configuration); err != nil {
			return nil, fmt.Errorf("cluster %q: %w", name, err)
		}
	}
	return clusters, nil
}

func decodeFile(path string, clusters map[string]ConnectClusterAPIConfiguration) error {
	root, err := yamlconfig.ReadFile(path)
	if err != nil {
		return err
	}
	for i := 0; i < len(root.Content); i += 2 {
		name := root.Content[i].Value
		configuration, exists := clusters[name]
		if !exists {
			configuration = DefaultConfiguration()
		}
		if err := yamlconfig.Decode(root.Content[i+1], &configuration); err != nil {
			return fmt.Errorf("%q: cluster %q: %w", path, name, err)
		}
		clusters[name] = configuration
	}
	return nil
}
