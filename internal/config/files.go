package config

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"go.yaml.in/yaml/v3"
)

// LoadFiles loads defaults, then the required base file, then an optional Secret
// overlay. Only supplied fields replace existing values, including false and "".
// No configuration is returned unless both files and the final values are valid.
func LoadFiles(basePath, secretPath string) (Configuration, error) {
	configuration := DefaultConfiguration()
	if err := decodeFile(basePath, &configuration); err != nil {
		return Configuration{}, fmt.Errorf("base configuration: %w", err)
	}
	if secretPath != "" {
		if err := decodeFile(secretPath, &configuration); err != nil {
			return Configuration{}, fmt.Errorf("Secret overlay: %w", err)
		}
	}
	if err := ValidateConfiguration(configuration); err != nil {
		return Configuration{}, fmt.Errorf("invalid merged configuration: %w", err)
	}
	return configuration, nil
}

func decodeFile(path string, configuration *Configuration) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %q: %w", path, err)
	}
	if err := decodeYAML(data, configuration); err != nil {
		return fmt.Errorf("%q: %w", path, err)
	}
	return nil
}

func decodeYAML(data []byte, configuration *Configuration) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		// Parser and decoder errors may contain credential values. Never expose
		// their original text, even for the base file.
		return fmt.Errorf("expected one YAML mapping document; check YAML syntax")
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return fmt.Errorf("expected one YAML mapping document")
	}
	if err := checkYAMLNode(document.Content[0]); err != nil {
		return err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("expected exactly one YAML document")
	}

	decoder = yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(configuration); err != nil {
		return fmt.Errorf("invalid YAML field or value; check field names, types, duration strings, and log level")
	}
	return nil
}

// Keep overlay semantics explicit: null cannot silently preserve a default,
// and aliases/merge keys cannot introduce another layer of precedence.
func checkYAMLNode(node *yaml.Node) error {
	if node.Kind == yaml.AliasNode || node.Tag == "!!merge" {
		return fmt.Errorf("line %d: YAML aliases and merge keys are not supported", node.Line)
	}
	if node.Tag == "!!null" {
		return fmt.Errorf("line %d: null values are not supported; omit the field or supply a value", node.Line)
	}
	if node.Kind == yaml.MappingNode {
		seen := make(map[string]bool)
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i]
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
				return fmt.Errorf("line %d: mapping keys must be strings; YAML merge keys are not supported", key.Line)
			}
			if seen[key.Value] {
				return fmt.Errorf("line %d: duplicate YAML key", key.Line)
			}
			seen[key.Value] = true
		}
	}
	for _, child := range node.Content {
		if err := checkYAMLNode(child); err != nil {
			return err
		}
	}
	return nil
}
