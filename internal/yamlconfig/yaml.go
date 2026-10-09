// Package yamlconfig provides strict YAML decoding shared by configuration loaders.
package yamlconfig

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"go.yaml.in/yaml/v3"
)

// ReadFile requires exactly one mapping document and rejects ambiguous overlays.
// Parser errors are sanitized because they may contain credential values.
func ReadFile(path string) (*yaml.Node, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("%q: expected one YAML mapping document; check YAML syntax", path)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%q: expected one YAML mapping document", path)
	}
	if err := checkNode(document.Content[0]); err != nil {
		return nil, fmt.Errorf("%q: %w", path, err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("%q: expected exactly one YAML document", path)
	}
	return document.Content[0], nil
}

// Decode overlays supplied fields onto out while rejecting unknown fields.
func Decode(node *yaml.Node, out any) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: expected a YAML mapping", node.Line)
	}
	// Node.Decode does not support KnownFields. Preserve strict field checking
	// when decoding one named cluster rather than replacing its entire map value.
	data, err := yaml.Marshal(node)
	if err != nil {
		return fmt.Errorf("invalid YAML mapping")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("invalid YAML field or value; check field names, types, duration strings, and log level")
	}
	return nil
}

func checkNode(node *yaml.Node) error {
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
		if err := checkNode(child); err != nil {
			return err
		}
	}
	return nil
}
