package config

import (
	"bytes"
	jsonv1 "encoding/json"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"

	"entropicworks.com/kafka-connector-restarter/internal/config"
)

// json/v2 rejects duplicate keys itself. Inspect tokens only to enforce our
// additional rule that patches cannot contain null, which v2 otherwise accepts.
func validatePatchJSON(data []byte) error {
	decoder := jsontext.NewDecoder(bytes.NewReader(data))
	for {
		token, err := decoder.ReadToken()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return patchJSONError(err)
		}
		if token.Kind() == jsontext.KindNull {
			return fmt.Errorf("%s: null values are not allowed in configuration patches", decoder.StackPointer())
		}
	}
}

func decodeConfigPatch(data []byte, configuration *config.Configuration) error {
	if err := json.Unmarshal(data, configuration,
		json.RejectUnknownMembers(true),
		json.MatchCaseInsensitiveNames(true),
		// Preserve casing-only aliases, without newly accepting underscores or dashes.
		jsonv1.MatchCaseSensitiveDelimiter(true),
	); err != nil {
		return patchJSONError(err)
	}
	return nil
}

// JSON errors can contain credentials. Report only their location and a safe
// category, never the original error text, JSONValue, or underlying Err text.
func patchJSONError(err error) error {
	path := "configuration"
	reason := "invalid JSON"
	if semantic, ok := errors.AsType[*json.SemanticError](err); ok {
		if semantic.JSONPointer != "" {
			path = string(semantic.JSONPointer)
		}
		reason = "invalid value"
	} else if syntax, ok := errors.AsType[*jsontext.SyntacticError](err); ok && syntax.JSONPointer != "" {
		path = string(syntax.JSONPointer)
	}
	if errors.Is(err, jsontext.ErrDuplicateName) {
		reason = "duplicate keys are not allowed in configuration patches"
	} else if errors.Is(err, json.ErrUnknownName) {
		reason = "unknown field"
	}
	return fmt.Errorf("%s: %s", path, reason)
}
