package config

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

// LoadConfig reads a YAML configuration file from the given path
// and unmarshals it into a Configuration struct.
func LoadConfig(filePath string) (*Configuration, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read configuration file %s: %w", filePath, err)
	}

	var config Configuration
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal configuration from %s: %w", filePath, err)
	}

	// Schema Validation
	if err := ValidateConfiguration(&config); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return &config, nil
}

// validateConfiguration performs validation on the loaded Configuration struct.
func ValidateConfiguration(cfg *Configuration) error {
	if cfg == nil {
		return fmt.Errorf("configuration cannot be nil")
	}

	// Validate IndexSchemas
	if len(cfg.IndexSchemas) == 0 {
		return fmt.Errorf("at least one index schema must be defined")
	}
	for _, schema := range cfg.IndexSchemas {
		if schema.Name == "" {
			return fmt.Errorf("index schema name cannot be empty")
		}
		if len(schema.Fields) == 0 {
			return fmt.Errorf("index schema '%s' must define at least one field", schema.Name)
		}
		for _, field := range schema.Fields {
			if field.Name == "" {
				return fmt.Errorf("field name in schema '%s' cannot be empty", schema.Name)
			}
			if field.Type == "" {
				return fmt.Errorf("field '%s' in schema '%s' must have a type", field.Name, schema.Name)
			}
			// Basic type validation (can be extended with a map of valid types)
			switch field.Type {
			case "string", "text", "integer", "float", "boolean", "datetime":
				// Valid type
			default:
				return fmt.Errorf("field '%s' in schema '%s' has an unsupported type '%s'", field.Name, schema.Name, field.Type)
			}
		}
	}

	// Validate ComputedFields
	for _, cField := range cfg.ComputedFields {
		if cField.Name == "" {
			return fmt.Errorf("computed field name cannot be empty")
		}
		if cField.Expression == "" {
			return fmt.Errorf("computed field '%s' must have an expression", cField.Name)
		}
		if cField.Type == "" {
			return fmt.Errorf("computed field '%s' must have a type", cField.Name)
		}
		// Basic type validation for computed fields
		switch cField.Type {
		case "string", "integer", "float", "boolean":
			// Valid type
		default:
			return fmt.Errorf("computed field '%s' has an unsupported type '%s'", cField.Name, cField.Type)
		}
	}

	// Validate QueryPlanningPipelines
	for _, pipeline := range cfg.QueryPlanningPipelines {
		if pipeline.Name == "" {
			return fmt.Errorf("query planning pipeline name cannot be empty")
		}
		if len(pipeline.Steps) == 0 {
			return fmt.Errorf("query planning pipeline '%s' must define at least one step", pipeline.Name)
		}
		for _, step := range pipeline.Steps {
			if step == "" {
				return fmt.Errorf("query planning pipeline '%s' contains an empty step", pipeline.Name)
			}
		}
	}

	return nil
}
