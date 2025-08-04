package query_understanding

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"
)

// DataType defines the type of data for an index field.
type DataType int

const (
	STRING DataType = iota
	INTEGER
	FLOAT
	BOOLEAN
	DATETIME
)

// IndexField defines the schema for a single field within an index.
type IndexField struct {
	FieldName    string   `json:"field_name" yaml:"field_name"`
	DataType     DataType `json:"data_type" yaml:"data_type"`
	Indexed      bool     `json:"indexed" yaml:"indexed"`
	Stored       bool     `json:"stored" yaml:"stored"`
	Sortable     bool     `json:"sortable" yaml:"sortable"`
	Aggregatable bool     `json:"aggregatable" yaml:"aggregatable"`
	Analyzer     string   `json:"analyzer" yaml:"analyzer"` // e.g., "standard", "keyword", "whitespace"
}

// ComputedField defines a field whose value is derived from an expression based on other fields.
type ComputedField struct {
	FieldName  string `json:"field_name" yaml:"field_name"`
	Expression string `json:"expression" yaml:"expression"` // e.g., "first_name + ' ' + last_name"
}

// IndexConfiguration holds the complete configuration for an index, including fields, computed fields, and pipelines.
type IndexConfiguration struct {
	IndexFields    []IndexField            `json:"index_fields" yaml:"index_fields"`
	ComputedFields []ComputedField         `json:"computed_fields" yaml:"computed_fields"`
	Pipelines      []QueryPlanningPipeline `json:"pipelines" yaml:"pipelines"`
}

// LoadIndexConfiguration loads index configuration from a YAML file.
func LoadIndexConfiguration(filePath string) (*IndexConfiguration, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("configuration file not found: %s", filePath)
	}

	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read configuration file %s: %w", filePath, err)
	}

	var config IndexConfiguration
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal configuration file %s: %w", filePath, err)
	}

	return &config, nil
}

// Validate performs basic validation on the IndexConfiguration.
func (ic *IndexConfiguration) Validate() error {
	fieldNames := make(map[string]bool)

	// Validate IndexFields
	for _, field := range ic.IndexFields {
		if field.FieldName == "" {
			return errors.New("index field name cannot be empty")
		}
		if fieldNames[field.FieldName] {
			return fmt.Errorf("duplicate index field name found: %s", field.FieldName)
		}
		fieldNames[field.FieldName] = true

		// Basic data type validation
		// Assuming DATETIME is the last valid enum value.
		if field.DataType < STRING || field.DataType > DATETIME {
			return fmt.Errorf("invalid data type '%d' for field '%s'", field.DataType, field.FieldName)
		}
		// Further validation for analyzer etc. could go here
	}

	// Validate ComputedFields
	for _, field := range ic.ComputedFields {
		if field.FieldName == "" {
			return errors.New("computed field name cannot be empty")
		}
		if fieldNames[field.FieldName] {
			return fmt.Errorf("duplicate field name (index or computed) found: %s", field.FieldName)
		}
		fieldNames[field.FieldName] = true
		if field.Expression == "" {
			return fmt.Errorf("computed field '%s' must have an expression", field.FieldName)
		}
	}

	// Validate Pipelines
	pipelineNames := make(map[string]bool)
	for _, pipeline := range ic.Pipelines {
		if pipeline.Name == "" {
			return errors.New("query planning pipeline name cannot be empty")
		}
		if pipelineNames[pipeline.Name] {
			return fmt.Errorf("duplicate query planning pipeline name found: %s", pipeline.Name)
		}
		pipelineNames[pipeline.Name] = true
		if len(pipeline.Stages) == 0 {
			return fmt.Errorf("query planning pipeline '%s' must have at least one stage", pipeline.Name)
		}
	}

	return nil
}

// QueryPlanningPipeline represents a sequence of stages for query processing.
// This is a placeholder for future implementation.
type QueryPlanningPipeline struct {
	Name   string   `json:"name" yaml:"name"`     // Name of the pipeline
	Stages []string `json:"stages" yaml:"stages"` // Example: "tokenize", "lowercase", "remove_stopwords", "synonym_expansion"
}
