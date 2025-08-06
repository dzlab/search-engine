package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Helper function to create a temporary config file
func createTempConfigFile(t *testing.T, content string) (string, func()) {
	tmpFile, err := os.CreateTemp("", "config_test_*.yaml")
	assert.NoError(t, err)

	_, err = tmpFile.WriteString(content)
	assert.NoError(t, err)
	tmpFile.Close()

	return tmpFile.Name(), func() { os.Remove(tmpFile.Name()) }
}

func TestLoadConfig_Success(t *testing.T) {
	validConfigYAML := `
index_schemas:
  - name: products
    fields:
      - name: id
        type: integer
      - name: name
        type: text
      - name: price
        type: float
  - name: users
    fields:
      - name: id
        type: integer
      - name: username
        type: string
computed_fields:
  - name: discounted_price
    expression: "price * 0.9"
    type: float
query_planning_pipelines:
  - name: default_pipeline
    steps:
      - "tokenize"
      - "normalize"
      - "identify_entities"
  - name: product_search_pipeline
    steps:
      - "tokenize"
      - "spell_check"
`
	filePath, cleanup := createTempConfigFile(t, validConfigYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, config)

	// Validate IndexSchemas
	assert.Len(t, config.IndexSchemas, 2)
	assert.Equal(t, "products", config.IndexSchemas[0].Name)
	assert.Len(t, config.IndexSchemas[0].Fields, 3)
	assert.Equal(t, "id", config.IndexSchemas[0].Fields[0].Name)
	assert.Equal(t, "integer", config.IndexSchemas[0].Fields[0].Type)

	// Validate ComputedFields
	assert.Len(t, config.ComputedFields, 1)
	assert.Equal(t, "discounted_price", config.ComputedFields[0].Name)
	assert.Equal(t, "price * 0.9", config.ComputedFields[0].Expression)
	assert.Equal(t, "float", config.ComputedFields[0].Type)

	// Validate QueryPlanningPipelines
	assert.Len(t, config.QueryPlanningPipelines, 2)
	assert.Equal(t, "default_pipeline", config.QueryPlanningPipelines[0].Name)
	assert.Len(t, config.QueryPlanningPipelines[0].Steps, 3)
	assert.Equal(t, "tokenize", config.QueryPlanningPipelines[0].Steps[0])
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	config, err := LoadConfig("/path/does/not/exist/config.yaml")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read configuration file")
	assert.Nil(t, config)
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	invalidYAML := `
index_schemas:
  - name: products
    fields:
      - name: id
        type: integer
      - name: name
        type: text
  invalid_key: [
`
	filePath, cleanup := createTempConfigFile(t, invalidYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal configuration")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_NoIndexSchemas(t *testing.T) {
	configYAML := `
index_schemas: []
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one index schema must be defined")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_EmptyIndexSchemaName(t *testing.T) {
	configYAML := `
index_schemas:
  - name: ""
    fields:
      - name: id
        type: integer
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index schema name cannot be empty")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_NoFieldsInIndexSchema(t *testing.T) {
	configYAML := `
index_schemas:
  - name: products
    fields: []
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index schema 'products' must define at least one field")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_EmptyFieldName(t *testing.T) {
	configYAML := `
index_schemas:
  - name: products
    fields:
      - name: ""
        type: integer
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "field name in schema 'products' cannot be empty")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_EmptyFieldType(t *testing.T) {
	configYAML := `
index_schemas:
  - name: products
    fields:
      - name: id
        type: ""
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "field 'id' in schema 'products' must have a type")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_UnsupportedFieldType(t *testing.T) {
	configYAML := `
index_schemas:
  - name: products
    fields:
      - name: id
        type: uuid
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "field 'id' in schema 'products' has an unsupported type 'uuid'")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_EmptyComputedFieldName(t *testing.T) {
	configYAML := `
index_schemas:
  - name: products
    fields:
      - name: id
        type: integer
computed_fields:
  - name: ""
    expression: "price * 0.9"
    type: float
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "computed field name cannot be empty")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_EmptyComputedFieldExpression(t *testing.T) {
	configYAML := `
index_schemas:
  - name: products
    fields:
      - name: id
        type: integer
computed_fields:
  - name: discounted_price
    expression: ""
    type: float
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "computed field 'discounted_price' must have an expression")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_EmptyComputedFieldType(t *testing.T) {
	configYAML := `
index_schemas:
  - name: products
    fields:
      - name: id
        type: integer
computed_fields:
  - name: discounted_price
    expression: "price * 0.9"
    type: ""
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "computed field 'discounted_price' must have a type")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_UnsupportedComputedFieldType(t *testing.T) {
	configYAML := `
index_schemas:
  - name: products
    fields:
      - name: id
        type: integer
computed_fields:
  - name: discounted_price
    expression: "price * 0.9"
    type: date
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "computed field 'discounted_price' has an unsupported type 'date'")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_EmptyPipelineName(t *testing.T) {
	configYAML := `
index_schemas:
  - name: products
    fields:
      - name: id
        type: integer
query_planning_pipelines:
  - name: ""
    steps:
      - "tokenize"
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query planning pipeline name cannot be empty")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_NoStepsInPipeline(t *testing.T) {
	configYAML := `
index_schemas:
  - name: products
    fields:
      - name: id
        type: integer
query_planning_pipelines:
  - name: default_pipeline
    steps: []
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query planning pipeline 'default_pipeline' must define at least one step")
	assert.Nil(t, config)
}

func TestLoadConfig_ValidationFailed_EmptyStepInPipeline(t *testing.T) {
	configYAML := `
index_schemas:
  - name: products
    fields:
      - name: id
        type: integer
query_planning_pipelines:
  - name: default_pipeline
    steps:
      - "tokenize"
      - ""
`
	filePath, cleanup := createTempConfigFile(t, configYAML)
	defer cleanup()

	config, err := LoadConfig(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query planning pipeline 'default_pipeline' contains an empty step")
	assert.Nil(t, config)
}

func TestValidateConfiguration_NilConfig(t *testing.T) {
	err := ValidateConfiguration(nil) // Assuming ValidateConfiguration is exported for testing
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "configuration cannot be nil")
}
