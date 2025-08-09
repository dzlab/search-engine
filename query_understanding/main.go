package query_understanding

import (
	"errors"
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v2"

	"query_understanding/processing"
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

// QueryPlanningPipeline represents a sequence of stages for query processing.
type QueryPlanningPipeline struct {
	Name   string   `json:"name" yaml:"name"`     // Name of the pipeline
	Stages []string `json:"stages" yaml:"stages"` // Example: "tokenize", "lowercase", "remove_stopwords", "synonym_expansion"
}

// IndexConfiguration holds the complete configuration for an index, including fields, computed fields, and pipelines.
type IndexConfiguration struct {
	IndexFields    []IndexField            `json:"index_fields" yaml:"index_fields"`
	ComputedFields []ComputedField         `json:"computed_fields" yaml:"computed_fields"`
	Pipelines      []QueryPlanningPipeline `json:"pipelines" yaml:"pipelines"`
}

// stopwordsConfig is a helper struct to unmarshal the stopwords YAML file.
type stopwordsConfig struct {
	Stopwords []string `yaml:"stopwords"`
}

var (
	stageRegistry    *processing.StageRegistry
	pipelineExecutor *processing.PipelineExecutor
	defaultStopwords []string
)

// init initializes the query understanding service components.
func init() {
	stageRegistry = processing.NewStageRegistry()

	// Register core query processing stages
	if err := stageRegistry.Register("lowercase", &processing.LowerCaseStage{}); err != nil {
		log.Fatalf("Failed to register lowercase stage: %v", err)
	}
	if err := stageRegistry.Register("tokenize", &processing.TokenizeStage{}); err != nil {
		log.Fatalf("Failed to register tokenize stage: %v", err)
	}

	// Load default stopwords
	stopwordsFilePath := "search-engine/query_understanding/config/default_stopwords.yaml"
	data, err := os.ReadFile(stopwordsFilePath)
	if err != nil {
		log.Fatalf("Failed to read stopwords file %s: %v", stopwordsFilePath, err)
	}

	var swConfig stopwordsConfig
	if err := yaml.Unmarshal(data, &swConfig); err != nil {
		log.Fatalf("Failed to unmarshal stopwords file %s: %v", stopwordsFilePath, err)
	}
	defaultStopwords = swConfig.Stopwords

	// Register RemoveStopwordsStage
	// Note: The stopwords are passed as config during pipeline execution if needed,
	//       but here we simply register the stage itself.
	if err := stageRegistry.Register("remove_stopwords", &processing.RemoveStopwordsStage{}); err != nil {
		log.Fatalf("Failed to register remove_stopwords stage: %v", err)
	}

	if err := stageRegistry.Register("synonym_expansion", &processing.SynonymExpansionStage{}); err != nil {
		log.Fatalf("Failed to register synonym_expansion stage: %v", err)
	}

	pipelineExecutor = processing.NewPipelineExecutor(stageRegistry)
}

// LoadIndexConfiguration loads index configuration from a YAML file.
func LoadIndexConfiguration(filePath string) (*IndexConfiguration, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("configuration file not found: %s", filePath)
	}

	data, err := os.ReadFile(filePath)
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

// ProcessClientQuery is the main entry point for processing a raw client query.
// It takes the raw query string and the specific index configuration,
// then processes it through the "default_pipeline" or a pipeline specified by the configuration.
func ProcessClientQuery(rawQuery string, config *IndexConfiguration) (string, error) {
	// For simplicity, we'll assume a "default_pipeline" exists and use it.
	// In a more complex scenario, the pipeline to use could be determined by
	// domain-specific rules or an explicit parameter in the client query.

	// Find the pipeline named "default_pipeline"
	pipelineName := "default_pipeline"
	var defaultPipeline *QueryPlanningPipeline
	for _, p := range config.Pipelines {
		if p.Name == pipelineName {
			defaultPipeline = &p
			break
		}
	}

	if defaultPipeline == nil {
		return "", fmt.Errorf("default_pipeline not found in the provided index configuration")
	}

	// Prepare stage-specific configurations.
	// For RemoveStopwordsStage, we need to pass the stopwords list.
	stageConfigs := make(map[string]map[string]interface{})
	stageConfigs["remove_stopwords"] = map[string]interface{}{
		"stopwords": defaultStopwords,
	}

	currentQuery := rawQuery
	for _, stageName := range defaultPipeline.Stages {
		stage, found := stageRegistry.Get(stageName)
		if !found {
			return "", fmt.Errorf("query stage '%s' not found in registry for pipeline '%s'", stageName, pipelineName)
		}

		// Get stage-specific config, if any
		configForStage := stageConfigs[stageName]
		if configForStage == nil {
			configForStage = make(map[string]interface{}) // Ensure it's not nil
		}

		processedQuery, err := stage.Process(currentQuery, configForStage)
		if err != nil {
			return "", fmt.Errorf("failed to execute stage '%s' in pipeline '%s': %w", stageName, pipelineName, err)
		}
		currentQuery = processedQuery
	}

	return currentQuery, nil
}
