package query_understanding

import (
	"fmt"
	"log"
	"os"

	"query_understanding/config"
	"query_understanding/processing"

	"gopkg.in/yaml.v2"
)

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
	stopwordsFilePath := "config/default_stopwords.yaml"
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

// LoadConfiguration loads the main service configuration from a YAML file.
func LoadConfiguration(filePath string) (*config.Configuration, error) {
	cfg, err := config.LoadConfig(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load main configuration: %w", err)
	}
	return cfg, nil
}

// ProcessClientQuery is the main entry point for processing a raw client query.
// It takes the raw query string and the specific service configuration,
// then processes it through the "default_pipeline" or a pipeline specified by the configuration.
func ProcessClientQuery(rawQuery string, cfg *config.Configuration) (string, error) {
	pipelineName := "default_pipeline" // For simplicity, assume default_pipeline

	var defaultPipeline *config.QueryPlanningPipeline
	for i := range cfg.QueryPlanningPipelines {
		if cfg.QueryPlanningPipelines[i].Name == pipelineName {
			defaultPipeline = &cfg.QueryPlanningPipelines[i]
			break
		}
	}

	if defaultPipeline == nil {
		return "", fmt.Errorf("query planning pipeline '%s' not found in the provided configuration", pipelineName)
	}

	// Prepare stage-specific configurations.
	stageConfigs := make(map[string]map[string]interface{})
	stageConfigs["remove_stopwords"] = map[string]interface{}{
		"stopwords": defaultStopwords,
	}

	// Execute the pipeline using the PipelineExecutor
	processedQuery, err := pipelineExecutor.ExecutePipeline(defaultPipeline, rawQuery, stageConfigs)
	if err != nil {
		return "", fmt.Errorf("failed to process query with pipeline '%s': %w", pipelineName, err)
	}

	return processedQuery, nil
}
