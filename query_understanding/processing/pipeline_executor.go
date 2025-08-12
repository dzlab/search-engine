package processing

import (
	"fmt"

	"query_understanding/config"
)

// PipelineExecutor is responsible for executing a sequence of query processing stages.
type PipelineExecutor struct {
	registry *StageRegistry
}

// NewPipelineExecutor creates a new PipelineExecutor with the given StageRegistry.
func NewPipelineExecutor(registry *StageRegistry) *PipelineExecutor {
	return &PipelineExecutor{
		registry: registry,
	}
}

// ExecutePipeline processes a raw query string through a specified query planning pipeline.
// It retrieves the pipeline definition from the provided IndexConfiguration and applies
// each stage in sequence.
func (pe *PipelineExecutor) ExecutePipeline(pipeline *config.QueryPlanningPipeline, rawQuery string, stageConfigs map[string]map[string]interface{}) (string, error) {
	if pipeline == nil {
		return "", fmt.Errorf("query planning pipeline cannot be nil")
	}

	currentQuery := rawQuery
	for _, stageName := range pipeline.Steps {
		stage, found := pe.registry.Get(stageName)
		if !found {
			return "", fmt.Errorf("query stage '%s' not found in registry for pipeline '%s'", stageName, pipeline.Name)
		}

		configForStage := stageConfigs[stageName]
		if configForStage == nil {
			configForStage = make(map[string]interface{}) // Ensure it's not nil
		}

		processedQuery, err := stage.Process(currentQuery, configForStage)
		if err != nil {
			return "", fmt.Errorf("failed to execute stage '%s' in pipeline '%s': %w", stageName, pipeline.Name, err)
		}
		currentQuery = processedQuery
	}

	return currentQuery, nil
}
