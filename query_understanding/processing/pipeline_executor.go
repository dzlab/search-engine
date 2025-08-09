package processing

import (
	"fmt"

	"query_understanding"
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
func (pe *PipelineExecutor) ExecutePipeline(pipelineName string, rawQuery string, config *query_understanding.IndexConfiguration) (string, error) {
	var pipeline *query_understanding.QueryPlanningPipeline
	for i := range config.Pipelines {
		if config.Pipelines[i].Name == pipelineName {
			pipeline = &config.Pipelines[i]
			break
		}
	}

	if pipeline == nil {
		return "", fmt.Errorf("query planning pipeline '%s' not found in configuration", pipelineName)
	}

	currentQuery := rawQuery
	for _, stageName := range pipeline.Stages {
		stage, found := pe.registry.Get(stageName)
		if !found {
			return "", fmt.Errorf("query stage '%s' not found in registry for pipeline '%s'", stageName, pipelineName)
		}

		// Placeholder for stage-specific configuration. For now, passing an empty map.
		// In future, configuration could be resolved from config.yaml based on stageName.
		processedQuery, err := stage.Process(currentQuery, make(map[string]interface{}))
		if err != nil {
			return "", fmt.Errorf("failed to execute stage '%s' in pipeline '%s': %w", stageName, pipelineName, err)
		}
		currentQuery = processedQuery
	}

	return currentQuery, nil
}
