package processing

import (
	"fmt"
	"sync"
)

// StageRegistry manages the registration and retrieval of QueryStage implementations.
type StageRegistry struct {
	mu     sync.RWMutex
	stages map[string]QueryStage
}

// NewStageRegistry creates and returns a new, empty StageRegistry.
func NewStageRegistry() *StageRegistry {
	return &StageRegistry{
		stages: make(map[string]QueryStage),
	}
}

// Register adds a QueryStage implementation to the registry under a given name.
// It returns an error if a stage with the same name is already registered.
func (sr *StageRegistry) Register(name string, stage QueryStage) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if _, exists := sr.stages[name]; exists {
		return fmt.Errorf("query stage '%s' is already registered", name)
	}
	sr.stages[name] = stage
	return nil
}

// Get retrieves a QueryStage implementation by its registered name.
// It returns the stage and true if found, otherwise nil and false.
func (sr *StageRegistry) Get(name string) (QueryStage, bool) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	stage, found := sr.stages[name]
	return stage, found
}
