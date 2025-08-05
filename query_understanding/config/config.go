package config

// IndexSchema represents the configuration for an index schema.
type IndexSchema struct {
	Name    string            `yaml:"name"`
	Fields  []SchemaField     `yaml:"fields"`
	Options map[string]string `yaml:"options"`
}

// SchemaField represents a field within an index schema.
type SchemaField struct {
	Name    string `yaml:"name"`
	Type    string `yaml:"type"`
	Indexed bool   `yaml:"indexed"`
	Stored  bool   `yaml:"stored"`
}

// ComputedField represents the configuration for a computed field.
type ComputedField struct {
	Name       string `yaml:"name"`
	Expression string `yaml:"expression"`
	Type       string `yaml:"type"`
}

// QueryPlanningPipeline represents the configuration for a query planning pipeline.
type QueryPlanningPipeline struct {
	Name    string   `yaml:"name"`
	Steps   []string `yaml:"steps"`
	Enabled bool     `yaml:"enabled"`
}

// Configuration is the root structure for the entire service configuration.
type Configuration struct {
	IndexSchemas           []IndexSchema           `yaml:"indexSchemas"`
	ComputedFields         []ComputedField         `yaml:"computedFields"`
	QueryPlanningPipelines []QueryPlanningPipeline `yaml:"queryPlanningPipelines"`
}
