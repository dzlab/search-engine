package query_understanding

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
	FieldName    string   `json:"field_name"`
	DataType     DataType `json:"data_type"`
	Indexed      bool     `json:"indexed"`
	Stored       bool     `json:"stored"`
	Sortable     bool     `json:"sortable"`
	Aggregatable bool     `json:"aggregatable"`
	Analyzer     string   `json:"analyzer"` // e.g., "standard", "keyword", "whitespace"
}

// ComputedField defines a field whose value is derived from an expression
// based on other fields or external data.
type ComputedField struct {
	FieldName  string `json:"field_name"`
	Expression string `json:"expression"` // e.g., "first_name + ' ' + last_name"
}

// QueryPlanningPipeline represents a sequence of stages for query processing.
// This is a placeholder for future implementation.
type QueryPlanningPipeline struct {
	Stages []string // Example: "tokenize", "lowercase", "remove_stopwords", "synonym_expansion"
}
