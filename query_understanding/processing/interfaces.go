package processing

// QueryStage defines the interface for a single stage in the query processing pipeline.
// Each stage takes a query string and a map of configuration parameters, processes it,
// and returns the modified query string or an error.
type QueryStage interface {
	Process(query string, config map[string]interface{}) (string, error)
}
