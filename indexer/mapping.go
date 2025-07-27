package indexer

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
)

// LoadIndexMapping loads a Bleve index mapping from a JSON file.
func LoadIndexMapping(filePath string) (mapping.IndexMapping, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read mapping file %s: %w", filePath, err)
	}

	var indexMapping mapping.IndexMapping
	if err := json.Unmarshal(data, &indexMapping); err != nil {
		return nil, fmt.Errorf("failed to unmarshal mapping JSON from %s: %w", filePath, err)
	}

	return indexMapping, nil
}

// CreateDefaultIndexMapping creates a default Bleve index mapping.
// This can be used if no external mapping file is provided or as a fallback.
func CreateDefaultIndexMapping() *mapping.IndexMappingImpl {
	// Use bleve.NewIndexMapping to create a new index mapping.
	// The argument is the default type name, often empty if no specific default is set.
	indexMapping := bleve.NewIndexMapping()

	// Default text field mapping (e.g., for 'content' or generic text)
	textFieldMapping := bleve.NewTextFieldMapping()
	textFieldMapping.Analyzer = "en" // English analyzer
	textFieldMapping.Store = true    // Store term vectors for highlighting

	// Create a default document mapping
	docMapping := bleve.NewDocumentMapping()
	docMapping.AddFieldMapping(textFieldMapping)

	// Example: Add a specific mapping for a 'title' field
	titleFieldMapping := bleve.NewTextFieldMapping()
	titleFieldMapping.Analyzer = "en"
	// The Boost field is not directly available on FieldMapping.
	// Boost can be set when creating a SearchRequest or by composing mappings.
	titleFieldMapping.Store = true
	docMapping.AddFieldMappingsAt("title", titleFieldMapping)

	// Example: Keyword field mapping (for exact matches, e.g., tags, categories)
	keywordFieldMapping := bleve.NewKeywordFieldMapping()
	keywordFieldMapping.Store = true
	docMapping.AddFieldMappingsAt("tags", keywordFieldMapping)
	docMapping.AddFieldMappingsAt("category", keywordFieldMapping)

	// Example: Numeric field mapping (for numbers like price, views, etc.)
	numericFieldMapping := bleve.NewNumericFieldMapping()
	numericFieldMapping.Store = true
	docMapping.AddFieldMappingsAt("price", numericFieldMapping)
	docMapping.AddFieldMappingsAt("views", numericFieldMapping)

	// Example: Date field mapping
	dateFieldMapping := bleve.NewDateTimeFieldMapping()
	dateFieldMapping.Store = true
	docMapping.AddFieldMappingsAt("created_at", dateFieldMapping)

	// Example: GeoPoint field mapping (for latitude/longitude)
	geoFieldMapping := bleve.NewGeoPointFieldMapping()
	geoFieldMapping.Store = true
	docMapping.AddFieldMappingsAt("location", geoFieldMapping)

	// Add the document mapping to the index mapping with the type name "document"
	indexMapping.AddDocumentMapping("document", docMapping)

	// Configure default analyzer and tokenizer, etc.
	// You can define custom analyzers here as well.
	// mapping.AddAnalyzer("myCustomAnalyzer", myCustomAnalyzer)

	// Consider dynamic mapping for fields not explicitly defined
	// Set DefaultMapDynamic to true to allow dynamic mapping of undeclared fields
	// Note: DefaultMapDynamic is a method on IndexMappingImpl, not directly on IndexMapping.
	// Since we are returning IndexMappingImpl, we can call it.
	// indexMapping.SetDynamic(true)

	return indexMapping
}
