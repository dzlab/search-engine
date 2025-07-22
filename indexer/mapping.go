package indexer

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
)

// LoadIndexMapping loads a Bleve index mapping from a JSON file.
func LoadIndexMapping(filePath string) (*mapping.IndexMapping, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read mapping file %s: %w", filePath, err)
	}

	var indexMapping mapping.IndexMapping
	if err := json.Unmarshal(data, &indexMapping); err != nil {
		return nil, fmt.Errorf("failed to unmarshal mapping JSON from %s: %w", filePath, err)
	}

	return &indexMapping, nil
}

// CreateDefaultIndexMapping creates a default Bleve index mapping.
// This can be used if no external mapping file is provided or as a fallback.
func CreateDefaultIndexMapping() *mapping.IndexMapping {
	mapping := bleve.NewIndexMapping()

	// Default text field mapping (e.g., for 'content' or generic text)
	textFieldMapping := bleve.NewTextFieldMapping()
	textFieldMapping.Analyzer = "en" // English analyzer
	textFieldMapping.Store = true    // Store term vectors for highlighting
	mapping.DefaultMapping.AddFieldMapping(textFieldMapping)

	// Example: Add a specific mapping for a 'title' field
	titleFieldMapping := bleve.NewTextFieldMapping()
	titleFieldMapping.Analyzer = "en"
	titleFieldMapping.Boost = 2.0 // Give title a higher boost
	titleFieldMapping.Store = true
	mapping.AddDocumentMapping("document", bleve.NewDocumentMapping()) // Define a document type
	mapping.DocumentMapping("document").AddFieldMappingsAt("title", titleFieldMapping)

	// Example: Keyword field mapping (for exact matches, e.g., tags, categories)
	keywordFieldMapping := bleve.NewKeywordFieldMapping()
	keywordFieldMapping.Store = true
	mapping.DocumentMapping("document").AddFieldMappingsAt("tags", keywordFieldMapping)
	mapping.DocumentMapping("document").AddFieldMappingsAt("category", keywordFieldMapping)

	// Example: Numeric field mapping (for numbers like price, views, etc.)
	numericFieldMapping := bleve.NewNumericFieldMapping()
	numericFieldMapping.Store = true
	mapping.DocumentMapping("document").AddFieldMappingsAt("price", numericFieldMapping)
	mapping.DocumentMapping("document").AddFieldMappingsAt("views", numericFieldMapping)

	// Example: Date field mapping
	dateFieldMapping := bleve.NewDateTimeFieldMapping()
	dateFieldMapping.Store = true
	mapping.DocumentMapping("document").AddFieldMappingsAt("created_at", dateFieldMapping)

	// Example: GeoPoint field mapping (for latitude/longitude)
	geoFieldMapping := bleve.NewGeoPointFieldMapping()
	geoFieldMapping.Store = true
	mapping.DocumentMapping("document").AddFieldMappingsAt("location", geoFieldMapping)

	// Configure default analyzer and tokenizer, etc.
	// You can define custom analyzers here as well.
	// mapping.AddAnalyzer("myCustomAnalyzer", myCustomAnalyzer)

	// Consider dynamic mapping for fields not explicitly defined
	mapping.DefaultMapping.Dynamic = true // Set to true to allow dynamic mapping of undeclared fields

	return mapping
}
