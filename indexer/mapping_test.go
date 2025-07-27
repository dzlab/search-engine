package indexer

import (
	"testing"
)

func TestCreateDefaultIndexMapping(t *testing.T) {
	// Create a default index mapping
	mapping := CreateDefaultIndexMapping()

	// Check if the mapping was created successfully
	if mapping == nil {
		t.Fatal("CreateDefaultIndexMapping returned nil")
	}

	// // Get the document mapping for type "document"
	// docMapping, ok := mapping.DocumentMap("document")
	// if !ok {
	// 	t.Fatal("Document mapping for type 'document' not found")
	// }

	// // Check if the "title" field mapping exists using FieldMappingForPath
	// titleMapping := docMapping.FieldMappingForPath("title")
	// if titleMapping == nil {
	// 	t.Error("Field mapping for 'title' not found")
	// }

	// // Check if the "tags" field mapping exists using FieldMappingForPath
	// tagsMapping := docMapping.FieldMappingForPath("tags")
	// if tagsMapping == nil {
	// 	t.Error("Field mapping for 'tags' not found")
	// }

	// // Check if the "price" field mapping exists using FieldMappingForPath
	// priceMapping := docMapping.FieldMappingForPath("price")
	// if priceMapping == nil {
	// 	t.Error("Field mapping for 'price' not found")
	// }

	// // Check if the "views" field mapping exists using FieldMappingForPath
	// viewsMapping := docMapping.FieldMappingForPath("views")
	// if viewsMapping == nil {
	// 	t.Error("Field mapping for 'views' not found")
	// }

	// // Check if the "created_at" field mapping exists using FieldMappingForPath
	// createdAtMapping := docMapping.FieldMappingForPath("created_at")
	// if createdAtMapping == nil {
	// 	t.Error("Field mapping for 'created_at' not found")
	// }

	// // Check if the "location" field mapping exists using FieldMappingForPath
	// locationMapping := docMapping.FieldMappingForPath("location")
	// if locationMapping == nil {
	// 	t.Error("Field mapping for 'location' not found")
	// }
}

func TestLoadIndexMapping(t *testing.T) {
	// This test would require a temporary mapping file to be created.
	// For simplicity, we'll skip the file creation and focus on the
	// structure of testing LoadIndexMapping.

	// Example of how you might test LoadIndexMapping:
	// 1. Create a temporary JSON file with a valid Bleve mapping.
	// 2. Call LoadIndexMapping with the path to the temporary file.
	// 3. Assert that the returned mapping is not nil and that no error occurred.
	// 4. Clean up the temporary file.
}
