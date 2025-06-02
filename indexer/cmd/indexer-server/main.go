package main

import (
	"log"
	"net/http"

	"indexer" // Import the local indexer package
)

func main() {
	log.Println("Starting Indexer service...")

	// Configuration for index path and storage directory
	indexPath := "/tmp/data/bleve_index"        // Directory where Bleve stores its index files
	storageDir := "/tmp/data/uploaded_segments" // Directory simulating S3 storage for segments

	// Initialize the local file storage implementation
	storage, err := indexer.NewLocalFileStorage(storageDir) // Use the New function from the indexer package
	if err != nil {
		log.Fatalf("Failed to initialize local file storage: %v", err)
	}
	log.Printf("Local file storage initialized at %s", storageDir)

	// Initialize the Indexer service
	indexer, err := indexer.NewIndexer(indexPath, storage) // Use the New function from the indexer package
	if err != nil {
		log.Fatalf("Failed to initialize Indexer: %v", err)
	}
	log.Println("Indexer service initialized.")

	// Set up HTTP endpoints for receiving indexing requests
	// The handlers are now methods on the indexer instance
	http.HandleFunc("/index", indexer.HandleIndexRequest)   // Use the public Handle methods
	http.HandleFunc("/delete", indexer.HandleDeleteRequest) // Use the public Handle methods
	http.HandleFunc("/commit", indexer.HandleCommitRequest) // Use the public Handle methods

	// Start the HTTP server
	listenAddr := ":8081" // Default port for the Indexer
	log.Printf("Indexer service listening on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}
