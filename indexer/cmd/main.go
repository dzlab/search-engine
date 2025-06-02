package main

import (
	"flag"
	"log"

	"indexer"
	"indexer/service"
)

func main() {
	var (
		indexPath  = flag.String("index-path", "/tmp/data/bleve_index", "Path to the Bleve index")
		storageDir = flag.String("storage-dir", "/tmp/data/uploaded_segments", "Directory for segment storage")
		listenAddr = flag.String("listen-addr", ":8081", "Address to listen on")
	)
	flag.Parse()

	log.Println("Starting Indexer service...")

	// Initialize local file storage
	storage, err := indexer.NewLocalFileStorage(*storageDir)
	if err != nil {
		log.Fatalf("Failed to initialize local file storage: %v", err)
	}
	log.Printf("Local file storage initialized at %s", *storageDir)

	// Initialize the Indexer service
	indexer, err := indexer.NewIndexer(*indexPath, storage)
	if err != nil {
		log.Fatalf("Failed to initialize Indexer: %v", err)
	}
	log.Println("Indexer service initialized.")

	// Create and start the web service
	ws := service.NewWebService(indexer, *listenAddr)
	if err := ws.Start(); err != nil {
		log.Fatalf("Failed to start web service: %v", err)
	}
}
