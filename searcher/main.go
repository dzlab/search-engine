package main

import (
	"context"
	"log"

	"github.com/gin-gonic/gin"
)

const (
	port = ":8081" // Port for the Searcher service API
)

func main() {
	// Initialize Searcher
	searcher, err := NewSearcher()
	if err != nil {
		log.Fatalf("Failed to initialize Searcher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start routine to update index segments
	go searcher.updateIndex(ctx)

	// Set up Gin router
	router := gin.Default()
	router.GET("/search", searcher.SearchHandler)

	log.Printf("Searcher Service started on port %s", port)
	if err := router.Run(port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
