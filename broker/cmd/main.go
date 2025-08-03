package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"broker"
)

// In a real application, these would be configured via environment variables or a config file.
const (
	defaultPort = "8080"
)

// MockQueryUnderstandingService is a simple mock implementation for demonstration.
type MockQueryUnderstandingService struct{}

func (m *MockQueryUnderstandingService) Process(ctx context.Context, rawQuery broker.RawQuery) (broker.StructuredQuery, error) {
	log.Printf("MockQueryUnderstandingService: Processing raw query: \"%s\"", rawQuery)
	// For simplicity, let's just split the raw query into keywords.
	// In a real scenario, this would involve NLP, entity recognition, etc.
	keywords := []string{string(rawQuery)} // Treat the whole raw query as one keyword for now.
	return broker.StructuredQuery{Keywords: keywords}, nil
}

// MockSearcher is a simple mock implementation for demonstration.
type MockSearcher struct {
	ID      string
	ShardID int
}

func (m *MockSearcher) Search(ctx context.Context, query broker.StructuredQuery) ([]broker.SearchResult, error) {
	log.Printf("MockSearcher %s (Shard %d): Searching for keywords: %v", m.ID, m.ShardID, query.Keywords)
	results := []broker.SearchResult{}
	// Simulate some search results based on keywords
	for _, keyword := range query.Keywords {
		results = append(results, broker.SearchResult{
			ID:    fmt.Sprintf("%s-doc1-%s-%d", m.ID, keyword, m.ShardID), // Include ShardID in ID for better differentiation
			Title: fmt.Sprintf("Result for \"%s\" from Searcher %s", keyword, m.ID),
			URL:   fmt.Sprintf("http://example.com/%s/%s", m.ID, keyword),
			Score: 0.9,
		})
		results = append(results, broker.SearchResult{
			ID:    fmt.Sprintf("%s-doc2-%s-%d", m.ID, keyword, m.ShardID), // Include ShardID in ID for better differentiation
			Title: fmt.Sprintf("Another result for \"%s\" from Searcher %s", keyword, m.ID),
			URL:   fmt.Sprintf("http://example.com/%s/another/%s", m.ID, keyword),
			Score: 0.8,
		})
	}
	return results, nil
}

func (m *MockSearcher) GetShardID() int {
	return m.ShardID
}

// Ensure MockSearcher implements the Searcher interface
var _ broker.Searcher = (*MockSearcher)(nil)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	quService := &MockQueryUnderstandingService{}

	// Create a few mock searchers to simulate sharding
	searchers := []broker.Searcher{
		&MockSearcher{ID: "searcher-1", ShardID: 0},
		&MockSearcher{ID: "searcher-2", ShardID: 1},
		&MockSearcher{ID: "searcher-3", ShardID: 0}, // Another searcher for shard 0
		&MockSearcher{ID: "searcher-4", ShardID: 1}, // Another searcher for shard 1
	}

	// Initialize the broker
	b := broker.NewBroker(quService, searchers)

	// Define the HTTP handler for search queries
	http.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		queryParam := r.URL.Query().Get("q")
		if queryParam == "" {
			http.Error(w, "Missing 'q' query parameter", http.StatusBadRequest)
			return
		}

		log.Printf("Received raw query: \"%s\"", queryParam)

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		results, err := b.Search(ctx, broker.RawQuery(queryParam))
		if err != nil {
			log.Printf("Broker search failed: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(results); err != nil {
			log.Printf("Failed to encode response: %v", err)
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		}
	})

	log.Printf("Broker service starting on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
