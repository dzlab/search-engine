package searcher

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/gin-gonic/gin"
)

const (
	segmentsDir = "./segments" // Directory to store downloaded segments
)

// Searcher represents the search service
type Searcher struct {
	index bleve.Index
}

// NewSearcher initializes a new Searcher instance
func NewSearcher() (*Searcher, error) {
	// For demonstration, we'll create a new in-memory index.
	// In a real scenario, this would involve loading/opening an existing Lucene index
	// potentially from downloaded segments.
	mapping := bleve.NewIndexMapping()
	index, err := bleve.NewMemOnly(mapping) // Using in-memory for statelessness example
	if err != nil {
		return nil, fmt.Errorf("failed to create Bleve index: %w", err)
	}
	return &Searcher{index: index}, nil
}

// downloadSegments simulates downloading index segments from a storage layer.
// In a real implementation, this would involve interacting with S3, GCS, etc.
func (s *Searcher) downloadSegments(ctx context.Context) error {
	log.Println("Simulating downloading latest index segments...")
	// Ensure segments directory exists
	if err := os.MkdirAll(segmentsDir, 0755); err != nil {
		return fmt.Errorf("failed to create segments directory: %w", err)
	}

	// Simulate downloading a segment file
	segmentFilePath := filepath.Join(segmentsDir, fmt.Sprintf("segment_%d.txt", time.Now().Unix()))
	file, err := os.Create(segmentFilePath)
	if err != nil {
		return fmt.Errorf("failed to create dummy segment file: %w", err)
	}
	file.WriteString("This is a dummy index segment content.")
	file.Close()

	log.Printf("Dummy segment downloaded to: %s\n", segmentFilePath)

	// In a real Lucene implementation, you would then load these segments
	// into a Directory and open an IndexReader.
	return nil
}

// updateIndex periodically checks for and downloads new segments.
func (s *Searcher) UpdateIndex(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute) // Check for new segments every 5 minutes
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("Checking for new index segments...")
			if err := s.downloadSegments(ctx); err != nil {
				log.Printf("Error downloading segments: %v\n", err)
			}
			// After downloading, you would typically rebuild/reopen your Lucene index
			// with the new segments.
		case <-ctx.Done():
			log.Println("Stopping index update routine.")
			return
		}
	}
}

// SearchHandler handles search queries from the Broker.
func (s *Searcher) SearchHandler(c *gin.Context) {
	query := c.Query("q")
	if query == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "query parameter 'q' is required"})
		return
	}

	// In a real Lucene implementation, you would parse the query,
	// execute it against your Lucene index, and format results.
	// For this Bleve example, we'll perform a simple query.
	searchQuery := bleve.NewMatchQuery(query)
	searchRequest := bleve.NewSearchRequest(searchQuery)
	searchResults, err := s.index.Search(searchRequest)
	if err != nil {
		log.Printf("Error executing search: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to perform search"})
		return
	}

	// Simulate adding some dummy documents for search to work with Bleve
	if searchResults.Total == 0 {
		// Only index if no documents found (first run)
		log.Println("No documents in index, adding dummy document...")
		docID := "doc1"
		data := map[string]interface{}{
			"text":    "This is a sample document for testing the searcher service.",
			"another": "another field content",
		}
		if err := s.index.Index(docID, data); err != nil {
			log.Printf("Error indexing dummy document: %v\n", err)
		} else {
			log.Println("Dummy document indexed.")
			// Re-run search after indexing
			searchResults, err = s.index.Search(searchRequest)
			if err != nil {
				log.Printf("Error re-executing search after indexing: %v\n", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to perform search after indexing"})
				return
			}
		}
	}

	log.Printf("Search query: '%s', Results: %d hits\n", query, searchResults.Total)
	c.JSON(http.StatusOK, gin.H{
		"query":      query,
		"results":    searchResults.Hits,
		"total_hits": searchResults.Total,
	})
}
