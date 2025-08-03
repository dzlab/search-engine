package broker

import (
	"context"
	"sync"
)

// RawQuery represents the initial query string from the user.
type RawQuery string

// StructuredQuery represents the query after being processed by the Query Understanding Service.
// This struct should contain fields that are suitable for searching, e.g., keywords, filters, etc.
type StructuredQuery struct {
	Keywords []string
	Filters  map[string]string
	// Add other relevant fields as needed (e.g., intent, entities)
}

// SearchResult represents a single search result item.
type SearchResult struct {
	ID    string
	Title string
	URL   string
	Score float64
	// Add other relevant fields as needed (e.g., snippet, source)
}

// QueryUnderstandingService defines the interface for the service that processes raw queries
// into structured queries.
type QueryUnderstandingService interface {
	Process(ctx context.Context, rawQuery RawQuery) (StructuredQuery, error)
}

// Searcher defines the interface for a single searcher instance.
// A searcher is responsible for performing a search against its own index and returning results.
type Searcher interface {
	Search(ctx context.Context, query StructuredQuery) ([]SearchResult, error)
}

// Broker is the service that acts as an entry point for user queries,
// orchestrates calls to other services, and aggregates results.
type Broker struct {
	queryUnderstanding QueryUnderstandingService
	searchers          []Searcher
}

// NewBroker creates a new Broker instance with the given QueryUnderstandingService
// and a slice of Searcher instances.
func NewBroker(quService QueryUnderstandingService, searchers []Searcher) *Broker {
	return &Broker{
		queryUnderstanding: quService,
		searchers:          searchers,
	}
}

// Search receives a raw query, communicates with the Query Understanding Service,
// fans out the structured query to multiple Searcher instances, and merges their results.
func (b *Broker) Search(ctx context.Context, rawQuery RawQuery) ([]SearchResult, error) {
	// 1. Communicate with the Query Understanding Service to get a structured query.
	structuredQuery, err := b.queryUnderstanding.Process(ctx, rawQuery)
	if err != nil {
		return nil, err
	}

	// 2. Fan out queries to multiple Searcher instances concurrently.
	var (
		mu         sync.Mutex // Mutex to protect allResults during concurrent writes
		allResults []SearchResult
		wg         sync.WaitGroup                       // WaitGroup to wait for all searchers to complete
		errChan    = make(chan error, len(b.searchers)) // Channel to collect errors from searchers
	)

	for _, searcher := range b.searchers {
		wg.Add(1)
		go func(s Searcher) {
			defer wg.Done()
			results, searchErr := s.Search(ctx, structuredQuery)
			if searchErr != nil {
				// If a searcher fails, we send the error to the channel.
				// The broker can still return results from other successful searchers.
				errChan <- searchErr
				return
			}

			// Lock to safely append results from this searcher to the shared slice.
			mu.Lock()
			allResults = append(allResults, results...)
			mu.Unlock()
		}(searcher)
	}

	// Wait for all searcher goroutines to finish.
	wg.Wait()
	close(errChan) // Close the error channel once all goroutines are done sending.

	// Check if any searcher encountered an error.
	// In a production system, you might want to aggregate these errors, log them,
	// or return a multi-error type. For simplicity, we just peek at the channel.
	select {
	case searcherErr := <-errChan:
		// An error occurred in at least one searcher.
		// For this implementation, we acknowledge the error but proceed with available results.
		// In a real scenario, you might log this error or decide on a different error handling strategy.
		_ = searcherErr
	default:
		// No errors were reported by any searcher.
	}

	// 3. Merge results from Searchers.
	// Currently, this is a simple concatenation. In a more advanced system,
	// this step would involve sophisticated logic such as:
	// - De-duplication of results (if multiple searchers return the same document).
	// - Re-ranking results based on a global scoring model, freshness, personalization, etc.
	// - Pagination or result limiting.
	// - Aggregation of facets or other metadata.

	return allResults, nil
}
