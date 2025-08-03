package broker

import (
	"context"
	"fmt" // For fmt.Errorf
	"log" // For log.Println
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
	GetShardID() int // Add method to retrieve the shard ID
}

// Broker is the service that acts as an entry point for user queries,
// orchestrates calls to other services, and aggregates results.
type Broker struct {
	queryUnderstanding QueryUnderstandingService
	searchersByShard   map[int][]Searcher // Group searchers by shard ID
}

// NewBroker creates a new Broker instance with the given QueryUnderstandingService
// and a slice of Searcher instances.
func NewBroker(quService QueryUnderstandingService, searchers []Searcher) *Broker {
	searchersByShard := make(map[int][]Searcher)
	for _, s := range searchers {
		shardID := s.GetShardID()
		searchersByShard[shardID] = append(searchersByShard[shardID], s)
	}
	return &Broker{
		queryUnderstanding: quService,
		searchersByShard:   searchersByShard,
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
		wg         sync.WaitGroup // WaitGroup to wait for all searchers to complete
	)

	// Determine target shards based on the structured query.
	// For simplicity, we'll hash the first keyword to a shard ID.
	// In a real system, this would be more complex, involving query planning
	// from the Query Understanding Service, or a more sophisticated routing table.
	var targetShardIDs []int
	if len(structuredQuery.Keywords) > 0 {
		// Get all available shard IDs from the map keys
		var availableShardIDs []int
		for shardID := range b.searchersByShard {
			availableShardIDs = append(availableShardIDs, shardID)
		}

		if len(availableShardIDs) > 0 {
			// A consistent hash function would be better in a real system.
			// For simplicity, we'll use a basic FNV-like hash modulo the number of distinct shards.
			// This assumes shard IDs are contiguous for this hashing scheme, or at least
			// we can map the hash result to an actual shard ID from `availableShardIDs`.
			hash := 0
			for _, r := range structuredQuery.Keywords[0] {
				hash = (hash*31 + int(r)) // Simple hash
			}
			if hash < 0 { // Handle potential negative hash if int overflows or for other reasons
				hash = -hash
			}
			targetShardIDs = append(targetShardIDs, availableShardIDs[hash%len(availableShardIDs)])
		} else {
			log.Println("No searchers configured for any shard.")
			return nil, fmt.Errorf("no searchers available")
		}
	} else {
		// If no keywords, query all shards or a default shard.
		// For now, let's query all shards if no specific keyword for sharding.
		for shardID := range b.searchersByShard {
			targetShardIDs = append(targetShardIDs, shardID)
		}
	}

	// Channel to collect errors from searchers, sized to the number of *potential* searchers
	totalTargetSearchers := 0
	for _, shardID := range targetShardIDs {
		totalTargetSearchers += len(b.searchersByShard[shardID])
	}
	errChan := make(chan error, totalTargetSearchers)

	for _, shardID := range targetShardIDs {
		if searchersInShard, ok := b.searchersByShard[shardID]; ok {
			for _, searcher := range searchersInShard {
				wg.Add(1)
				go func(s Searcher) {
					defer wg.Done()
					results, searchErr := s.Search(ctx, structuredQuery)
					if searchErr != nil {
						errChan <- searchErr
						return
					}

					mu.Lock()
					allResults = append(allResults, results...)
					mu.Unlock()
				}(searcher)
			}
		}
	}

	// Wait for all searcher goroutines to finish.
	wg.Wait()
	close(errChan) // Close the error channel once all goroutines are done sending.

	// Check if any searcher encountered an error.
	select {
	case searcherErr := <-errChan:
		// An error occurred in at least one searcher.
		// For this implementation, we acknowledge the error but proceed with available results.
		log.Printf("Warning: one or more searchers returned an error: %v", searcherErr)
	default:
		// No errors were reported by any searcher.
	}

	// 3. Merge and de-duplicate results from Searchers.
	// Initialize a map to keep track of seen result IDs for deduplication.
	seenIDs := make(map[string]struct{})
	deduplicatedResults := []SearchResult{}

	for _, result := range allResults {
		if _, seen := seenIDs[result.ID]; !seen {
			seenIDs[result.ID] = struct{}{}
			deduplicatedResults = append(deduplicatedResults, result)
		}
	}

	// In a more advanced system, this step would also involve:
	// - Re-ranking results based on a global scoring model, freshness, personalization, etc.
	// - Pagination or result limiting.
	// - Aggregation of facets or other metadata.

	return deduplicatedResults, nil
}
