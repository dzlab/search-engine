package broker

import (
	"context"
	"errors"
	"testing"
)

// MockQueryUnderstandingService
type MockQueryUnderstandingService struct {
	ProcessFunc func(ctx context.Context, rawQuery RawQuery) (StructuredQuery, error)
}

func (m *MockQueryUnderstandingService) Process(ctx context.Context, rawQuery RawQuery) (StructuredQuery, error) {
	if m.ProcessFunc != nil {
		return m.ProcessFunc(ctx, rawQuery)
	}
	return StructuredQuery{}, nil
}

// MockSearcher
type MockSearcher struct {
	ShardID    int
	SearchFunc func(ctx context.Context, query StructuredQuery) ([]SearchResult, error)
}

func (m *MockSearcher) Search(ctx context.Context, query StructuredQuery) ([]SearchResult, error) {
	if m.SearchFunc != nil {
		return m.SearchFunc(ctx, query)
	}
	return []SearchResult{}, nil
}

func (m *MockSearcher) GetShardID() int {
	return m.ShardID
}

func TestNewBroker(t *testing.T) {
	mockQU := &MockQueryUnderstandingService{}

	searcher1_shard0 := &MockSearcher{ShardID: 0}
	searcher2_shard0 := &MockSearcher{ShardID: 0}
	searcher3_shard1 := &MockSearcher{ShardID: 1}
	searcher4_shard1 := &MockSearcher{ShardID: 1}
	searcher5_shard2 := &MockSearcher{ShardID: 2}

	searchers := []Searcher{
		searcher1_shard0,
		searcher2_shard0,
		searcher3_shard1,
		searcher4_shard1,
		searcher5_shard2,
	}

	broker := NewBroker(mockQU, searchers)

	if broker == nil {
		t.Fatal("NewBroker returned nil")
	}
	if broker.queryUnderstanding != mockQU {
		t.Errorf("NewBroker did not set QueryUnderstandingService correctly")
	}

	// Verify searchers are grouped by shard ID correctly
	if len(broker.searchersByShard) != 3 {
		t.Errorf("Expected 3 shards, got %d", len(broker.searchersByShard))
	}

	if shard0Searchers, ok := broker.searchersByShard[0]; !ok || len(shard0Searchers) != 2 {
		t.Errorf("Expected 2 searchers in shard 0, got %d", len(shard0Searchers))
	}
	if shard1Searchers, ok := broker.searchersByShard[1]; !ok || len(shard1Searchers) != 2 {
		t.Errorf("Expected 2 searchers in shard 1, got %d", len(shard1Searchers))
	}
	if shard2Searchers, ok := broker.searchersByShard[2]; !ok || len(shard2Searchers) != 1 {
		t.Errorf("Expected 1 searcher in shard 2, got %d", len(shard2Searchers))
	}

	// Test case with no searchers
	brokerNoSearchers := NewBroker(mockQU, []Searcher{})
	if len(brokerNoSearchers.searchersByShard) != 0 {
		t.Errorf("Expected 0 shards when no searchers are provided, got %d", len(brokerNoSearchers.searchersByShard))
	}
}

func TestBroker_Search_Success(t *testing.T) {
	ctx := context.Background()
	rawQuery := RawQuery("test query")

	mockQU := &MockQueryUnderstandingService{
		ProcessFunc: func(_ context.Context, rq RawQuery) (StructuredQuery, error) {
			if rq != rawQuery {
				t.Errorf("Expected raw query %q, got %q", rawQuery, rq)
			}
			return StructuredQuery{Keywords: []string{"test"}}, nil
		},
	}

	// Only provide mockSearcher0 to ensure all queries hit it.
	mockSearcher0 := &MockSearcher{
		ShardID: 0,
		SearchFunc: func(_ context.Context, sq StructuredQuery) ([]SearchResult, error) {
			if len(sq.Keywords) == 0 || sq.Keywords[0] != "test" {
				t.Errorf("Expected structured query with keyword 'test', got %v", sq.Keywords)
			}
			return []SearchResult{
				{ID: "doc1", Title: "Result 1 Shard 0", URL: "url1", Score: 0.9},
				{ID: "doc2", Title: "Result 2 Shard 0", URL: "url2", Score: 0.8},
			}, nil
		},
	}

	searchers := []Searcher{mockSearcher0} // Only one searcher for determinism
	broker := NewBroker(mockQU, searchers)

	results, err := broker.Search(ctx, rawQuery)
	if err != nil {
		t.Fatalf("Broker.Search returned an error: %v", err)
	}

	expectedResultsCount := 2 // From mockSearcher0
	if len(results) != expectedResultsCount {
		t.Errorf("Expected %d results, got %d", expectedResultsCount, len(results))
	}

	foundDoc1 := false
	foundDoc2 := false
	for _, r := range results {
		if r.ID == "doc1" {
			foundDoc1 = true
		}
		if r.ID == "doc2" {
			foundDoc2 = true
		}
	}

	if !foundDoc1 || !foundDoc2 {
		t.Errorf("Missing expected results. Found doc1: %t, Found doc2: %t", foundDoc1, foundDoc2)
	}
}

func TestBroker_Search_QueryUnderstandingServiceError(t *testing.T) {
	ctx := context.Background()
	rawQuery := RawQuery("error query")
	expectedErr := errors.New("failed to process query")

	mockQU := &MockQueryUnderstandingService{
		ProcessFunc: func(_ context.Context, _ RawQuery) (StructuredQuery, error) {
			return StructuredQuery{}, expectedErr
		},
	}

	broker := NewBroker(mockQU, []Searcher{}) // No searchers needed for this test

	_, err := broker.Search(ctx, rawQuery)
	if err == nil {
		t.Fatal("Broker.Search did not return an error when QU service failed")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

func TestBroker_Search_SearcherError(t *testing.T) {
	ctx := context.Background()
	rawQuery := RawQuery("query with searcher error")
	expectedSearcherErr := errors.New("searcher failed")

	mockQU := &MockQueryUnderstandingService{
		ProcessFunc: func(_ context.Context, _ RawQuery) (StructuredQuery, error) {
			return StructuredQuery{Keywords: []string{"error"}}, nil
		},
	}

	mockSearcherWithError := &MockSearcher{
		ShardID: 0,
		SearchFunc: func(_ context.Context, _ StructuredQuery) ([]SearchResult, error) {
			return nil, expectedSearcherErr
		},
	}

	mockSearcherSuccess := &MockSearcher{
		ShardID: 0, // Same shard to ensure it's hit by the simple sharding
		SearchFunc: func(_ context.Context, _ StructuredQuery) ([]SearchResult, error) {
			return []SearchResult{{ID: "doc_ok", Title: "OK", URL: "ok", Score: 1.0}}, nil
		},
	}

	// For determinism in sharding, let's create a single shard containing both searchers.
	// The current hashing depends on `len(availableShardIDs)`, so if we have only shard 0,
	// any keyword will map to it.
	broker := NewBroker(mockQU, []Searcher{mockSearcherWithError, mockSearcherSuccess})

	results, err := broker.Search(ctx, rawQuery)
	// The current implementation logs the error but proceeds with available results,
	// so `err` should be `nil` here, but results should contain `doc_ok`.
	if err != nil {
		t.Fatalf("Broker.Search returned an unexpected error: %v", err)
	}

	// Verify that the successful searcher's results are present,
	// even though another searcher failed.
	if len(results) != 1 || results[0].ID != "doc_ok" {
		t.Errorf("Expected 1 result from successful searcher, got %d. Results: %+v", len(results), results)
	}
}

func TestBroker_Search_Deduplication(t *testing.T) {
	ctx := context.Background()
	rawQuery := RawQuery("dedup query")

	mockQU := &MockQueryUnderstandingService{
		ProcessFunc: func(_ context.Context, _ RawQuery) (StructuredQuery, error) {
			return StructuredQuery{Keywords: []string{"dedup"}}, nil
		},
	}

	// Searcher 1 provides some results including a duplicate
	mockSearcher1 := &MockSearcher{
		ShardID: 0,
		SearchFunc: func(_ context.Context, _ StructuredQuery) ([]SearchResult, error) {
			return []SearchResult{
				{ID: "docA", Title: "Result A", URL: "urlA", Score: 0.9},
				{ID: "docB", Title: "Result B", URL: "urlB", Score: 0.8},
				{ID: "docC", Title: "Result C", URL: "urlC", Score: 0.7},
			}, nil
		},
	}

	// Searcher 2 provides results including one that duplicates docB
	mockSearcher2 := &MockSearcher{
		ShardID: 0, // Same shard to ensure both are hit
		SearchFunc: func(_ context.Context, _ StructuredQuery) ([]SearchResult, error) {
			return []SearchResult{
				{ID: "docB", Title: "Result B Duplicate", URL: "urlB_dup", Score: 0.85}, // Different score/URL for duplicate
				{ID: "docD", Title: "Result D", URL: "urlD", Score: 0.6},
			}, nil
		},
	}

	broker := NewBroker(mockQU, []Searcher{mockSearcher1, mockSearcher2})

	results, err := broker.Search(ctx, rawQuery)
	if err != nil {
		t.Fatalf("Broker.Search returned an error: %v", err)
	}

	expectedResultsCount := 4 // docA, docB, docC, docD (docB deduplicated)
	if len(results) != expectedResultsCount {
		t.Errorf("Expected %d deduplicated results, got %d. Results: %+v", expectedResultsCount, len(results), results)
	}

	// Verify unique IDs are present
	seenIDs := make(map[string]struct{})
	for _, r := range results {
		if _, seen := seenIDs[r.ID]; seen {
			t.Errorf("Duplicate ID found in results after deduplication: %s", r.ID)
		}
		seenIDs[r.ID] = struct{}{}
	}

	expectedIDs := map[string]struct{}{"docA": {}, "docB": {}, "docC": {}, "docD": {}}
	if len(seenIDs) != len(expectedIDs) {
		t.Errorf("Mismatched unique result count. Expected %d, Got %d", len(expectedIDs), len(seenIDs))
	}
	for id := range expectedIDs {
		if _, ok := seenIDs[id]; !ok {
			t.Errorf("Expected ID %q not found in deduplicated results", id)
		}
	}
}

func TestBroker_Search_NoKeywordsQueryAllShards(t *testing.T) {
	ctx := context.Background()
	rawQuery := RawQuery("no keywords") // Will result in StructuredQuery with no keywords

	mockQU := &MockQueryUnderstandingService{
		ProcessFunc: func(_ context.Context, _ RawQuery) (StructuredQuery, error) {
			return StructuredQuery{}, nil // No keywords
		},
	}

	// Searchers for different shards
	mockSearcher0 := &MockSearcher{
		ShardID: 0,
		SearchFunc: func(_ context.Context, _ StructuredQuery) ([]SearchResult, error) {
			return []SearchResult{{ID: "shard0_doc1"}}, nil
		},
	}
	mockSearcher1 := &MockSearcher{
		ShardID: 1,
		SearchFunc: func(_ context.Context, _ StructuredQuery) ([]SearchResult, error) {
			return []SearchResult{{ID: "shard1_doc1"}}, nil
		},
	}
	mockSearcher2 := &MockSearcher{
		ShardID: 2,
		SearchFunc: func(_ context.Context, _ StructuredQuery) ([]SearchResult, error) {
			return []SearchResult{{ID: "shard2_doc1"}}, nil
		},
	}

	searchers := []Searcher{mockSearcher0, mockSearcher1, mockSearcher2}
	broker := NewBroker(mockQU, searchers)

	results, err := broker.Search(ctx, rawQuery)
	if err != nil {
		t.Fatalf("Broker.Search returned an error: %v", err)
	}

	expectedResultsCount := 3 // One from each shard
	if len(results) != expectedResultsCount {
		t.Errorf("Expected %d results when querying all shards, got %d. Results: %+v", expectedResultsCount, len(results), results)
	}

	foundShard0 := false
	foundShard1 := false
	foundShard2 := false
	for _, r := range results {
		if r.ID == "shard0_doc1" {
			foundShard0 = true
		} else if r.ID == "shard1_doc1" {
			foundShard1 = true
		} else if r.ID == "shard2_doc1" {
			foundShard2 = true
		}
	}

	if !foundShard0 || !foundShard1 || !foundShard2 {
		t.Errorf("Expected results from all shards, but missing some. Shard0: %t, Shard1: %t, Shard2: %t", foundShard0, foundShard1, foundShard2)
	}
}

func TestBroker_Search_NoSearchersAvailable(t *testing.T) {
	ctx := context.Background()
	rawQuery := RawQuery("any query")

	mockQU := &MockQueryUnderstandingService{
		ProcessFunc: func(_ context.Context, _ RawQuery) (StructuredQuery, error) {
			return StructuredQuery{Keywords: []string{"test"}}, nil
		},
	}

	// Create a broker with no searchers
	broker := NewBroker(mockQU, []Searcher{})

	_, err := broker.Search(ctx, rawQuery)
	if err == nil {
		t.Error("Expected an error when no searchers are available, but got nil")
	}
	if err != nil && err.Error() != "no searchers available" {
		t.Errorf("Expected error 'no searchers available', got: %v", err)
	}
}

// Helper to calculate the simple hash used in broker.go
func calculateHash(s string) int {
	hash := 0
	for _, r := range s {
		hash = (hash*31 + int(r))
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}

// Helper to find the first space in a string, for simulating keyword extraction
func findSpace(s string) int {
	for i, r := range s {
		if r == ' ' {
			return i
		}
	}
	return -1
}
