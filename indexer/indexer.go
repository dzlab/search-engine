package indexer

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/blevesearch/bleve/v2"
)

// Indexer represents the Indexer service responsible for managing the search index.
type Indexer struct {
	indexPath string
	index     bleve.Index
	storage   IndexSegmentStorage // Use the interface defined elsewhere
	mu        sync.Mutex          // Mutex to protect concurrent access to the index
}

// NewIndexer creates a new Indexer instance, opening or creating the Bleve index.
func NewIndexer(indexPath string, storage IndexSegmentStorage) (*Indexer, error) {
	// Ensure parent directory for index exists
	if err := os.MkdirAll(filepath.Dir(indexPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create index parent directory %s: %w", filepath.Dir(indexPath), err)
	}

	// Open or create the Bleve index
	index, err := bleve.Open(indexPath)
	if err == bleve.ErrorIndexPathDoesNotExist {
		log.Printf("Creating new index at %s using mapping from mapping.json", indexPath)
		mapping, err := LoadIndexMapping("search-engine/indexer/mapping.json")
		if err != nil {
			log.Printf("Failed to load index mapping from file, attempting to create default mapping: %v", err)
			// Fallback to a default mapping if file loading fails
			mapping = CreateDefaultIndexMapping()
		}

		index, err = bleve.New(indexPath, mapping)
		if err != nil {
			return nil, fmt.Errorf("failed to create bleve index at %s: %w", indexPath, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to open bleve index at %s: %w", indexPath, err)
	}

	log.Printf("Bleve index opened/created at %s", indexPath)

	return &Indexer{
		indexPath: indexPath,
		index:     index,
		storage:   storage,
	}, nil
}

// IndexDocument adds or updates a document in the index.
func (i *Indexer) IndexDocument(id string, data interface{}) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	log.Printf("Attempting to index document with ID: %s", id)
	// Bleve automatically handles updates if the ID exists
	if err := i.index.Index(id, data); err != nil {
		log.Printf("Failed to index document %s: %v", id, err)
		return fmt.Errorf("failed to index document %s: %w", id, err)
	}
	log.Printf("Successfully indexed document with ID: %s", id)
	return nil
}

// DeleteDocument removes a document from the index.
func (i *Indexer) DeleteDocument(id string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	log.Printf("Attempting to delete document with ID: %s", id)
	if err := i.index.Delete(id); err != nil {
		// Bleve's Delete might return an error if the document doesn't exist,
		// or depending on configuration. Handle specific errors if necessary.
		log.Printf("Failed to delete document %s: %v", id, err)
		return fmt.Errorf("failed to delete document %s: %w", id, err)
	}
	log.Printf("Successfully deleted document with ID: %s", id)
	return nil
}

// BulkIndexDocuments adds or updates multiple documents in the index using a batch.
func (i *Indexer) BulkIndexDocuments(docs map[string]interface{}) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	log.Printf("Attempting to bulk index %d documents", len(docs))
	batch := i.index.NewBatch()

	for id, data := range docs {
		log.Printf("Adding document %s to batch", id)
		batch.Index(id, data)
	}

	if err := i.index.Batch(batch); err != nil {
		log.Printf("Failed to execute batch index operation: %v", err)
		return fmt.Errorf("failed to execute batch index operation: %w", err)
	}

	log.Printf("Successfully processed batch for %d documents", len(docs))
	return nil
}

// CommitAndUpload commits the current index changes and uploads the index state.
// In Bleve, indexing operations are eventually consistent. A 'commit' might mean
// waiting for pending operations or creating a snapshot point. Uploading means
// making the latest committed state available to Searchers.
func (i *Indexer) CommitAndUpload() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	log.Println("Committing index changes and preparing for upload...")
	// Bleve handles flushing internally. To ensure durability before 'upload',
	// you might need to force a flush or close/re-open, but this can be disruptive.
	// Relying on Bleve's default flushing is often sufficient if Searchers
	// can tolerate slightly delayed updates.
	// A true "segment upload" in Bleve might involve copying the entire
	// index directory after ensuring it's in a consistent state.

	// Simulate waiting for writes to settle (not a real Bleve function)
	// time.Sleep(1 * time.Second) // Just for demonstration

	// The 'segmentPath' to upload for Bleve is typically the index directory itself
	// or a snapshot created within it.
	log.Printf("Triggering upload of index data from %s", i.indexPath)
	if err := i.storage.UploadSegment(i.indexPath); err != nil {
		log.Printf("Error during segment upload: %v", err)
		return fmt.Errorf("failed to upload segment: %w", err)
	}

	log.Println("Index commit simulation complete and segment upload simulated successfully.")
	return nil
}

// Close closes the bleve index.
func (i *Indexer) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	log.Printf("Closing bleve index at %s", i.indexPath)
	return i.index.Close()
}
