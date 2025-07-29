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
			// Log the failure to load the mapping and proceed with a default. This is a recoverable state.
			log.Printf("Could not load index mapping from 'search-engine/indexer/mapping.json': %v. Falling back to default mapping.", err)
			mapping = CreateDefaultIndexMapping()
		}

		index, err = bleve.New(indexPath, mapping)
		if err != nil {
			return nil, fmt.Errorf("could not create new bleve index at %s: %w", indexPath, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("could not open existing bleve index at %s: %w", indexPath, err)
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
		log.Printf("ERROR: Failed to index document with ID '%s': %v", id, err)
		return fmt.Errorf("error indexing document with ID '%s': %w", id, err)
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
		log.Printf("ERROR: Failed to execute batch index operation for %d documents: %v", len(docs), err)
		return fmt.Errorf("error executing batch index operation for %d documents: %w", len(docs), err)
	}

	log.Printf("Successfully processed batch for %d documents", len(docs))
	return nil
}

// CommitAndUpload commits index changes and uploads the segment. It uses a file-based lock
// to prevent race conditions from multiple indexer instances. This is crucial if indexers
// might run concurrently (e.g., in a distributed setup before a distributed lock manager is in place).
func (i *Indexer) CommitAndUpload() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Define a lock file path. Placing it alongside the index directory is a common pattern.
	lockFilePath := filepath.Join(filepath.Dir(i.indexPath), ".indexer.lock")
	log.Printf("Attempting to acquire lock: %s", lockFilePath)

	// Create a lock file with O_EXCL to ensure atomic creation. If it exists, another process holds the lock.
	lockFile, err := os.OpenFile(lockFilePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsExist(err) {
			log.Printf("Index is locked by another process. Path: %s", lockFilePath)
			return fmt.Errorf("index is locked, another upload may be in progress")
		}
		return fmt.Errorf("failed to create lock file %s: %w", lockFilePath, err)
	}

	// Defer the closing and removal of the lock file to ensure it's cleaned up.
	defer func() {
		lockFile.Close() // Close the file handle.
		if err := os.Remove(lockFilePath); err != nil {
			log.Printf("CRITICAL: Failed to remove lock file %s: %v. Manual intervention may be required.", lockFilePath, err)
		} else {
			log.Printf("Successfully released lock: %s", lockFilePath)
		}
	}()
	log.Printf("Lock acquired successfully. Proceeding with commit and upload.")

	log.Println("Committing index changes and preparing for upload...")
	// The core logic of uploading the segment.
	log.Printf("Triggering upload of index data from %s", i.indexPath)
	if err := i.storage.UploadSegment(i.indexPath); err != nil {
		log.Printf("ERROR: Error during segment upload from path %s: %v", i.indexPath, err)
		// Return a specific error to indicate that the upload failed.
		return fmt.Errorf("failed to upload index segment from %s: %w", i.indexPath, err)
	}

	log.Println("Index commit and upload completed successfully.")
	return nil
}

// Close closes the bleve index.
func (i *Indexer) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	log.Printf("Closing bleve index at %s", i.indexPath)
	return i.index.Close()
}
