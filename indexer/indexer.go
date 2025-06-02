package indexer

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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
		log.Printf("Creating new index at %s", indexPath)
		// Define a simple mapping for demonstration. A real mapping would be complex.
		mapping := bleve.NewIndexMapping()
		// Example text field mapping
		textFieldMapping := bleve.NewTextFieldMapping()
		textFieldMapping.Analyzer = "standard"
		mapping.DefaultMapping.AddFieldMapping(textFieldMapping)

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

// Structs for request bodies
type IndexRequest struct {
	ID   string      `json:"id"`
	Data interface{} `json:"data"` // Use interface{} to accept any JSON object
}

type DeleteRequest struct {
	ID string `json:"id"`
}

// HandleIndexRequest is an HTTP handler for adding/updating documents.
func (i *Indexer) HandleIndexRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading index request body: %v", err)
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var req IndexRequest
	if err := json.Unmarshal(body, &req); err != nil {
		log.Printf("Error unmarshalling index request body: %v", err)
		http.Error(w, "Error parsing request body: invalid JSON", http.StatusBadRequest)
		return
	}

	if req.ID == "" {
		http.Error(w, "Document ID is required", http.StatusBadRequest)
		return
	}

	if err := i.IndexDocument(req.ID, req.Data); err != nil {
		log.Printf("Error indexing document %s: %v", req.ID, err)
		http.Error(w, fmt.Sprintf("Failed to index document %s", req.ID), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Document %s indexed successfully", req.ID)))
	log.Printf("Handled index request for document %s", req.ID)
}

// HandleDeleteRequest is an HTTP handler for deleting documents.
func (i *Indexer) HandleDeleteRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { // Using POST as discussed, could be DELETE
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading delete request body: %v", err)
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var req DeleteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		log.Printf("Error unmarshalling delete request body: %v", err)
		http.Error(w, "Error parsing request body: invalid JSON", http.StatusBadRequest)
		return
	}

	if req.ID == "" {
		http.Error(w, "Document ID is required", http.StatusBadRequest)
		return
	}

	if err := i.DeleteDocument(req.ID); err != nil {
		log.Printf("Error deleting document %s: %v", req.ID, err)
		http.Error(w, fmt.Sprintf("Failed to delete document %s", req.ID), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Document %s deleted successfully", req.ID)))
	log.Printf("Handled delete request for document %s", req.ID)
}

// HandleCommitRequest is an HTTP handler for committing and uploading index segments.
func (i *Indexer) HandleCommitRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	log.Println("Received commit and upload request.")
	if err := i.CommitAndUpload(); err != nil {
		log.Printf("Error during commit and upload: %v", err)
		http.Error(w, "Failed to commit and upload index", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Index committed and uploaded successfully"))
	log.Println("Handled commit and upload request.")
}
