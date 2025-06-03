package service

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"indexer"
)

// Structs for request bodies
type IndexRequest struct {
	ID   string      `json:"id"`
	Data interface{} `json:"data"` // Use interface{} to accept any JSON object
}

type DeleteRequest struct {
	ID string `json:"id"`
}

// BulkIndexRequest represents a request to index multiple documents in a batch.
// It's a map where keys are document IDs and values are the document data.
type BulkIndexRequest map[string]interface{}

// WebService handles HTTP requests for the indexer.
type WebService struct {
	indexer    *indexer.Indexer
	listenAddr string
}

// NewWebService creates a new WebService instance.
func NewWebService(indexer *indexer.Indexer, listenAddr string) *WebService {
	return &WebService{
		indexer:    indexer,
		listenAddr: listenAddr,
	}
}

// Start starts the web service and listens for incoming requests.
func (ws *WebService) Start() error {
	// Set up HTTP endpoints for receiving indexing requests
	http.HandleFunc("/index", ws.HandleIndexRequest)
	http.HandleFunc("/delete", ws.HandleDeleteRequest)
	http.HandleFunc("/commit", ws.HandleCommitRequest)
	http.HandleFunc("/bulk_index", ws.HandleBulkIndexRequest) // New endpoint for bulk indexing

	log.Printf("Web service listening on %s", ws.listenAddr)
	if err := http.ListenAndServe(ws.listenAddr, nil); err != nil {
		return fmt.Errorf("failed to start web service: %w", err)
	}
	return nil
}

// HandleIndexRequest is an HTTP handler for adding/updating documents.
func (ws *WebService) HandleIndexRequest(w http.ResponseWriter, r *http.Request) {
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

	if err := ws.indexer.IndexDocument(req.ID, req.Data); err != nil {
		log.Printf("Error indexing document %s: %v", req.ID, err)
		http.Error(w, fmt.Sprintf("Failed to index document %s", req.ID), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Document %s indexed successfully", req.ID)))
	log.Printf("Handled index request for document %s", req.ID)
}

// HandleDeleteRequest is an HTTP handler for deleting documents.
func (ws *WebService) HandleDeleteRequest(w http.ResponseWriter, r *http.Request) {
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

	if err := ws.indexer.DeleteDocument(req.ID); err != nil {
		log.Printf("Error deleting document %s: %v", req.ID, err)
		http.Error(w, fmt.Sprintf("Failed to delete document %s", req.ID), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Document %s deleted successfully", req.ID)))
	log.Printf("Handled delete request for document %s", req.ID)
}

// HandleBulkIndexRequest is an HTTP handler for bulk adding/updating documents.
func (ws *WebService) HandleBulkIndexRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading bulk index request body: %v", err)
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var req BulkIndexRequest
	if err := json.Unmarshal(body, &req); err != nil {
		log.Printf("Error unmarshalling bulk index request body: %v", err)
		http.Error(w, "Error parsing request body: invalid JSON", http.StatusBadRequest)
		return
	}

	if len(req) == 0 {
		http.Error(w, "Request body is empty", http.StatusBadRequest)
		return
	}

	if err := ws.indexer.BulkIndexDocuments(req); err != nil {
		log.Printf("Error bulk indexing documents: %v", err)
		http.Error(w, "Failed to bulk index documents", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Successfully processed bulk index request for %d documents", len(req))))
	log.Printf("Handled bulk index request for %d documents", len(req))
}

// HandleCommitRequest is an HTTP handler for committing and uploading index segments.
func (ws *WebService) HandleCommitRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	log.Println("Received commit and upload request.")
	if err := ws.indexer.CommitAndUpload(); err != nil {
		log.Printf("Error during commit and upload: %v", err)
		http.Error(w, "Failed to commit and upload index", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Index committed and uploaded successfully"))
	log.Println("Handled commit and upload request.")
}
