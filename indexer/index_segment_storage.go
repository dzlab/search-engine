package indexer

// IndexSegmentStorage defines the interface for storing index segments.
// In a real system, this would interact with S3, GCS, etc.
type IndexSegmentStorage interface {
	UploadSegment(segmentPath string) error
	// Potentially add methods for listing/downloading segments if needed later by other services.
}
