package indexer

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// IndexSegmentStorage defines the interface for storing index segments.
// In a real system, this would interact with S3, GCS, etc.
type IndexSegmentStorage interface {
	UploadSegment(segmentPath string) error
	// Potentially add methods for listing/downloading segments if needed later by other services.
}

// S3Storage implements IndexSegmentStorage for AWS S3.
type S3Storage struct {
	uploader *s3manager.Uploader
	bucket   string
}

// NewS3Storage creates a new S3Storage instance.
// It initializes an AWS session and an S3 uploader.
// AWS credentials and region should be configured via environment variables
// (e.g., AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION) or IAM roles.
func NewS3Storage(bucketName string) (*S3Storage, error) {
	// Load the Shared AWS Configuration (~/.aws/credentials) or environment variables.
	sess, err := session.NewSession(&aws.Config{
		// Region: aws.String("your-aws-region"), // Can specify region here or rely on env var AWS_REGION
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	// Create an S3 Uploader from the session
	uploader := s3manager.NewUploader(sess)

	log.Printf("Initialized S3Storage for bucket: %s", bucketName)
	return &S3Storage{
		uploader: uploader,
		bucket:   bucketName,
	}, nil
}

// UploadSegment uploads the contents of the segment directory to S3.
// The segmentPath is expected to be a directory.
// Each file within the directory (and its subdirectories) will be uploaded
// to S3 with a key prefixed by a timestamped segment name.
// For example, if segmentPath is "/tmp/myindex" and a file is "/tmp/myindex/data/file1.dat",
// the S3 key might be "myindex_20230101T120000Z/data/file1.dat".
func (s *S3Storage) UploadSegment(segmentPath string) error {
	info, err := os.Stat(segmentPath)
	if err != nil {
		return fmt.Errorf("failed to stat segment path %s: %w", segmentPath, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("segment path %s is not a directory", segmentPath)
	}

	// Create a unique prefix for this segment upload (e.g., base name + timestamp)
	segmentBaseName := filepath.Base(segmentPath)
	timestamp := time.Now().UTC().Format("20060102T150405Z")      // YYYYMMDDTHHMMSSZ
	s3Prefix := fmt.Sprintf("%s_%s/", segmentBaseName, timestamp) // Add trailing slash for directory-like prefix

	log.Printf("Starting upload of index segment from %s to S3 bucket %s with prefix %s", segmentPath, s.bucket, s3Prefix)

	err = filepath.WalkDir(segmentPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err // Return error if walking fails
		}

		if d.IsDir() {
			return nil // Skip directories, we only upload files
		}

		// Calculate the relative path from the segmentPath
		// e.g., if segmentPath="/tmp/myindex" and path="/tmp/myindex/data/file1.dat",
		// then relPath will be "data/file1.dat"
		relPath, err := filepath.Rel(segmentPath, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", path, err)
		}

		// Construct the S3 key
		s3Key := filepath.Join(s3Prefix, relPath)
		// S3 uses forward slashes, ensure that even on Windows
		s3Key = filepath.ToSlash(s3Key)

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", path, err)
		}
		defer file.Close() // Ensure file is closed after upload

		log.Printf("Uploading %s to s3://%s/%s", path, s.bucket, s3Key)

		_, err = s.uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(s3Key),
			Body:   file,
		})
		if err != nil {
			return fmt.Errorf("failed to upload file %s to S3 %s/%s: %w", path, s.bucket, s3Key, err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("error during segment upload to S3: %w", err)
	}

	log.Printf("Successfully uploaded index segment from %s to S3 bucket %s with prefix %s", segmentPath, s.bucket, s3Prefix)
	return nil
}

// LocalFileStorage implements IndexSegmentStorage for local filesystem.
// This is a stand-in for cloud storage like S3, kept for local testing/development purposes.
type LocalFileStorage struct {
	storageDir string
}

// NewLocalFileStorage creates a new LocalFileStorage instance, ensuring the directory exists.
func NewLocalFileStorage(dir string) (*LocalFileStorage, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create storage directory %s: %w", dir, err)
		}
	}
	return &LocalFileStorage{storageDir: dir}, nil
}

// UploadSegment simulates uploading the segment file(s) to the local storage directory.
// For Bleve, a segment might be a directory containing multiple files representing a snapshot.
// This simplified version just logs the action and expects segmentPath to be the index directory path.
func (s *LocalFileStorage) UploadSegment(segmentPath string) error {
	log.Printf("Simulating uploading index data from %s to local storage %s", segmentPath, s.storageDir)
	log.Printf("Index data from %s conceptually 'uploaded' to %s", segmentPath, s.storageDir)
	return nil // Simulate success for the placeholder
}
