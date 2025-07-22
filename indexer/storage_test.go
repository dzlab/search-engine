package indexer

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalFileStorage_New(t *testing.T) {
	t.Run("directory_creation", func(t *testing.T) {
		nonExistentDir := "test_storage_new"
		// Ensure cleanup from previous runs
		if _, err := os.Stat(nonExistentDir); !os.IsNotExist(err) {
			os.RemoveAll(nonExistentDir)
		}

		storage, err := NewLocalFileStorage(nonExistentDir)
		if err != nil {
			t.Fatalf("Failed to create new LocalFileStorage: %v", err)
		}
		defer os.RemoveAll(nonExistentDir) // Schedule cleanup

		// Attempt to create storage in the same directory again, which should now exist but might not be writable
		// This is to ensure that even if the directory exists, the permissions are checked correctly on subsequent operations.
		// However, for the initial creation, the primary check is if the directory can be created and accessed.
		// The test `invalid_directory_permissions` below specifically targets the permission aspect.

		// Verify the directory was created
		if _, err := os.Stat(nonExistentDir); os.IsNotExist(err) {
			t.Errorf("Expected directory %s to be created, but it was not", nonExistentDir)
		}

		// Verify the storage object is correctly initialized
		if storage.storageDir != nonExistentDir {
			t.Errorf("Expected storageDir to be %s, but got %s", nonExistentDir, storage.storageDir)
		}
	})

	t.Run("existing_directory", func(t *testing.T) {
		existingDir := "test_storage_existing"
		os.Mkdir(existingDir, 0755)
		defer os.RemoveAll(existingDir) // Ensure cleanup

		storageExisting, err := NewLocalFileStorage(existingDir)
		if err != nil {
			t.Fatalf("Failed to create LocalFileStorage with existing directory: %v", err)
		}

		if storageExisting.storageDir != existingDir {
			t.Errorf("Expected storageDir to be %s, but got %s", existingDir, storageExisting.storageDir)
		}
	})

	t.Run("invalid_directory_permissions", func(t *testing.T) {
		// Attempt to create in a directory where we don't have write permissions
		// This is system dependent, but often '/' or '/root' can be used for testing this.
		// Create a temporary directory and remove write permissions to simulate a permission error.
		tempDirForPermissionsTest, err := os.MkdirTemp("", "unwritable_dir_test")
		if err != nil {
			t.Fatalf("Failed to create temp dir for permission test: %v", err)
		}
		// Remove write permissions for the owner, group, and others.
		if err := os.Chmod(tempDirForPermissionsTest, 0555); err != nil { // Read and execute only
			t.Fatalf("Failed to remove write permissions from temp dir: %v", err)
		}
		defer os.RemoveAll(tempDirForPermissionsTest) // Schedule cleanup

		_, err = NewLocalFileStorage(tempDirForPermissionsTest)
		if err == nil {
			t.Errorf("Expected an error when creating storage in a directory without write permissions, but got none")
		}
		// Check if the error message indicates a permission issue.
		// The exact error message might vary, so we check for common indicators.
		if err == nil || !strings.Contains(err.Error(), "does not have write permissions") {
			t.Errorf("Expected an error indicating lack of write permissions, but got: %v", err)
		}
	})
}

func TestLocalFileStorage_UploadSegment(t *testing.T) {
	t.Run("successful_upload", func(t *testing.T) {
		// Create a temporary directory for the source segment
		segmentSourceDir, err := os.MkdirTemp("", "segment_source")
		if err != nil {
			t.Fatalf("Failed to create segment source temp dir: %v", err)
		}
		defer os.RemoveAll(segmentSourceDir)

		// Create some dummy files and directories within the segment source
		if err := os.MkdirAll(filepath.Join(segmentSourceDir, "subdir"), 0755); err != nil {
			t.Fatalf("Failed to create subdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(segmentSourceDir, "file1.txt"), []byte("content1"), 0644); err != nil {
			t.Fatalf("Failed to write file1.txt: %v", err)
		}
		if err := os.WriteFile(filepath.Join(segmentSourceDir, "subdir", "file2.dat"), []byte("content2"), 0644); err != nil {
			t.Fatalf("Failed to write file2.dat: %v", err)
		}

		// Create a temporary directory for the storage destination
		storageDestDir, err := os.MkdirTemp("", "storage_dest")
		if err != nil {
			t.Fatalf("Failed to create storage destination temp dir: %v", err)
		}
		defer os.RemoveAll(storageDestDir)

		storage, err := NewLocalFileStorage(storageDestDir)
		if err != nil {
			t.Fatalf("Failed to initialize LocalFileStorage: %v", err)
		}

		// Perform the upload
		err = storage.UploadSegment(segmentSourceDir)
		if err != nil {
			t.Errorf("UploadSegment returned an unexpected error: %v", err)
		}

		// Verify that the files and directories were copied correctly
		expectedFile1 := filepath.Join(storageDestDir, filepath.Base(segmentSourceDir), "file1.txt")
		if _, err := os.Stat(expectedFile1); os.IsNotExist(err) {
			t.Errorf("Expected file %s to be copied, but it was not found", expectedFile1)
		}

		expectedFile2 := filepath.Join(storageDestDir, filepath.Base(segmentSourceDir), "subdir", "file2.dat")
		if _, err := os.Stat(expectedFile2); os.IsNotExist(err) {
			t.Errorf("Expected file %s to be copied, but it was not found", expectedFile2)
		}

		// Verify directory structure
		expectedSubdir := filepath.Join(storageDestDir, filepath.Base(segmentSourceDir), "subdir")
		if _, err := os.Stat(expectedSubdir); os.IsNotExist(err) {
			t.Errorf("Expected directory %s to be created, but it was not", expectedSubdir)
		}
	})

	t.Run("segment_path_not_found", func(t *testing.T) {
		storageDestDir, err := os.MkdirTemp("", "storage_dest_notfound")
		if err != nil {
			t.Fatalf("Failed to create storage destination temp dir: %v", err)
		}
		defer os.RemoveAll(storageDestDir)

		storage, err := NewLocalFileStorage(storageDestDir)
		if err != nil {
			t.Fatalf("Failed to initialize LocalFileStorage: %v", err)
		}

		nonExistentSegmentPath := "non_existent_segment_dir"
		err = storage.UploadSegment(nonExistentSegmentPath)
		if err == nil {
			t.Errorf("Expected an error when uploading from a non-existent segment path, but got none")
		}
		expectedErrorMsg := fmt.Sprintf("segment path %s does not exist", nonExistentSegmentPath)
		if err != nil && !strings.Contains(err.Error(), expectedErrorMsg) {
			t.Errorf("Expected error message containing '%s', but got '%v'", expectedErrorMsg, err)
		}
	})

	t.Run("segment_path_is_file", func(t *testing.T) {
		// Create a temporary file as the segment path
		segmentFile, err := os.CreateTemp("", "segment_file.txt")
		if err != nil {
			t.Fatalf("Failed to create segment file: %v", err)
		}
		segmentFilePath := segmentFile.Name()
		segmentFile.Close() // Close it immediately as we only need its path
		defer os.RemoveAll(segmentFilePath)

		storageDestDir, err := os.MkdirTemp("", "storage_dest_isfile")
		if err != nil {
			t.Fatalf("Failed to create storage destination temp dir: %v", err)
		}
		defer os.RemoveAll(storageDestDir)

		storage, err := NewLocalFileStorage(storageDestDir)
		if err != nil {
			t.Fatalf("Failed to initialize LocalFileStorage: %v", err)
		}

		err = storage.UploadSegment(segmentFilePath)
		if err == nil {
			t.Errorf("Expected an error when uploading from a path that is a file, but got none")
		}
		expectedErrorMsg := fmt.Sprintf("segment path %s is not a directory", segmentFilePath)
		if err != nil && !strings.Contains(err.Error(), expectedErrorMsg) {
			t.Errorf("Expected error message containing '%s', but got '%v'", expectedErrorMsg, err)
		}
	})

	t.Run("copy_file_error_handling", func(t *testing.T) {
		// Create a temporary directory for the source segment
		segmentSourceDir, err := os.MkdirTemp("", "segment_source_err")
		if err != nil {
			t.Fatalf("Failed to create segment source temp dir: %v", err)
		}
		defer os.RemoveAll(segmentSourceDir)

		// Create a file that we will later make unreadable to simulate a copy error
		sourceFileToFail := filepath.Join(segmentSourceDir, "unreadable.txt")
		if err := os.WriteFile(sourceFileToFail, []byte("content"), 0644); err != nil {
			t.Fatalf("Failed to write unreadable file: %v", err)
		}

		// Create a temporary directory for the storage destination
		storageDestDir, err := os.MkdirTemp("", "storage_dest_err")
		if err != nil {
			t.Fatalf("Failed to create storage destination temp dir: %v", err)
		}
		defer os.RemoveAll(storageDestDir)

		storage, err := NewLocalFileStorage(storageDestDir)
		if err != nil {
			t.Fatalf("Failed to initialize LocalFileStorage: %v", err)
		}

		// Make the source file unreadable to trigger a copy error
		if err := os.Chmod(sourceFileToFail, 0000); err != nil { // No permissions
			t.Fatalf("Failed to make file unreadable: %v", err)
		}

		// Perform the upload
		err = storage.UploadSegment(segmentSourceDir)
		if err == nil {
			t.Errorf("Expected an error during file copy due to permissions, but got none")
		}
		// Check if the error message indicates a copy issue (e.g., permission denied)
		// The exact error message can vary between OS and Go versions, so we check for a common substring.
		if err != nil && !strings.Contains(err.Error(), "permission denied") && !strings.Contains(err.Error(), "open ") {
			t.Errorf("Expected error related to file access permissions, but got: %v", err)
		}

		// Verify that the file was NOT copied due to the error
		copiedFile := filepath.Join(storageDestDir, filepath.Base(segmentSourceDir), "unreadable.txt")
		if _, err := os.Stat(copiedFile); !os.IsNotExist(err) {
			t.Errorf("Expected file %s not to be copied, but it was found", copiedFile)
		}
	})
}
