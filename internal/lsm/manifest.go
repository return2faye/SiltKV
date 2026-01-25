package lsm

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Manifest is like a "virtual disk directory" that records which SSTable files
// are valid and their order. It serves as the authoritative source of truth for
// the LSM tree's on-disk structure.
//
// Why Manifest instead of scanning the directory?
//  1. Order guarantee: Manifest records the exact order of SSTables (newest first),
//     which is critical for correct query results (newer data overrides older).
//  2. Validity tracking: After compaction, old SSTable files are deleted but may
//     still exist temporarily. Manifest only lists valid, active SSTables.
//  3. Atomic updates: Using temp file + rename ensures Manifest updates are atomic,
//     preventing corruption during crashes.
//  4. Portability: Relative paths in Manifest allow moving the entire data directory.
//
// Manifest file format:
//   - One SSTable path per line (relative to dataDir)
//   - Order: newest SSTable at the end (we read in reverse order)
//   - Example:
//     active-123.sst
//     active-456.sst
//     compact-789-0.sst
const manifestFileName = "MANIFEST"

// manifestPath returns the path to the manifest file
func manifestPath(dataDir string) string {
	return filepath.Join(dataDir, manifestFileName)
}

// loadManifest loads SSTable paths from manifest file.
// This is called during DB.Open() to recover the list of valid SSTables.
// Returns empty slice if manifest doesn't exist (first run, no SSTables yet).
func loadManifest(dataDir string) ([]string, error) {
	manifestPath := manifestPath(dataDir)

	file, err := os.Open(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// First run, no manifest yet
			return []string{}, nil
		}
		return nil, err
	}
	defer file.Close()

	var paths []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// Convert to absolute path if relative
		if !filepath.IsAbs(line) {
			line = filepath.Join(dataDir, line)
		}
		paths = append(paths, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return paths, nil
}

// appendToManifest appends a new SSTable path to the manifest.
// This is called after each flush when a new SSTable is created.
// New SSTables are appended (newest at the end), but we read in reverse order
// to maintain newest-first order in memory.
func appendToManifest(dataDir string, sstPath string) error {
	manifestPath := manifestPath(dataDir)

	// Convert to relative path for portability
	relPath, err := filepath.Rel(dataDir, sstPath)
	if err != nil {
		// If relative path fails, use absolute
		relPath = sstPath
	}

	file, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = fmt.Fprintln(file, relPath)
	return err
}

// rewriteManifest rewrites the entire manifest with current SSTable list.
// This is used after compaction to:
//   - Remove paths of deleted SSTables (that were merged)
//   - Add paths of newly created merged SSTables
//   - Maintain correct order of all valid SSTables
//
// Uses atomic update (temp file + rename) to prevent corruption during crashes.
func rewriteManifest(dataDir string, sstPaths []string) error {
	manifestPath := manifestPath(dataDir)

	// Create temp file
	tmpPath := manifestPath + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write all paths (as relative paths)
	for _, sstPath := range sstPaths {
		relPath, err := filepath.Rel(dataDir, sstPath)
		if err != nil {
			relPath = sstPath
		}
		if _, err := fmt.Fprintln(file, relPath); err != nil {
			os.Remove(tmpPath)
			return err
		}
	}

	// Sync and close
	if err := file.Sync(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Atomic rename
	return os.Rename(tmpPath, manifestPath)
}
