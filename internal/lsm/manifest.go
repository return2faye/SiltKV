package lsm

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const manifestFileName = "MANIFEST"

// manifestPath returns the path to the manifest file
func manifestPath(dataDir string) string {
	return filepath.Join(dataDir, manifestFileName)
}

// loadManifest loads SSTable paths from manifest file.
// Returns empty slice if manifest doesn't exist (first run).
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
// New SSTables are appended (newest at the end), but we read in reverse order.
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