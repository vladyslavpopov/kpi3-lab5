package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func (db *Db) MergeSegments() error {
	entries, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}

	var segments []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, "seg_") && strings.HasSuffix(name, ".dat") {
			segments = append(segments, filepath.Join(db.dir, name))
		}
	}
	if len(segments) == 0 {
		return nil
	}

	type entryLoc struct {
		filePath string
		offset   int64
	}
	latest := make(map[string]entryLoc)

	for _, segPath := range segments {
		f, err := os.Open(segPath)
		if err != nil {
			return fmt.Errorf("MergeSegments: cannot open %s: %w", segPath, err)
		}
		in := bufio.NewReader(f)
		var off int64 = 0
		for {
			var rec entry
			n, errRead := rec.DecodeFromReader(in)
			if errors.Is(errRead, io.EOF) {
				break
			}
			if errRead != nil {
				f.Close()
				return fmt.Errorf("MergeSegments: decode error in %s: %w", segPath, errRead)
			}
			latest[rec.key] = entryLoc{filePath: segPath, offset: off}
			off += int64(n)
		}
		f.Close()
	}

	mergedPath := filepath.Join(db.dir, "merged.tmp")
	mf, err := os.Create(mergedPath)
	if err != nil {
		return fmt.Errorf("MergeSegments: cannot create merged.tmp: %w", err)
	}

	for _, loc := range latest {
		sf, err := os.Open(loc.filePath)
		if err != nil {
			mf.Close()
			return fmt.Errorf("MergeSegments: cannot reopen %s: %w", loc.filePath, err)
		}
		if _, err := sf.Seek(loc.offset, io.SeekStart); err != nil {
			sf.Close()
			mf.Close()
			return fmt.Errorf("MergeSegments: cannot seek %s: %w", loc.filePath, err)
		}
		var rec entry
		if _, err := rec.DecodeFromReader(bufio.NewReader(sf)); err != nil {
			sf.Close()
			mf.Close()
			return fmt.Errorf("MergeSegments: decodeFromReader: %w", err)
		}
		sf.Close()

		if _, err := mf.Write(rec.Encode()); err != nil {
			mf.Close()
			return fmt.Errorf("MergeSegments: write to merged.tmp: %w", err)
		}
	}

	if err := mf.Close(); err != nil {
		return fmt.Errorf("MergeSegments: cannot close merged.tmp: %w", err)
	}

	for _, segPath := range segments {
		_ = os.Remove(segPath)
	}

	finalPath := filepath.Join(db.dir, "seg_0.dat")
	if err := os.Rename(mergedPath, finalPath); err != nil {
		return fmt.Errorf("MergeSegments: rename merged.tmp: %w", err)
	}

	db.index = make(hashIndex)
	if err := db.recoverFile(finalPath); err != nil {
		return fmt.Errorf("MergeSegments: recover merged segment: %w", err)
	}

	currPath := filepath.Join(db.dir, outFileName)
	if err := db.recoverFile(currPath); err != nil {
		return fmt.Errorf("MergeSegments: recover current-data: %w", err)
	}

	db.segmentIndex = 1

	return nil
}
