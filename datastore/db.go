package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const DefaultMaxSegmentSize = 10 * 1024 * 1024

var MaxSegmentSize int64 = DefaultMaxSegmentSize

const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type filePos struct {
	fileName string
	offset   int64
}

type hashIndex map[string]filePos

type Db struct {
	dir          string
	out          *os.File
	outOffset    int64
	segmentIndex int
	index        hashIndex
	mu           sync.RWMutex
	writeCh      chan writeRequest
}

type writeRequest struct {
	key   string
	value string
	done  chan error
}

func Open(dir string) (*Db, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	db := &Db{
		dir:   dir,
		index: make(hashIndex),
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	maxIdx := -1
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var idx int
		if n, _ := fmt.Sscanf(entry.Name(), "seg_%d.dat", &idx); n == 1 && idx > maxIdx {
			maxIdx = idx
		}
	}
	db.segmentIndex = maxIdx + 1

	currPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(currPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db.out = f

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	db.outOffset = info.Size()

	for i := 0; i <= maxIdx; i++ {
		segName := filepath.Join(dir, fmt.Sprintf("seg_%d.dat", i))
		if err := db.recoverFile(segName); err != nil && err != io.EOF {
			f.Close()
			return nil, err
		}
	}
	if err := db.recoverFile(currPath); err != nil && err != io.EOF {
		f.Close()
		return nil, err
	}

	db.writeCh = make(chan writeRequest)
	go db.runWriter()

	return db, nil
}

func (db *Db) recoverFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	in := bufio.NewReader(f)
	var offset int64 = 0
	for {
		var rec entry
		n, err := rec.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("recoverFile, decode error: %w", err)
		}
		db.mu.Lock()
		db.index[rec.key] = filePos{fileName: path, offset: offset}
		db.mu.Unlock()
		offset += int64(n)
	}
	return nil
}

func (db *Db) runWriter() {
	for req := range db.writeCh {
		e := entry{key: req.key, value: req.value}
		b := e.Encode()
		toWrite := int64(len(b))

		if db.outOffset+toWrite > MaxSegmentSize {
			if err := db.rotateSegment(); err != nil {
				req.done <- err
				continue
			}
		}

		n, err := db.out.Write(b)
		if err != nil {
			req.done <- err
			continue
		}

		currFile := filepath.Join(db.dir, outFileName)
		db.mu.Lock()
		db.index[req.key] = filePos{fileName: currFile, offset: db.outOffset}
		db.mu.Unlock()
		db.outOffset += int64(n)

		req.done <- nil
	}
}

func (db *Db) Put(key, value string) error {
	done := make(chan error)
	db.writeCh <- writeRequest{key: key, value: value, done: done}
	return <-done
}

func (db *Db) Get(key string) (string, error) {
	db.mu.RLock()
	pos, ok := db.index[key]
	db.mu.RUnlock()
	if !ok {
		return "", ErrNotFound
	}

	f, err := os.Open(pos.fileName)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if _, err := f.Seek(pos.offset, io.SeekStart); err != nil {
		return "", err
	}

	var rec entry
	if _, err := rec.DecodeFromReader(bufio.NewReader(f)); err != nil {
		return "", err
	}
	return rec.value, nil
}

func (db *Db) Size() (int64, error) {
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (db *Db) Close() error {
	close(db.writeCh)
	return db.out.Close()
}

func (db *Db) rotateSegment() error {
	if err := db.out.Close(); err != nil {
		return err
	}

	oldPath := filepath.Join(db.dir, outFileName)
	newPath := filepath.Join(db.dir, fmt.Sprintf("seg_%d.dat", db.segmentIndex))
	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}

	db.segmentIndex++

	f, err := os.OpenFile(oldPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	db.out = f
	db.outOffset = 0
	return nil
}
