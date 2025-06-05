package datastore

import (
  "fmt"
  "os"
  "strings"
  "testing"
)

func TestSegmentCreationAndMerge(t *testing.T) {
  dir := t.TempDir()

  oldMax := MaxSegmentSize
  MaxSegmentSize = 1024
  defer func() { MaxSegmentSize = oldMax }()

  db, err := Open(dir)
  if err != nil {
    t.Fatalf("Open failed: %v", err)
  }
  defer db.Close()

  for i := 0; i < 20; i++ {
    key := "key_" + fmt.Sprint(i)
    val := strings.Repeat("x", 90)
    if err := db.Put(key, val); err != nil {
      t.Fatalf("Put failed at iteration %d: %v", i, err)
    }
  }

  files, err := os.ReadDir(dir)
  if err != nil {
    t.Fatalf("ReadDir failed: %v", err)
  }
  var segCount int
  for _, f := range files {
    name := f.Name()
    if strings.HasPrefix(name, "seg_") && strings.HasSuffix(name, ".dat") {
      segCount++
    }
  }
  if segCount < 1 {
    t.Errorf("expected at least 1 segment file, got %d", segCount)
  }

  if err := db.Put("delete_me", "val"); err != nil {
    t.Fatalf("Put failed: %v", err)
  }
  if err := db.Delete("delete_me"); err != nil {
    t.Fatalf("Delete failed: %v", err)
  }
  _, err = db.Get("delete_me")
  if err != ErrNotFound {
    t.Fatalf("Expected ErrNotFound after delete, got %v", err)
  }

  if err := db.MergeSegments(); err != nil {
    t.Fatalf("MergeSegments failed: %v", err)
  }

  filesAfter, err := os.ReadDir(dir)
  if err != nil {
    t.Fatalf("ReadDir failed: %v", err)
  }
  var mergedCount int
  for _, f := range filesAfter {
    name := f.Name()
    if strings.HasPrefix(name, "seg_") && strings.HasSuffix(name, ".dat") {
      mergedCount++
      if name != "seg_0.dat" {
        t.Errorf("unexpected segment name %q, expected only seg_0.dat", name)
      }
    }
  }
  if mergedCount != 1 {
    t.Errorf("after merge, expected exactly 1 segment file, got %d", mergedCount)
  }

  if _, err := db.Get("delete_me"); err != ErrNotFound {
    t.Errorf("Expected delete_me to be gone after merge, got %v", err)
  }
  val, err := db.Get("key_10")
  if err != nil {
    t.Errorf("Expected key_10 to exist, got error %v", err)
  }
  if len(val) == 0 {
    t.Errorf("Expected non-empty value for key_10 after merge")
  }
}
