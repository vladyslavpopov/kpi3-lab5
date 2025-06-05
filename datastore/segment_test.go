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

  if err := db.Put("key_5", "newVal"); err != nil {
    t.Fatalf("Put new key_5 failed: %v", err)
  }
  if err := db.MergeSegments(); err != nil {
    t.Fatalf("MergeSegments (second) failed: %v", err)
  }
  val, err := db.Get("key_5")
  if err != nil {
    t.Fatalf("Get after merge failed: %v", err)
  }
  if val != "newVal" {
    t.Errorf("expected merged value %q for key_5, got %q", "newVal", val)
  }
}
