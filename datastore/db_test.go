package datastore

import (
  "testing"
)

func TestDb(t *testing.T) {
  tmp := t.TempDir()
  db, err := Open(tmp)
  if err != nil {
    t.Fatal(err)
  }
  t.Cleanup(func() {
    _ = db.Close()
  })

  pairs := [][]string{
    {"k1", "v1"},
    {"k2", "v2"},
    {"k3", "v3"},
    {"k2", "v2.1"},
  }

  t.Run("put/get", func(t *testing.T) {
    for _, pair := range pairs {
      err := db.Put(pair[0], pair[1])
      if err != nil {
        t.Errorf("Cannot put %s: %s", pair[0], err)
      }
      value, err := db.Get(pair[0])
      if err != nil {
        t.Errorf("Cannot get %s: %s", pair[0], err)
      }
      if value != pair[1] {
        t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
      }
    }
  })

  t.Run("delete", func(t *testing.T) {
    err := db.Put("dx", "toDelete")
    if err != nil {
      t.Fatalf("Put before delete failed: %v", err)
    }
    val, err := db.Get("dx")
    if err != nil || val != "toDelete" {
      t.Fatalf("Expected to read %q before delete, got (%q, %v)", "toDelete", val, err)
    }

    if err := db.Delete("dx"); err != nil {
      t.Fatalf("Delete failed: %v", err)
    }
    _, err = db.Get("dx")
    if err != ErrNotFound {
      t.Errorf("Expected ErrNotFound after delete, got %v", err)
    }
  })

  t.Run("file growth", func(t *testing.T) {
    sizeBefore, err := db.Size()
    if err != nil {
      t.Fatal(err)
    }
    for _, pair := range pairs {
      err := db.Put(pair[0], pair[1])
      if err != nil {
        t.Errorf("Cannot put %s: %s", pair[0], err)
      }
    }
    sizeAfter, err := db.Size()
    if err != nil {
      t.Fatal(err)
    }
    if sizeAfter <= sizeBefore {
      t.Errorf("Size does not grow after put (before %d, after %d)", sizeBefore, sizeAfter)
    }
  })

  t.Run("new db process", func(t *testing.T) {
    if err := db.Close(); err != nil {
      t.Fatal(err)
    }
    db, err = Open(tmp)
    if err != nil {
      t.Fatal(err)
    }

    uniquePairs := make(map[string]string)
    for _, pair := range pairs {
      uniquePairs[pair[0]] = pair[1]
    }

    for key, expectedValue := range uniquePairs {
      value, err := db.Get(key)
      if err != nil {
        t.Errorf("Cannot get %s: %s", key, err)
      }
      if value != expectedValue {
        t.Errorf("Get(%q) = %q, wanted %q", key, value, expectedValue)
      }
    }

    _, err = db.Get("dx")
    if err != ErrNotFound {
      t.Errorf("Expected ErrNotFound for deleted key after reopen, got %v", err)
    }
  })
}
