package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/roman-mazur/architecture-practice-4-template/datastore"
)

const (
	dbDir      = "./data"
	listenAddr = ":8083"
)

var db *datastore.Db

func main() {
	var err error
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		log.Fatalf("cannot create data dir: %v", err)
	}

	db, err = datastore.Open(dbDir)
	if err != nil {
		log.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	http.HandleFunc("/db/", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/db/"):]
		switch r.Method {
		case http.MethodGet:
			val, err := db.Get(key)
			if err == datastore.ErrNotFound {
				http.NotFound(w, r)
				return
			} else if err != nil {
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{
				"key":   key,
				"value": val,
			})
		case http.MethodPost:
			var body struct {
				Value string `json:"value"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			if err := db.Put(key, body.Value); err != nil {
				http.Error(w, "cannot save", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	log.Printf("DB service running on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}
