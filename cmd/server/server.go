package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var port = flag.Int("port", 8080, "server port")

const (
	confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
	confHealthFailure    = "CONF_HEALTH_FAILURE"
	teamKey              = "los_polos"
)

func main() {
	flag.Parse()
	h := http.NewServeMux()

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)
	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(rw, "missing key parameter", http.StatusBadRequest)
			return
		}

		resp, err := http.Get("http://db:8083/db/" + key)
		if err != nil {
			http.Error(rw, "failed to query db", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(rw, resp.Body)
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	go server.Start()

	today := time.Now().Format("2006-01-02")
	postBody := fmt.Sprintf(`{"value":"%s"}`, today)
	resp, err := http.Post("http://db:8083/db/"+teamKey, "application/json", strings.NewReader(postBody))
	if err != nil {
		log.Fatalf("failed to POST initial data to DB: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Fatalf("unexpected status from DB POST: %d; body=%s", resp.StatusCode, string(bodyBytes))
	}

	signal.WaitForTerminationSignal()
}
