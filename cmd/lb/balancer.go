package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 5, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var timeout = time.Duration(*timeoutSec) * time.Second

var serverAddrs = []string{
	"server1:8080",
	"server2:8080",
	"server3:8080",
}

type Backend struct {
	Addr      string
	ConnCount int
	Healthy   bool
	mu        sync.Mutex
}

var backends []*Backend

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	fwdReq := r.Clone(ctx)
	fwdReq.RequestURI = ""
	fwdReq.URL.Host = dst
	fwdReq.URL.Scheme = scheme()
	fwdReq.Host = dst

	resp, err := http.DefaultClient.Do(fwdReq)
	if err != nil {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
	defer resp.Body.Close()

	for k, values := range resp.Header {
		for _, v := range values {
			rw.Header().Add(k, v)
		}
	}
	if *traceEnabled {
		rw.Header().Set("lb-from", dst)
	}
	log.Println("fwd", resp.StatusCode, resp.Request.URL)
	rw.WriteHeader(resp.StatusCode)

	_, err = io.Copy(rw, resp.Body)
	if err != nil {
		log.Printf("Failed to write response body: %s", err)
	}
	return nil
}

func selectMinConn(list []*Backend) *Backend {
	var chosen *Backend

	for _, b := range list {
		if !b.Healthy {
			continue
		}
		if chosen == nil || b.ConnCount < chosen.ConnCount {
			chosen = b
		}
	}
	return chosen
}

func main() {
	flag.Parse()

	backends = make([]*Backend, len(serverAddrs))
	for i, addr := range serverAddrs {
		backends[i] = &Backend{
			Addr:      addr,
			ConnCount: 0,
			Healthy:   false,
		}
	}

	for _, b := range backends {
		b := b
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				<-ticker.C
				alive := health(b.Addr)
				b.Healthy = alive
				log.Println("Health-check:", b.Addr, "healthy =", alive)
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		backend := selectMinConn(backends)
		if backend == nil {
			rw.WriteHeader(http.StatusServiceUnavailable)
			_, _ = rw.Write([]byte("No healthy backend available"))
			return
		}

		backend.mu.Lock()
		backend.ConnCount++
		backend.mu.Unlock()

		err := forward(backend.Addr, rw, r)

		backend.mu.Lock()
		backend.ConnCount--
		backend.mu.Unlock()

		if err != nil {
			log.Printf("Error forwarding to %s: %s", backend.Addr, err)
		}
	}))

	log.Println("Starting load balancer on port", *port)
	log.Printf("Tracing enabled: %v", *traceEnabled)
	frontend.Start()

	signal.WaitForTerminationSignal()
}
