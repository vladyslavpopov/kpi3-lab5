package integration

import (
  "bufio"
  "io"
  "net/http"
  "strings"
  "sync"
  "testing"
  "time"
)

const (
  balancerURL       = "http://balancer:8090/api/v1/some-data?key=los_polos"
  totalRequests     = 30
  concurrentWorkers = 10
)

func collectHeaders(t *testing.T, n int) []string {
  var wg sync.WaitGroup
  chResults := make(chan string, n)

  worker := func() {
    defer wg.Done()
    resp, err := http.Get(balancerURL)
    if err != nil {
      t.Errorf("помилка під час запиту до балансувальника: %v", err)
      return
    }
    defer resp.Body.Close()

    bodyBytes, err := io.ReadAll(resp.Body)
    if err != nil {
      t.Errorf("не вдалося прочитати тіло відповіді: %v", err)
      return
    }
    if len(bodyBytes) == 0 {
      t.Errorf("очікували непусте тіло відповіді від DB, отримали порожнє")
      return
    }

    lbFrom := resp.Header.Get("lb-from")
    if lbFrom == "" {
      t.Errorf("заголовок lb-from відсутній у відповіді")
      return
    }
    chResults <- lbFrom

    scanner := bufio.NewScanner(
      strings.NewReader(string(bodyBytes)),
    )
    for scanner.Scan() {
    }
  }

  sem := make(chan struct{}, concurrentWorkers)
  for i := 0; i < n; i++ {
    wg.Add(1)
    sem <- struct{}{}
    go func() {
      defer func() { <-sem }()
      worker()
    }()
  }

  wg.Wait()
  close(chResults)

  results := make([]string, 0, n)
  for src := range chResults {
    results = append(results, src)
  }
  return results
}

func TestBalancedDistribution(t *testing.T) {
  time.Sleep(5 * time.Second)

  headers := collectHeaders(t, totalRequests)
  if len(headers) != totalRequests {
    t.Fatalf("замість %d отримаємо %d lb-from значень", totalRequests, len(headers))
  }

  counts := make(map[string]int)
  for _, src := range headers {
    counts[src]++
  }

  expectedServers := []string{"server1:8080", "server2:8080", "server3:8080"}
  for _, srv := range expectedServers {
    if counts[srv] == 0 {
      t.Errorf("очікували, що хоча б один запит піде до %s, а отримали 0", srv)
    }
  }

  avg := totalRequests / len(expectedServers)
  maxAllowed := avg + 2
  for _, srv := range expectedServers {
    if counts[srv] > maxAllowed {
      t.Errorf("%s отримав %d запитів, що більше за допустимий ліміт %d", srv, counts[srv], maxAllowed)
    }
  }

  t.Logf("Розподіл запитів: %+v", counts)
}
