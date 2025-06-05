package main

import "testing"

func TestSelectMinConn_AllHealthy(t *testing.T) {
  b1 := &Backend{Addr: "serverA", ConnCount: 5, Healthy: true}
  b2 := &Backend{Addr: "serverB", ConnCount: 2, Healthy: true}
  b3 := &Backend{Addr: "serverC", ConnCount: 3, Healthy: true}

  backends := []*Backend{b1, b2, b3}

  chosen := selectMinConn(backends)
  if chosen != b2 {
    t.Errorf("очікували обрати serverB (ConnCount=2), але вибрано %v (ConnCount=%d)",
      chosen.Addr, chosen.ConnCount)
  }
}

func TestSelectMinConn_SomeUnhealthy(t *testing.T) {
  b1 := &Backend{Addr: "serverA", ConnCount: 1, Healthy: false}
  b2 := &Backend{Addr: "serverB", ConnCount: 0, Healthy: true}
  b3 := &Backend{Addr: "serverC", ConnCount: 0, Healthy: false}

  backends := []*Backend{b1, b2, b3}

  chosen := selectMinConn(backends)
  if chosen != b2 {
    t.Errorf("очікували обрати serverB (єдиний healthy), але вибрано %v", chosen)
  }
}

func TestSelectMinConn_NoHealthy(t *testing.T) {
  b1 := &Backend{Addr: "serverA", ConnCount: 1, Healthy: false}
  b2 := &Backend{Addr: "serverB", ConnCount: 0, Healthy: false}

  backends := []*Backend{b1, b2}

  chosen := selectMinConn(backends)
  if chosen != nil {
    t.Errorf("очікували nil, бо жоден бекенд не healthy, але отримали %v", chosen)
  }
}

func TestSelectMinConn_Tie(t *testing.T) {
  b1 := &Backend{Addr: "serverA", ConnCount: 1, Healthy: true}
  b2 := &Backend{Addr: "serverB", ConnCount: 1, Healthy: true}

  backends := []*Backend{b1, b2}

  chosen := selectMinConn(backends)
  if chosen != b1 {
    t.Errorf("очікували, що при рівних ConnCount вибереться перший до здорових, але вибрано %v", chosen)
  }
}

func TestConnCountIncrementDecrement(t *testing.T) {
  b := &Backend{Addr: "serverA", ConnCount: 0, Healthy: true}

  b.ConnCount++
  if b.ConnCount != 1 {
    t.Errorf("очікували ConnCount=1 після інкременту, але отримали %d", b.ConnCount)
  }

  b.ConnCount--
  if b.ConnCount != 0 {
    t.Errorf("очікували ConnCount=0 після декременту, але отримали %d", b.ConnCount)
  }
}
