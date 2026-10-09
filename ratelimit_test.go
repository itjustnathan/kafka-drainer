package main

import (
	"testing"
	"time"
)

func TestLimiterUnlimited(t *testing.T) {
	limiter := NewLimiter(0, 0)
	start := time.Now()
	for i := 0; i < 1000; i++ {
		limiter.Wait(100, 1024)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatalf("unlimited limiter should be near-instant, took %v", time.Since(start))
	}
}

func TestLimiterMsgRate(t *testing.T) {
	// 100 msgs per second
	limiter := NewLimiter(100, 0)
	// Initial burst should consume tokens
	limiter.Wait(100, 0)

	start := time.Now()
	// Next 50 should require at least ~0.4s
	limiter.Wait(50, 0)
	elapsed := time.Since(start)
	if elapsed < 350*time.Millisecond {
		t.Fatalf("expected rate limit delay of at least 350ms, got %v", elapsed)
	}
}
