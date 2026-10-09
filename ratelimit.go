package main

import (
	"sync"
	"time"
)

// Limiter provides dual token-bucket rate limiting for messages (QPS) and bytes (throughput).
type Limiter struct {
	mu sync.Mutex

	msgRate   float64 // tokens per second
	msgTokens float64
	msgMax    float64

	byteRate   float64 // bytes per second
	byteTokens float64
	byteMax    float64

	lastTime time.Time
}

// NewLimiter creates a new dual limiter. 0 means unlimited.
func NewLimiter(msgPerSec int64, bytesPerSec int64) *Limiter {
	l := &Limiter{
		lastTime: time.Now(),
	}

	if msgPerSec > 0 {
		l.msgRate = float64(msgPerSec)
		l.msgMax = float64(msgPerSec)
		l.msgTokens = l.msgMax
	}

	if bytesPerSec > 0 {
		l.byteRate = float64(bytesPerSec)
		l.byteMax = float64(bytesPerSec)
		l.byteTokens = l.byteMax
	}

	return l
}

// Allow checks and waits until nMsg messages and nBytes can be processed.
func (l *Limiter) Wait(nMsg int64, nBytes int64) {
	if l.msgRate <= 0 && l.byteRate <= 0 {
		return
	}

	for {
		l.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(l.lastTime).Seconds()
		l.lastTime = now

		// Replenish message tokens
		if l.msgRate > 0 {
			l.msgTokens += elapsed * l.msgRate
			if l.msgTokens > l.msgMax {
				l.msgTokens = l.msgMax
			}
		}

		// Replenish byte tokens
		if l.byteRate > 0 {
			l.byteTokens += elapsed * l.byteRate
			if l.byteTokens > l.byteMax {
				l.byteTokens = l.byteMax
			}
		}

		// Check if tokens are sufficient
		msgOk := (l.msgRate <= 0) || (l.msgTokens >= float64(nMsg))
		byteOk := (l.byteRate <= 0) || (l.byteTokens >= float64(nBytes))

		if msgOk && byteOk {
			if l.msgRate > 0 {
				l.msgTokens -= float64(nMsg)
			}
			if l.byteRate > 0 {
				l.byteTokens -= float64(nBytes)
			}
			l.mu.Unlock()
			return
		}

		// Compute required sleep duration
		var waitSec float64
		if l.msgRate > 0 && l.msgTokens < float64(nMsg) {
			needed := float64(nMsg) - l.msgTokens
			w := needed / l.msgRate
			if w > waitSec {
				waitSec = w
			}
		}
		if l.byteRate > 0 && l.byteTokens < float64(nBytes) {
			needed := float64(nBytes) - l.byteTokens
			w := needed / l.byteRate
			if w > waitSec {
				waitSec = w
			}
		}
		l.mu.Unlock()

		if waitSec > 0 {
			// Cap wait sleep to avoid hanging too long in one step
			if waitSec > 1.0 {
				waitSec = 1.0
			}
			time.Sleep(time.Duration(waitSec * float64(time.Second)))
		}
	}
}
