package main

import (
	"testing"
	"time"
)

func TestStatsManager(t *testing.T) {
	sm := NewStatsManager()

	sm.SetEndOffset("topic_a", 0, 1000)
	sm.SetEndOffset("topic_a", 1, 2000)

	sm.UpdateOffset("topic_a", 0, 200)
	sm.UpdateOffset("topic_a", 1, 500)

	lag := sm.CalculateReadLag()
	expectedLag := int64((1000 - 200) + (2000 - 500)) // 800 + 1500 = 2300
	if lag != expectedLag {
		t.Fatalf("expected lag %d, got %d", expectedLag, lag)
	}

	sm.RecordBatch(100, 10240)
	if sm.totalMsgs.Load() != 100 {
		t.Fatalf("expected total msgs 100, got %d", sm.totalMsgs.Load())
	}
	if sm.totalBytes.Load() != 10240 {
		t.Fatalf("expected total bytes 10240, got %d", sm.totalBytes.Load())
	}

	hStr := HumanBytes(1024 * 1024 * 5)
	if hStr != "5.00 MB" {
		t.Fatalf("expected 5.00 MB, got %s", hStr)
	}

	dStr := FormatDuration(3665 * time.Second)
	if dStr != "1h 01m 05s" {
		t.Fatalf("expected 1h 01m 05s, got %s", dStr)
	}
}

func TestReadProgressDoesNotProveCommittedLag(t *testing.T) {
	stats := NewStatsManager()
	stats.SetEndOffset("alarm", 0, 100)
	stats.UpdateOffset("alarm", 0, 100)
	if lag := stats.CalculateTotalLag(); lag != -1 {
		t.Fatalf("unverified group lag must be unknown (-1), got %d", lag)
	}
}
