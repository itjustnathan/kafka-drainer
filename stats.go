package main

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// PartitionLag tracks offset state for a single topic-partition.
type PartitionLag struct {
	Topic     string
	Partition int32
	Current   int64
	End       int64
}

// StatsManager coordinates throughput tracking, ETA calculation and terminal logging.
type StatsManager struct {
	mu sync.RWMutex

	startTime      time.Time
	lastReportTime time.Time

	totalMsgs  atomic.Uint64
	totalBytes atomic.Uint64
	lastMsgs   uint64
	lastBytes  uint64

	currentRateMsg     float64
	currentRateMB      float64
	groupLag           int64
	groupLagValidUntil time.Time

	partitionOffsets map[string]*PartitionLag // key: "topic:partition"
}

// NewStatsManager initializes a statistics manager.
func NewStatsManager() *StatsManager {
	now := time.Now()
	return &StatsManager{
		startTime:        now,
		lastReportTime:   now,
		partitionOffsets: make(map[string]*PartitionLag),
		groupLag:         -1,
	}
}

// RecordBatch accumulates consumed batch stats.
func (s *StatsManager) RecordBatch(nMsgs uint64, nBytes uint64) {
	s.totalMsgs.Add(nMsgs)
	s.totalBytes.Add(nBytes)
}

// UpdateOffset records the latest consumed offset for a partition.
func (s *StatsManager) UpdateOffset(topic string, partition int32, offset int64) {
	key := fmt.Sprintf("%s:%d", topic, partition)
	s.mu.Lock()
	defer s.mu.Unlock()
	pl, exists := s.partitionOffsets[key]
	if !exists {
		pl = &PartitionLag{Topic: topic, Partition: partition}
		s.partitionOffsets[key] = pl
	}
	pl.Current = offset
}

// SetEndOffset sets the latest log end offset (LEO) for lag computation.
func (s *StatsManager) SetEndOffset(topic string, partition int32, endOffset int64) {
	key := fmt.Sprintf("%s:%d", topic, partition)
	s.mu.Lock()
	defer s.mu.Unlock()
	pl, exists := s.partitionOffsets[key]
	if !exists {
		pl = &PartitionLag{Topic: topic, Partition: partition}
		s.partitionOffsets[key] = pl
	}
	pl.End = endOffset
}

// CalculateTotalLag computes the remaining sum of lags across all partitions.
func (s *StatsManager) CalculateReadLag() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var totalLag int64
	for _, pl := range s.partitionOffsets {
		if pl.End > pl.Current {
			totalLag += (pl.End - pl.Current)
		}
	}
	return totalLag
}

func (s *StatsManager) SetGroupLag(lag int64, validity time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groupLag = lag
	s.groupLagValidUntil = time.Now().Add(validity)
}

func (s *StatsManager) CalculateTotalLag() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if time.Now().After(s.groupLagValidUntil) {
		return -1
	}
	return s.groupLag
}

func formatLag(lag int64) string {
	if lag < 0 {
		return "UNKNOWN"
	}
	return fmt.Sprintf("%d", lag)
}

// HumanBytes formats bytes into human readable string.
func HumanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// FormatDuration formats seconds into human friendly duration.
func FormatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	if h > 0 {
		return fmt.Sprintf("%dh %02dm %02ds", h, m, s)
	}
	return fmt.Sprintf("%02dm %02ds", m, s)
}

// StartReporter runs the periodic terminal logging loop.
func (s *StatsManager) StartReporter(interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			s.PrintFinalSummary()
			return
		case <-ticker.C:
			s.reportCurrent()
		}
	}
}

func (s *StatsManager) reportCurrent() {
	now := time.Now()
	totalM := s.totalMsgs.Load()
	totalB := s.totalBytes.Load()

	s.mu.Lock()
	elapsedSec := now.Sub(s.lastReportTime).Seconds()
	if elapsedSec <= 0 {
		elapsedSec = 1
	}

	deltaM := totalM - s.lastMsgs
	deltaB := totalB - s.lastBytes
	s.lastMsgs = totalM
	s.lastBytes = totalB
	s.lastReportTime = now

	rateMsg := float64(deltaM) / elapsedSec
	rateMB := (float64(deltaB) / (1024 * 1024)) / elapsedSec
	s.currentRateMsg = rateMsg
	s.currentRateMB = rateMB
	s.mu.Unlock()

	totalLag := s.CalculateTotalLag()
	etaStr := "N/A"
	if rateMsg > 0 && totalLag > 0 {
		etaSec := float64(totalLag) / rateMsg
		etaStr = FormatDuration(time.Duration(etaSec) * time.Second)
	} else if totalLag == 0 {
		etaStr = "0s (committed-offset snapshot only)"
	}

	totalElapsed := FormatDuration(time.Since(s.startTime))

	fmt.Printf("[%s] Processed: %12d msgs (%8s) | Speed: %9.0f msg/s (%6.2f MB/s) | Group Lag (snapshot): %12s | ETA: %s\n",
		totalElapsed,
		totalM,
		HumanBytes(totalB),
		rateMsg,
		rateMB,
		formatLag(totalLag),
		etaStr,
	)
}

// PrintFinalSummary displays the full execution report on shutdown.
func (s *StatsManager) PrintFinalSummary() {
	totalM := s.totalMsgs.Load()
	totalB := s.totalBytes.Load()
	totalTime := time.Since(s.startTime)
	avgRateMsg := float64(totalM) / totalTime.Seconds()
	avgRateMB := (float64(totalB) / (1024 * 1024)) / totalTime.Seconds()

	fmt.Println("\n================================================================================")
	fmt.Println("                             DRAIN EXECUTION REPORT                             ")
	fmt.Println("================================================================================")
	fmt.Printf("Total Elapsed Time   : %s\n", FormatDuration(totalTime))
	fmt.Printf("Messages Read/Discarded: %d\n", totalM)
	fmt.Printf("Total Data Read      : %s (%d bytes)\n", HumanBytes(totalB), totalB)
	fmt.Printf("Average Purge Speed  : %.1f msg/s (%.2f MB/s)\n", avgRateMsg, avgRateMB)
	fmt.Printf("Final Group Lag (snapshot): %s offset positions\n", formatLag(s.CalculateTotalLag()))
	fmt.Println("================================================================================")
}

// StartMetricsServer optionally serves Prometheus metrics via HTTP.
func (s *StatsManager) StartMetricsServer(addr string) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		rateMsg := s.currentRateMsg
		rateMB := s.currentRateMB
		s.mu.RUnlock()

		totalM := s.totalMsgs.Load()
		totalB := s.totalBytes.Load()
		lag := s.CalculateTotalLag()

		fmt.Fprintf(w, "# HELP kafka_drain_messages_total Total messages consumed and discarded\n")
		fmt.Fprintf(w, "# TYPE kafka_drain_messages_total counter\n")
		fmt.Fprintf(w, "kafka_drain_messages_total %d\n\n", totalM)

		fmt.Fprintf(w, "# HELP kafka_drain_bytes_total Total bytes consumed and discarded\n")
		fmt.Fprintf(w, "# TYPE kafka_drain_bytes_total counter\n")
		fmt.Fprintf(w, "kafka_drain_bytes_total %d\n\n", totalB)

		fmt.Fprintf(w, "# HELP kafka_drain_rate_msgs_per_sec Instantaneous message purge speed\n")
		fmt.Fprintf(w, "# TYPE kafka_drain_rate_msgs_per_sec gauge\n")
		fmt.Fprintf(w, "kafka_drain_rate_msgs_per_sec %.2f\n\n", rateMsg)

		fmt.Fprintf(w, "# HELP kafka_drain_rate_mb_per_sec Instantaneous bandwidth consumption in MB/s\n")
		fmt.Fprintf(w, "# TYPE kafka_drain_rate_mb_per_sec gauge\n")
		fmt.Fprintf(w, "kafka_drain_rate_mb_per_sec %.2f\n\n", rateMB)

		fmt.Fprintf(w, "# HELP kafka_drain_remaining_lag_total Group committed offset lag snapshot; -1 means unknown or stale\n")
		fmt.Fprintf(w, "# TYPE kafka_drain_remaining_lag_total gauge\n")
		fmt.Fprintf(w, "kafka_drain_remaining_lag_total %d\n", lag)
	})

	go func() {
		server := &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 3 * time.Second,
		}
		_ = server.ListenAndServe()
	}()
}
