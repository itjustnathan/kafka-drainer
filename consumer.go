package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// RunDrain executes high-speed message draining and offset advancement.
func RunDrain(ctx context.Context, cfg *Config) error {
	stats := NewStatsManager()
	limiter := NewLimiter(cfg.MaxMsgPerSec, cfg.MaxBytesPerSec)
	opts, err := buildBaseClientOpts(cfg)
	if err != nil {
		return err
	}
	monitorClient, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer monitorClient.Close()
	admin := kadm.NewClient(monitorClient)
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	monitorDone := make(chan struct{})
	refresh := func(parent context.Context) {
		queryCtx, cancel := context.WithTimeout(parent, 5*time.Second)
		defer cancel()
		if err := refreshGroupLag(queryCtx, admin, cfg.Group, cfg.Topics, stats, 2*cfg.StatsInterval+5*time.Second); err != nil && parent.Err() == nil {
			fmt.Printf("[LAG] Group lag UNKNOWN: %v\n", err)
		}
	}
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(cfg.StatsInterval)
		defer ticker.Stop()
		for {
			refresh(monitorCtx)
			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	// Start optional Prometheus metrics server
	if cfg.MetricsAddr != "" {
		stats.StartMetricsServer(cfg.MetricsAddr)
		fmt.Printf("[METRICS] Prometheus endpoint started at http://%s/metrics\n", cfg.MetricsAddr)
	}

	stopReporterCh := make(chan struct{})
	reporterDone := make(chan struct{})
	go func() {
		defer close(reporterDone)
		stats.StartReporter(cfg.StatsInterval, stopReporterCh)
	}()
	defer func() {
		stopMonitor()
		<-monitorDone
		refresh(context.Background())
		close(stopReporterCh)
		<-reporterDone
	}()

	if cfg.Mode == "assign" {
		return runAssignDrain(ctx, cfg, stats, limiter)
	}
	return runGroupDrain(ctx, cfg, stats, limiter)
}

func runAssignDrain(ctx context.Context, cfg *Config, stats *StatsManager, limiter *Limiter) error {
	ctx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	fmt.Println("\n[DRAIN] Initializing Direct Assign Mode (requires an inactive target group)...")
	baseOpts, err := buildBaseClientOpts(cfg)
	if err != nil {
		return err
	}

	metaClient, err := kgo.NewClient(baseOpts...)
	if err != nil {
		return fmt.Errorf("failed to create Kafka metadata client: %w", err)
	}
	defer metaClient.Close()

	adm := kadm.NewClient(metaClient)
	if err := requireInactiveGroup(ctx, adm, cfg.Group); err != nil {
		return err
	}

	// 1. Fetch partition metadata and latest LEO
	fmt.Println("[DRAIN] Fetching partition topologies and End Offsets (LEO)...")
	endOffsets, err := adm.ListEndOffsets(ctx, cfg.Topics...)
	if err != nil {
		return fmt.Errorf("failed to fetch end offsets: %w", err)
	}
	if err := validateEndOffsets(endOffsets, cfg.Topics); err != nil {
		return err
	}

	// 2. Fetch current committed offsets for the group
	fmt.Printf("[DRAIN] Fetching existing committed offsets for group '%s'...\n", cfg.Group)
	committed, err := adm.FetchOffsetsForTopics(ctx, cfg.Group, cfg.Topics...)
	if err != nil {
		return fmt.Errorf("failed to fetch committed offsets: %w", err)
	}
	if err := committed.Error(); err != nil {
		return fmt.Errorf("failed to fetch partition committed offsets: %w", err)
	}

	type PartitionTarget struct {
		Topic       string
		Partition   int32
		StartOffset kgo.Offset
		EndOffset   int64
		Lag         int64
	}

	var targets []PartitionTarget
	var totalInitialLag int64

	endOffsets.Each(func(lo kadm.ListedOffset) {
		currentOffset := int64(-1)
		if resp, ok := committed.Lookup(lo.Topic, lo.Partition); ok {
			currentOffset = resp.At
		}

		var startOff kgo.Offset
		if currentOffset >= 0 {
			startOff = kgo.NewOffset().At(currentOffset)
		} else {
			if cfg.ResetPolicy == "latest" {
				startOff = kgo.NewOffset().At(lo.Offset)
			} else {
				startOff = kgo.NewOffset().AtStart()
			}
		}

		lag := int64(0)
		if currentOffset < 0 {
			lag = lo.Offset
		} else if lo.Offset > currentOffset {
			lag = lo.Offset - currentOffset
		}

		targets = append(targets, PartitionTarget{
			Topic:       lo.Topic,
			Partition:   lo.Partition,
			StartOffset: startOff,
			EndOffset:   lo.Offset,
			Lag:         lag,
		})
		totalInitialLag += lag

		stats.SetEndOffset(lo.Topic, lo.Partition, lo.Offset)
		if currentOffset >= 0 {
			stats.UpdateOffset(lo.Topic, lo.Partition, currentOffset)
		} else {
			stats.UpdateOffset(lo.Topic, lo.Partition, 0)
		}
	})

	fmt.Printf("[DRAIN] Discovered %d total partitions across topics %v. Total Initial Lag: %d msgs.\n",
		len(targets), cfg.Topics, totalInitialLag)

	if len(targets) == 0 {
		return fmt.Errorf("no partitions found for topics: %v", cfg.Topics)
	}

	// Determine concurrency workers
	workerCount := cfg.Workers
	if workerCount <= 0 {
		workerCount = len(targets)
		if workerCount > 4 {
			workerCount = 4
		}
	}
	if workerCount > len(targets) {
		workerCount = len(targets)
	}

	fmt.Printf("[DRAIN] Concurrency: %d parallel worker(s) allocated for %d partitions.\n", workerCount, len(targets))

	// Shared state for periodic offset commits
	var (
		offsetsMu     sync.Mutex
		latestOffsets = make(kadm.Offsets)
	)
	snapshotOffsets := func() kadm.Offsets {
		offsetsMu.Lock()
		defer offsetsMu.Unlock()
		snapshot := make(kadm.Offsets)
		for _, partitions := range latestOffsets {
			for _, offset := range partitions {
				snapshot.Add(offset)
			}
		}
		return snapshot
	}
	failures := make(chan error, 1)
	fail := func(err error) {
		select {
		case failures <- err:
		default:
		}
		cancelWorkers()
	}

	// Background committer loop
	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		ticker := time.NewTicker(cfg.CommitInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				snapshot := snapshotOffsets()

				if len(snapshot) > 0 {
					commitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
					cErr := commitOffsetsVerified(commitCtx, adm, cfg.Group, snapshot)
					cancel()
					if cErr != nil && ctx.Err() == nil {
						fail(fmt.Errorf("periodic offset commit failed: %w", cErr))
						return
					}
				}
			}
		}
	}()

	// Partition targets among workers
	workerPartitions := make([]map[string]map[int32]kgo.Offset, workerCount)
	for i := 0; i < workerCount; i++ {
		workerPartitions[i] = make(map[string]map[int32]kgo.Offset)
	}

	for idx, t := range targets {
		wID := idx % workerCount
		if workerPartitions[wID][t.Topic] == nil {
			workerPartitions[wID][t.Topic] = make(map[int32]kgo.Offset)
		}
		workerPartitions[wID][t.Topic][t.Partition] = t.StartOffset
	}

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		assignedParts := workerPartitions[i]
		go func(workerID int, parts map[string]map[int32]kgo.Offset) {
			defer wg.Done()

			wOpts := append(append([]kgo.Opt(nil), baseOpts...), kgo.ConsumePartitions(parts))
			wClient, cErr := kgo.NewClient(wOpts...)
			if cErr != nil {
				fail(fmt.Errorf("worker %d: %w", workerID, cErr))
				return
			}
			defer wClient.Close()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				fetches := wClient.PollFetches(ctx)
				if ctx.Err() != nil {
					return
				}

				if errs := fetches.Errors(); len(errs) > 0 {
					for _, fe := range errs {
						if ctx.Err() == nil {
							fail(fmt.Errorf("fetch %s-%d: %w", fe.Topic, fe.Partition, fe.Err))
						}
					}
					return
				}

				fetches.EachPartition(func(p kgo.FetchTopicPartition) {
					recCount := len(p.Records)
					if recCount == 0 {
						return
					}

					var byteCount int64
					for _, r := range p.Records {
						byteCount += int64(len(r.Key) + len(r.Value))
					}

					// Apply dual rate-limiting (QPS & Bandwidth) to protect broker
					limiter.Wait(int64(recCount), byteCount)

					// Update stats & telemetry
					stats.RecordBatch(uint64(recCount), uint64(byteCount))
					lastRec := p.Records[recCount-1]
					stats.SetEndOffset(p.Topic, p.Partition, p.HighWatermark)
					stats.UpdateOffset(p.Topic, p.Partition, lastRec.Offset+1)

					// Update committed offset tracker (next fetch offset = last offset + 1)
					offsetsMu.Lock()
					latestOffsets.Add(kadm.Offset{
						Topic:     p.Topic,
						Partition: p.Partition,
						At:        lastRec.Offset + 1,
					})
					offsetsMu.Unlock()
				})
			}
		}(i, assignedParts)
	}

	wg.Wait()
	cancelWorkers()
	<-commitDone
	var runErr error
	select {
	case runErr = <-failures:
	default:
	}
	finalSnapshot := snapshotOffsets()
	if len(finalSnapshot) > 0 {
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		flushErr := commitOffsetsVerified(flushCtx, adm, cfg.Group, finalSnapshot)
		cancel()
		if flushErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("final offset commit failed: %w", flushErr))
		} else {
			fmt.Println("[DRAIN] Final offsets committed and verified by broker readback.")
		}
	}
	return runErr
}

func resolveGroupBalancers(balancerName string) []kgo.GroupBalancer {
	switch balancerName {
	case "range":
		return []kgo.GroupBalancer{kgo.RangeBalancer()}
	case "roundrobin":
		return []kgo.GroupBalancer{kgo.RoundRobinBalancer()}
	case "sticky":
		return []kgo.GroupBalancer{kgo.StickyBalancer()}
	case "cooperative-sticky":
		return []kgo.GroupBalancer{kgo.CooperativeStickyBalancer()}
	default: // "auto"
		// Send multiple protocol candidates so Kafka Coordinator picks the one supported by group members
		return []kgo.GroupBalancer{
			kgo.CooperativeStickyBalancer(),
			kgo.StickyBalancer(),
			kgo.RangeBalancer(),
			kgo.RoundRobinBalancer(),
		}
	}
}

// runGroupDrain runs in standard Consumer Group mode.
func runGroupDrain(ctx context.Context, cfg *Config, stats *StatsManager, limiter *Limiter) error {
	fmt.Printf("\n[DRAIN] Initializing Consumer Group Mode (Balancer: %s)...\n", cfg.Balancer)
	baseOpts, err := buildBaseClientOpts(cfg)
	if err != nil {
		return err
	}

	balancers := resolveGroupBalancers(cfg.Balancer)

	groupOpts := append(baseOpts,
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.Balancers(balancers...),
		kgo.GreedyAutoCommit(),
		kgo.AutoCommitInterval(cfg.CommitInterval),
	)

	if cfg.ResetPolicy == "latest" {
		groupOpts = append(groupOpts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	} else {
		groupOpts = append(groupOpts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	client, err := kgo.NewClient(groupOpts...)
	if err != nil {
		return fmt.Errorf("failed to create Consumer Group client: %w", err)
	}
	defer client.Close()

	fmt.Printf("[DRAIN] Subscribed to topics %s under group '%s'. Polling batches...\n",
		strings.Join(cfg.Topics, ", "), cfg.Group)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			recCount := len(p.Records)
			if recCount == 0 {
				return
			}

			var byteCount int64
			for _, r := range p.Records {
				byteCount += int64(len(r.Key) + len(r.Value))
			}

			limiter.Wait(int64(recCount), byteCount)
			stats.RecordBatch(uint64(recCount), uint64(byteCount))
			lastRec := p.Records[recCount-1]
			stats.SetEndOffset(p.Topic, p.Partition, p.HighWatermark)
			stats.UpdateOffset(p.Topic, p.Partition, lastRec.Offset+1)
		})
	}
}
