package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// buildBaseClientOpts constructs standard client options from Config.
func buildBaseClientOpts(cfg *Config) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.FetchMinBytes(cfg.FetchMinBytes),
		kgo.FetchMaxBytes(cfg.FetchMaxBytes),
		kgo.FetchMaxPartitionBytes(cfg.MaxPartitionFetchBytes),
	}

	if cfg.TLSEnable {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: cfg.TLSSkipVerify,
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	if cfg.SASLMechanism != "" {
		switch cfg.SASLMechanism {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: cfg.SASLUser,
				Pass: cfg.SASLPass,
			}.AsMechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: cfg.SASLUser,
				Pass: cfg.SASLPass,
			}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: cfg.SASLUser,
				Pass: cfg.SASLPass,
			}.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", cfg.SASLMechanism)
		}
	}

	return opts, nil
}

func RunFastForward(ctx context.Context, cfg *Config) error {
	fmt.Println("\n[FAST-FORWARD] Initializing instant offset alignment mode...")
	fmt.Printf("[FAST-FORWARD] Target Group: %s | Topics: %s\n", cfg.Group, strings.Join(cfg.Topics, ", "))

	opts, err := buildBaseClientOpts(cfg)
	if err != nil {
		return err
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %w", err)
	}
	defer client.Close()

	adm := kadm.NewClient(client)
	if err := requireInactiveGroup(ctx, adm, cfg.Group); err != nil {
		return err
	}

	// 1. Query latest Log End Offsets (LEO)
	fmt.Println("[FAST-FORWARD] Querying latest Log End Offsets (LEO) from cluster...")
	endOffsets, err := adm.ListEndOffsets(ctx, cfg.Topics...)
	if err != nil {
		return fmt.Errorf("failed to query end offsets: %w", err)
	}
	if err := validateEndOffsets(endOffsets, cfg.Topics); err != nil {
		return err
	}

	// 2. Query currently committed offsets for the group
	fmt.Printf("[FAST-FORWARD] Querying committed offsets for group '%s'...\n", cfg.Group)
	committed, err := adm.FetchOffsetsForTopics(ctx, cfg.Group, cfg.Topics...)
	if err != nil {
		return fmt.Errorf("failed to query committed offsets for group %s: %w", cfg.Group, err)
	}
	if err := committed.Error(); err != nil {
		return fmt.Errorf("failed to query partition committed offsets: %w", err)
	}

	type PartitionDiff struct {
		Topic     string
		Partition int32
		Current   int64
		TargetLEO int64
		Lag       int64
	}

	var diffs []PartitionDiff
	var totalLag int64

	endOffsets.Each(func(lo kadm.ListedOffset) {
		currentOffset := int64(-1)
		if resp, ok := committed.Lookup(lo.Topic, lo.Partition); ok {
			currentOffset = resp.At
		}

		lag := int64(0)
		if currentOffset < 0 {
			lag = lo.Offset
		} else if lo.Offset > currentOffset {
			lag = lo.Offset - currentOffset
		}

		diffs = append(diffs, PartitionDiff{
			Topic:     lo.Topic,
			Partition: lo.Partition,
			Current:   currentOffset,
			TargetLEO: lo.Offset,
			Lag:       lag,
		})
		totalLag += lag
	})

	sort.Slice(diffs, func(i, j int) bool {
		if diffs[i].Topic != diffs[j].Topic {
			return diffs[i].Topic < diffs[j].Topic
		}
		return diffs[i].Partition < diffs[j].Partition
	})

	fmt.Println("\n--------------------------------------------------------------------------------")
	fmt.Printf("%-24s | %-9s | %-15s | %-15s | %-10s\n", "TOPIC", "PARTITION", "CURRENT OFFSET", "TARGET (LEO)", "CLEARED LAG")
	fmt.Println("--------------------------------------------------------------------------------")
	for _, d := range diffs {
		curStr := fmt.Sprintf("%d", d.Current)
		if d.Current < 0 {
			curStr = "None (-1)"
		}
		fmt.Printf("%-24s | %-9d | %-15s | %-15d | %-10d\n", d.Topic, d.Partition, curStr, d.TargetLEO, d.Lag)
	}
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("Offset distance to target snapshot: %d (not an exact retained-message count)\n\n", totalLag)

	startTime := time.Now()
	fmt.Printf("[FAST-FORWARD] Committing target snapshot to group '%s' (partition results are independent)...\n", cfg.Group)
	targetOffsets := endOffsets.Offsets()
	if err := commitOffsetsVerified(ctx, adm, cfg.Group, targetOffsets); err != nil {
		return err
	}

	elapsed := time.Since(startTime)
	fmt.Printf("[FAST-FORWARD] Target offsets committed and verified for %d partitions in %v.\n", len(diffs), elapsed)
	stats := NewStatsManager()
	if err := refreshGroupLag(ctx, adm, cfg.Group, cfg.Topics, stats, time.Minute); err != nil {
		return fmt.Errorf("offsets committed, but final lag verification failed: %w", err)
	}
	fmt.Printf("[FAST-FORWARD] Remaining group lag (new snapshot): %s offset positions; producers may continue appending.\n", formatLag(stats.CalculateTotalLag()))
	return nil
}
