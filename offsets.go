package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

type offsetAdmin interface {
	DescribeGroups(context.Context, ...string) (kadm.DescribedGroups, error)
	ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
	FetchOffsetsForTopics(context.Context, string, ...string) (kadm.OffsetResponses, error)
	CommitOffsets(context.Context, string, kadm.Offsets) (kadm.OffsetResponses, error)
}

func requireInactiveGroup(ctx context.Context, admin offsetAdmin, group string) error {
	groups, err := admin.DescribeGroups(ctx, group)
	if err != nil {
		return fmt.Errorf("describe group %q: %w", group, err)
	}
	description, exists := groups[group]
	if !exists {
		return fmt.Errorf("missing group description for %q", group)
	}
	if description.Err != nil {
		return fmt.Errorf("describe group %q: %w", group, description.Err)
	}
	if len(description.Members) != 0 || (description.State != "Empty" && description.State != "Dead") {
		return fmt.Errorf("group %q must be inactive for assign/fast-forward: state=%s members=%d; stop ALL consumers in this group (including other topics), or use drain -mode group with an approved rebalance window", group, description.State, len(description.Members))
	}
	return nil
}

func commitOffsetsVerified(ctx context.Context, admin offsetAdmin, group string, offsets kadm.Offsets) error {
	if len(offsets) == 0 {
		return nil
	}
	if err := requireInactiveGroup(ctx, admin, group); err != nil {
		return err
	}
	responses, err := admin.CommitOffsets(ctx, group, offsets)
	if err != nil {
		return fmt.Errorf("offset commit: %w", err)
	}
	topics := make([]string, 0, len(offsets))
	for topic, partitions := range offsets {
		topics = append(topics, topic)
		for partition := range partitions {
			response, exists := responses.Lookup(topic, partition)
			if !exists {
				return fmt.Errorf("offset commit response missing for %s-%d; partial commits may have succeeded", topic, partition)
			}
			if response.Err != nil {
				return fmt.Errorf("offset commit rejected for %s-%d: %w; partial commits may have succeeded", topic, partition, response.Err)
			}
		}
	}
	sort.Strings(topics)
	committed, err := admin.FetchOffsetsForTopics(ctx, group, topics...)
	if err != nil {
		return fmt.Errorf("commit verification: %w", err)
	}
	for topic, partitions := range offsets {
		for partition, target := range partitions {
			actual, exists := committed.Lookup(topic, partition)
			if !exists || actual.Err != nil || actual.At != target.At {
				return fmt.Errorf("commit verification failed for %s-%d: expected=%d actual=%d present=%t error=%v", topic, partition, target.At, actual.At, exists, actual.Err)
			}
		}
	}
	return nil
}

func validateEndOffsets(offsets kadm.ListedOffsets, topics []string) error {
	if err := offsets.Error(); err != nil {
		return fmt.Errorf("partition end offsets: %w", err)
	}
	for _, topic := range topics {
		if len(offsets[topic]) == 0 {
			return fmt.Errorf("missing end offsets for topic %q", topic)
		}
		for partition, offset := range offsets[topic] {
			if offset.Offset < 0 {
				return fmt.Errorf("invalid end offset for %s-%d: %d", topic, partition, offset.Offset)
			}
		}
	}
	return nil
}

func refreshGroupLag(ctx context.Context, admin offsetAdmin, group string, topics []string, stats *StatsManager, validity time.Duration) (err error) {
	defer func() {
		if err != nil {
			stats.SetGroupLag(-1, 0)
		}
	}()
	ends, err := admin.ListEndOffsets(ctx, topics...)
	if err != nil {
		return err
	}
	if err := validateEndOffsets(ends, topics); err != nil {
		return err
	}
	committed, err := admin.FetchOffsetsForTopics(ctx, group, topics...)
	if err != nil {
		return err
	}
	var lag int64
	for topic, partitions := range ends {
		for partition, end := range partitions {
			current, exists := committed.Lookup(topic, partition)
			if !exists || current.Err != nil || current.At < 0 {
				return fmt.Errorf("group lag unavailable for %s-%d: offset=%d present=%t error=%v", topic, partition, current.At, exists, current.Err)
			}
			if end.Offset > current.At {
				lag += end.Offset - current.At
			}
			stats.SetEndOffset(topic, partition, end.Offset)
		}
	}
	stats.SetGroupLag(lag, validity)
	return nil
}
