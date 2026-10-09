package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

type offsetAdminStub struct {
	groups    kadm.DescribedGroups
	ends      kadm.ListedOffsets
	committed kadm.OffsetResponses
	responses kadm.OffsetResponses
	commitErr error
	commits   int
}

func (stub *offsetAdminStub) DescribeGroups(context.Context, ...string) (kadm.DescribedGroups, error) {
	return stub.groups, nil
}

func (stub *offsetAdminStub) ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error) {
	return stub.ends, nil
}

func (stub *offsetAdminStub) FetchOffsetsForTopics(context.Context, string, ...string) (kadm.OffsetResponses, error) {
	return stub.committed, nil
}

func (stub *offsetAdminStub) CommitOffsets(context.Context, string, kadm.Offsets) (kadm.OffsetResponses, error) {
	stub.commits++
	return stub.responses, stub.commitErr
}

func TestCheckedCommit(t *testing.T) {
	target := kadm.Offset{Topic: "alarm", Partition: 0, At: 100}
	for _, test := range []struct {
		name         string
		state        string
		partitionErr error
		requestErr   error
		readback     int64
		missing      bool
		wantError    string
	}{
		{name: "verified", state: "Empty", readback: 100},
		{name: "active group", state: "Stable", readback: 100, wantError: "inactive"},
		{name: "partition rejection", state: "Empty", partitionErr: kerr.UnknownMemberID, readback: 100, wantError: "UNKNOWN_MEMBER_ID"},
		{name: "request failure", state: "Empty", requestErr: errors.New("connection lost"), wantError: "connection lost"},
		{name: "offset not stored", state: "Empty", readback: 20, wantError: "verification"},
		{name: "missing response", state: "Empty", missing: true, readback: 100, wantError: "missing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stub := &offsetAdminStub{
				groups:    kadm.DescribedGroups{"group": {Group: "group", State: test.state}},
				responses: kadm.OffsetResponses{"alarm": {0: {Offset: target, Err: test.partitionErr}}},
				committed: kadm.OffsetResponses{"alarm": {0: {Offset: kadm.Offset{Topic: "alarm", Partition: 0, At: test.readback}}}},
				commitErr: test.requestErr,
			}
			if test.missing {
				stub.responses = nil
			}
			err := commitOffsetsVerified(context.Background(), stub, "group", kadm.Offsets{"alarm": {0: target}})
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("expected %q, got %v", test.wantError, err)
			}
			if test.state == "Stable" && stub.commits != 0 {
				t.Fatal("active group was modified")
			}
		})
	}
}

func TestRefreshGroupLagUsesBrokerOffsets(t *testing.T) {
	stub := &offsetAdminStub{
		ends:      kadm.ListedOffsets{"alarm": {0: {Topic: "alarm", Partition: 0, Offset: 100}}},
		committed: kadm.OffsetResponses{"alarm": {0: {Offset: kadm.Offset{Topic: "alarm", Partition: 0, At: 20}}}},
	}
	stats := NewStatsManager()
	stats.SetEndOffset("alarm", 0, 100)
	stats.UpdateOffset("alarm", 0, 100)
	if err := refreshGroupLag(context.Background(), stub, "group", []string{"alarm"}, stats, time.Minute); err != nil {
		t.Fatal(err)
	}
	if lag := stats.CalculateTotalLag(); lag != 80 {
		t.Fatalf("expected committed lag 80, got %d", lag)
	}
	stub.ends["alarm"][0] = kadm.ListedOffset{Topic: "alarm", Partition: 0, Offset: 150}
	if err := refreshGroupLag(context.Background(), stub, "group", []string{"alarm"}, stats, time.Minute); err != nil {
		t.Fatal(err)
	}
	if lag := stats.CalculateTotalLag(); lag != 130 {
		t.Fatalf("expected refreshed lag 130, got %d", lag)
	}
	stub.committed["alarm"][0] = kadm.OffsetResponse{Err: kerr.GroupAuthorizationFailed}
	if err := refreshGroupLag(context.Background(), stub, "group", []string{"alarm"}, stats, time.Minute); err == nil {
		t.Fatal("expected authorization error")
	}
	if lag := stats.CalculateTotalLag(); lag != -1 {
		t.Fatalf("failed refresh must invalidate lag, got %d", lag)
	}
}

func TestGroupLagExpires(t *testing.T) {
	stats := NewStatsManager()
	stats.SetGroupLag(0, -time.Second)
	if lag := stats.CalculateTotalLag(); lag != -1 {
		t.Fatalf("stale zero must be unknown, got %d", lag)
	}
}

func TestInactiveGroupValidation(t *testing.T) {
	for _, test := range []struct {
		name      string
		groups    kadm.DescribedGroups
		wantError bool
	}{
		{name: "empty", groups: kadm.DescribedGroups{"group": {State: "Empty"}}},
		{name: "dead", groups: kadm.DescribedGroups{"group": {State: "Dead"}}},
		{name: "missing", wantError: true},
		{name: "unauthorized", groups: kadm.DescribedGroups{"group": {Err: kerr.GroupAuthorizationFailed}}, wantError: true},
		{name: "rebalancing", groups: kadm.DescribedGroups{"group": {State: "PreparingRebalance"}}, wantError: true},
		{name: "members", groups: kadm.DescribedGroups{"group": {State: "Empty", Members: []kadm.DescribedGroupMember{{}}}}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := requireInactiveGroup(context.Background(), &offsetAdminStub{groups: test.groups}, "group")
			if (err != nil) != test.wantError {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestLagIncompleteSnapshotsAreUnknown(t *testing.T) {
	for _, test := range []struct {
		name      string
		ends      kadm.ListedOffsets
		committed kadm.OffsetResponses
	}{
		{name: "missing topic"},
		{name: "partition metadata error", ends: kadm.ListedOffsets{"alarm": {0: {Err: kerr.UnknownTopicOrPartition}}}},
		{name: "negative LEO", ends: kadm.ListedOffsets{"alarm": {0: {Offset: -1}}}},
		{name: "missing committed partition", ends: kadm.ListedOffsets{"alarm": {0: {Offset: 100}}}},
		{name: "uninitialized offset", ends: kadm.ListedOffsets{"alarm": {0: {Offset: 100}}}, committed: kadm.OffsetResponses{"alarm": {0: {Offset: kadm.Offset{At: -1}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			stats := NewStatsManager()
			stats.SetGroupLag(0, time.Minute)
			stub := &offsetAdminStub{ends: test.ends, committed: test.committed}
			if err := refreshGroupLag(context.Background(), stub, "group", []string{"alarm"}, stats, time.Minute); err == nil {
				t.Fatal("expected snapshot error")
			}
			if lag := stats.CalculateTotalLag(); lag != -1 {
				t.Fatalf("expected unknown lag, got %d", lag)
			}
		})
	}
}
