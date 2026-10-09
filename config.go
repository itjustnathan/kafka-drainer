package main

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Config encapsulates all CLI options for the Kafka consumer drainer.
type Config struct {
	Brokers     []string
	Group       string
	Topics      []string
	Action      string // "drain" or "fast-forward"
	Mode        string // "assign" (zero-rebalance direct partition assignment) or "group" (standard consumer group)
	Balancer    string // "auto", "range", "roundrobin", "sticky", "cooperative-sticky"
	ResetPolicy string // "earliest" or "latest" (fallback if no committed offset found)

	// Rate limiting & Broker protection
	MaxMsgPerSec   int64
	MaxBytesPerSec int64
	Workers        int // Concurrency workers (0 = auto 1 worker per partition)

	// Fetch & Commit tuning
	CommitInterval         time.Duration
	FetchMinBytes          int32
	FetchMaxBytes          int32
	MaxPartitionFetchBytes int32

	// Observability
	StatsInterval time.Duration
	MetricsAddr   string

	// Security (SASL / TLS)
	SASLMechanism string
	SASLUser      string
	SASLPass      string
	TLSEnable     bool
	TLSSkipVerify bool
}

// ParseBytes parses string size like "50MB", "100KB", "1GB" or pure numbers into bytes.
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" || s == "0" {
		return 0, nil
	}

	multiplier := int64(1)
	cleanStr := s
	if strings.HasSuffix(s, "G") || strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		cleanStr = strings.TrimSuffix(strings.TrimSuffix(s, "GB"), "G")
	} else if strings.HasSuffix(s, "M") || strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		cleanStr = strings.TrimSuffix(strings.TrimSuffix(s, "MB"), "M")
	} else if strings.HasSuffix(s, "K") || strings.HasSuffix(s, "KB") {
		multiplier = 1024
		cleanStr = strings.TrimSuffix(strings.TrimSuffix(s, "KB"), "K")
	} else if strings.HasSuffix(s, "B") {
		multiplier = 1
		cleanStr = strings.TrimSuffix(s, "B")
	}

	val, err := strconv.ParseFloat(strings.TrimSpace(cleanStr), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte format '%s': %w", s, err)
	}
	return int64(val * float64(multiplier)), nil
}

// LoadConfig parses flags from os.Args.
func LoadConfig() (*Config, error) {
	var (
		brokersStr     string
		topicsStr      string
		groupStr       string
		action         string
		mode           string
		balancer       string
		resetPolicy    string
		maxMsgPerSec   int64
		maxBytesStr    string
		workers        int
		commitInterval time.Duration
		fetchMinStr    string
		fetchMaxStr    string
		fetchPartStr   string
		statsInterval  time.Duration
		metricsAddr    string
		saslMech       string
		saslUser       string
		saslPass       string
		tlsEnable      bool
		tlsSkipVerify  bool
	)

	flag.StringVar(&brokersStr, "brokers", "", "Kafka broker list, comma-separated (e.g. 10.0.0.1:9092,10.0.0.2:9092) [Required]")
	flag.StringVar(&groupStr, "group", "", "Target consumer group ID to drain [Required]")
	flag.StringVar(&topicsStr, "topics", "", "Target backlog topics, comma-separated (e.g. topic_a,topic_b) [Required]")
	flag.StringVar(&action, "action", "drain", "Execution action: 'drain' (stream & discard) or 'fast-forward' (commit LEO snapshot; requires inactive group)")
	flag.StringVar(&mode, "mode", "assign", "Partition mode: 'assign' (direct assignment; requires inactive group) or 'group' (consumer group protocol; may rebalance)")
	flag.StringVar(&balancer, "balancer", "auto", "Group balancer for -mode=group: 'auto' (fallback match), 'range', 'roundrobin', 'sticky', 'cooperative-sticky'")
	flag.StringVar(&resetPolicy, "offset-reset", "earliest", "Offset reset policy if no committed offset exists: 'earliest' or 'latest'")

	flag.Int64Var(&maxMsgPerSec, "max-msg-per-sec", 0, "Global QPS rate limit (msgs/sec, 0 = unlimited) to protect Broker CPU/Disk")
	flag.StringVar(&maxBytesStr, "max-bytes-per-sec", "0", "Global bandwidth limit (e.g. 50MB, 100MB, 0 = unlimited) to prevent network saturation")
	flag.IntVar(&workers, "workers", 0, "Concurrency workers count (0 = auto 1 worker per partition)")

	flag.DurationVar(&commitInterval, "commit-interval", 2*time.Second, "Offset commit interval (e.g. 1s, 2s, 5s)")
	flag.StringVar(&fetchMinStr, "fetch-min-bytes", "1MB", "Min fetch bytes per request from broker (e.g. 512KB, 1MB)")
	flag.StringVar(&fetchMaxStr, "fetch-max-bytes", "20MB", "Max fetch bytes per request from broker (e.g. 10MB, 20MB)")
	flag.StringVar(&fetchPartStr, "fetch-part-bytes", "5MB", "Max fetch bytes per partition (e.g. 2MB, 5MB)")

	flag.DurationVar(&statsInterval, "stats-interval", 3*time.Second, "Terminal statistics log interval (e.g. 1s, 3s, 5s)")
	flag.StringVar(&metricsAddr, "metrics-addr", "", "Optional Prometheus HTTP metrics endpoint (e.g. :9100)")

	flag.StringVar(&saslMech, "sasl-mech", "", "SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")
	flag.StringVar(&saslUser, "sasl-user", "", "SASL username")
	flag.StringVar(&saslPass, "sasl-pass", "", "SASL password")
	flag.BoolVar(&tlsEnable, "tls", false, "Enable TLS connection")
	flag.BoolVar(&tlsSkipVerify, "tls-insecure-skip-verify", false, "Skip TLS server certificate verification")

	flag.Parse()

	if brokersStr == "" {
		return nil, errors.New("missing required flag: -brokers")
	}
	if groupStr == "" {
		return nil, errors.New("missing required flag: -group")
	}
	if topicsStr == "" {
		return nil, errors.New("missing required flag: -topics")
	}

	action = strings.ToLower(strings.TrimSpace(action))
	if action != "drain" && action != "fast-forward" {
		return nil, fmt.Errorf("invalid -action '%s': must be 'drain' or 'fast-forward'", action)
	}

	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode != "assign" && mode != "group" {
		return nil, fmt.Errorf("invalid -mode '%s': must be 'assign' or 'group'", mode)
	}

	balancer = strings.ToLower(strings.TrimSpace(balancer))
	switch balancer {
	case "auto", "range", "roundrobin", "sticky", "cooperative-sticky":
	default:
		return nil, fmt.Errorf("invalid -balancer '%s': must be auto, range, roundrobin, sticky, or cooperative-sticky", balancer)
	}

	resetPolicy = strings.ToLower(strings.TrimSpace(resetPolicy))
	if resetPolicy != "earliest" && resetPolicy != "latest" {
		return nil, fmt.Errorf("invalid -offset-reset '%s': must be 'earliest' or 'latest'", resetPolicy)
	}

	maxBytes, err := ParseBytes(maxBytesStr)
	if err != nil {
		return nil, fmt.Errorf("invalid -max-bytes-per-sec: %w", err)
	}

	fetchMin, err := ParseBytes(fetchMinStr)
	if err != nil {
		return nil, fmt.Errorf("invalid -fetch-min-bytes: %w", err)
	}

	fetchMax, err := ParseBytes(fetchMaxStr)
	if err != nil {
		return nil, fmt.Errorf("invalid -fetch-max-bytes: %w", err)
	}

	fetchPart, err := ParseBytes(fetchPartStr)
	if err != nil {
		return nil, fmt.Errorf("invalid -fetch-part-bytes: %w", err)
	}

	var brokers []string
	for _, b := range strings.Split(brokersStr, ",") {
		b = strings.TrimSpace(b)
		if b != "" {
			brokers = append(brokers, b)
		}
	}

	var topics []string
	for _, t := range strings.Split(topicsStr, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			topics = append(topics, t)
		}
	}

	cfg := &Config{
		Brokers:                brokers,
		Group:                  strings.TrimSpace(groupStr),
		Topics:                 topics,
		Action:                 action,
		Mode:                   mode,
		Balancer:               balancer,
		ResetPolicy:            resetPolicy,
		MaxMsgPerSec:           maxMsgPerSec,
		MaxBytesPerSec:         maxBytes,
		Workers:                workers,
		CommitInterval:         commitInterval,
		FetchMinBytes:          int32(fetchMin),
		FetchMaxBytes:          int32(fetchMax),
		MaxPartitionFetchBytes: int32(fetchPart),
		StatsInterval:          statsInterval,
		MetricsAddr:            strings.TrimSpace(metricsAddr),
		SASLMechanism:          strings.ToUpper(strings.TrimSpace(saslMech)),
		SASLUser:               saslUser,
		SASLPass:               saslPass,
		TLSEnable:              tlsEnable,
		TLSSkipVerify:          tlsSkipVerify,
	}

	return cfg, nil
}

// PrintSummary outputs sanitized configuration for production sanity check.
func (c *Config) PrintSummary() {
	fmt.Println("================================================================================")
	fmt.Println("             HIGH-PERFORMANCE KAFKA CONSUMER DRAINER (PRODUCTION)               ")
	fmt.Println("================================================================================")
	fmt.Printf("Brokers          : %s\n", strings.Join(c.Brokers, ", "))
	fmt.Printf("Target Group     : %s\n", c.Group)
	fmt.Printf("Backlog Topics   : %s\n", strings.Join(c.Topics, ", "))
	fmt.Printf("Execution Action : %s (drain=stream & discard, fast-forward=instant jump to LEO)\n", c.Action)
	fmt.Printf("Partition Mode   : %s (assign=requires inactive group, group=may rebalance)\n", c.Mode)
	if c.Mode == "group" {
		fmt.Printf("Group Balancer   : %s\n", c.Balancer)
	}
	fmt.Printf("Offset Reset     : %s\n", c.ResetPolicy)
	fmt.Printf("Rate Limit (Msg) : %d msg/s (0 = unlimited)\n", c.MaxMsgPerSec)
	fmt.Printf("Rate Limit (Net) : %d B/s (0 = unlimited)\n", c.MaxBytesPerSec)
	fmt.Printf("Commit Interval  : %s\n", c.CommitInterval)
	fmt.Printf("Fetch Min / Max  : %d B / %d B (Partition Max: %d B)\n", c.FetchMinBytes, c.FetchMaxBytes, c.MaxPartitionFetchBytes)
	if c.SASLMechanism != "" {
		fmt.Printf("SASL Auth        : %s (User: %s)\n", c.SASLMechanism, c.SASLUser)
	} else {
		fmt.Printf("SASL Auth        : Disabled\n")
	}
	fmt.Printf("TLS Enabled      : %v\n", c.TLSEnable)
	if c.MetricsAddr != "" {
		fmt.Printf("Prometheus Metr. : http://%s/metrics\n", c.MetricsAddr)
	}
	fmt.Println("================================================================================")
}
