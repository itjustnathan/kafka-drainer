package main

import (
	"testing"
)

func TestParseBytes(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		hasError bool
	}{
		{"0", 0, false},
		{"", 0, false},
		{"1024", 1024, false},
		{"1KB", 1024, false},
		{"10K", 10 * 1024, false},
		{"5MB", 5 * 1024 * 1024, false},
		{"20M", 20 * 1024 * 1024, false},
		{"1GB", 1024 * 1024 * 1024, false},
		{"2G", 2 * 1024 * 1024 * 1024, false},
		{"invalid", 0, true},
	}

	for _, tc := range tests {
		val, err := ParseBytes(tc.input)
		if tc.hasError {
			if err == nil {
				t.Errorf("expected error for %q, got nil", tc.input)
			}
		} else {
			if err != nil {
				t.Errorf("unexpected error for %q: %v", tc.input, err)
			}
			if val != tc.expected {
				t.Errorf("for %q expected %d, got %d", tc.input, tc.expected, val)
			}
		}
	}
}
