package main

import (
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
)

// The producer (Python or Go) and the consumer must agree on the partition of
// every key. These expected values were computed with the Python expression
//
//	int(hashlib.sha256(key.encode()).hexdigest(), 16) % partitions
func TestPartitionMatchesPython(t *testing.T) {
	cases := []struct {
		key        string
		partitions int
		want       int
	}{
		{"user123", 1, 0},
		{"user123", 3, 0},
		{"user123", 7, 6},
		{"user123", 16, 6},
		{"k", 1, 0},
		{"k", 3, 2},
		{"k", 7, 4},
		{"k", 16, 10},
		{"key-0", 1, 0},
		{"key-0", 3, 2},
		{"key-0", 7, 1},
		{"key-0", 16, 4},
		{"key-17", 1, 0},
		{"key-17", 3, 2},
		{"key-17", 7, 0},
		{"key-17", 16, 10},
		{"123", 1, 0},
		{"123", 3, 2},
		{"123", 7, 5},
		{"123", 16, 3},
		{"1.5", 1, 0},
		{"1.5", 3, 2},
		{"1.5", 7, 3},
		{"1.5", 16, 5},
		{"True", 1, 0},
		{"True", 3, 0},
		{"True", 7, 4},
		{"True", 16, 8},
		{"", 1, 0},
		{"", 3, 1},
		{"", 7, 1},
		{"", 16, 5},
		{"ünïcödé", 1, 0},
		{"ünïcödé", 3, 2},
		{"ünïcödé", 7, 0},
		{"ünïcödé", 16, 10},
		{"a much longer key with spaces", 1, 0},
		{"a much longer key with spaces", 3, 1},
		{"a much longer key with spaces", 7, 1},
		{"a much longer key with spaces", 16, 1},
	}
	for _, c := range cases {
		if got := partitionOf(c.key, c.partitions); got != c.want {
			t.Errorf("partitionOf(%q, %d) = %d, want %d", c.key, c.partitions, got, c.want)
		}
	}
}

// keyText must give what Python's str(key) gives.
func TestKeyTextMatchesPythonStr(t *testing.T) {
	cases := []struct {
		json string
		want string
		ok   bool
	}{
		{`"user123"`, "user123", true},
		{`123`, "123", true},
		{`1.5`, "1.5", true},
		{`true`, "True", true},
		{`false`, "False", true},
		{`null`, "", false},
	}
	for _, c := range cases {
		var value any
		decoder := json.NewDecoder(strings.NewReader(c.json))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			t.Fatalf("decoding %s: %v", c.json, err)
		}
		got, ok := keyText(value)
		if got != c.want || ok != c.ok {
			t.Errorf("keyText(%s) = %q, %v; want %q, %v", c.json, got, ok, c.want, c.ok)
		}
	}
}

// Only a failed connection attempt may be retried: anything else may have reached the broker.
func TestOnlyConnectionFailuresAreRetried(t *testing.T) {
	refused := &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	brokeOff := &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}
	if !neverReachedBroker(refused) {
		t.Error("a refused connection must be retried")
	}
	if neverReachedBroker(brokeOff) {
		t.Error("a connection that broke after sending must not be retried")
	}
	if neverReachedBroker(errors.New("timeout")) {
		t.Error("an unknown error must not be retried")
	}
}
