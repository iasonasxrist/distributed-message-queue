package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func msgs(count int, start int) []json.RawMessage {
	out := make([]json.RawMessage, count)
	for i := range out {
		out[i] = json.RawMessage(fmt.Sprintf(`{"n":%d}`, start+i))
	}
	return out
}

func stamps(count int, start float64) []float64 {
	out := make([]float64, count)
	for i := range out {
		out[i] = start + float64(i)
	}
	return out
}

func newLog(t *testing.T, dir string) *PartitionLog {
	t.Helper()
	l, err := OpenPartitionLog(filepath.Join(dir, "partition_0"), 5)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func read(t *testing.T, l *PartitionLog, offset int64, limit int) (int64, []string) {
	t.Helper()
	start, messages, _, _, _, err := l.Read(offset, limit)
	if err != nil {
		t.Fatal(err)
	}
	out := []string{}
	for _, m := range messages {
		out = append(out, string(m))
	}
	return start, out
}

func texts(messages []json.RawMessage) []string {
	out := []string{}
	for _, m := range messages {
		out = append(out, string(m))
	}
	return out
}

func bases(l *PartitionLog) []int64 {
	out := []int64{}
	for _, s := range l.segments {
		out = append(out, s.base)
	}
	return out
}

func check(t *testing.T, what string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: got %v, want %v", what, got, want)
	}
}

func TestSegmentsRollWhenFull(t *testing.T) {
	l := newLog(t, t.TempDir())
	first, err := l.Append(msgs(12, 0), nil)
	check(t, "error", err, nil)
	check(t, "first offset", first, int64(0))
	check(t, "segment bases", bases(l), []int64{0, 5, 10})
	check(t, "end", l.End(), int64(12))
	names, _ := filepath.Glob(filepath.Join(l.dir, "*.log"))
	check(t, "files", len(names), 3)
	check(t, "first file", filepath.Base(names[0]), "00000000000000000000.log")
}

func TestReadAcrossSegmentsWithLimit(t *testing.T) {
	l := newLog(t, t.TempDir())
	l.Append(msgs(12, 0), nil)
	start, got := read(t, l, 7, 0)
	check(t, "start", start, int64(7))
	check(t, "messages", got, texts(msgs(5, 7)))
	_, got = read(t, l, 3, 4)
	check(t, "limited", got, texts(msgs(4, 3)))
	start, got = read(t, l, 50, 0)
	check(t, "past the end", start, int64(12))
	check(t, "past the end", got, []string{})
}

func TestStateIsRebuiltFromDisk(t *testing.T) {
	dir := t.TempDir()
	l := newLog(t, dir)
	l.Append(msgs(12, 0), stamps(12, 100))
	reopened := newLog(t, dir)
	check(t, "bases", bases(reopened), []int64{0, 5, 10})
	_, got := read(t, reopened, 0, 0)
	check(t, "messages", got, texts(msgs(12, 0)))
	check(t, "last ts of first segment", reopened.segments[0].lastTs, 104.0)
	first, _ := reopened.Append(msgs(1, 12), nil)
	check(t, "append continues", first, int64(12))
}

func TestPurgeDeletesWholeOldSegmentsAndKeepsOffsets(t *testing.T) {
	l := newLog(t, t.TempDir())
	l.Append(msgs(12, 0), stamps(12, 100)) // stored at t=100..111
	removed, _ := l.Purge(104)             // segment 0 holds t=104: not older
	check(t, "nothing yet", removed, int64(0))
	removed, _ = l.Purge(105)
	check(t, "removed", removed, int64(5))
	check(t, "bases", bases(l), []int64{5, 10})
	start, got := read(t, l, 0, 0)
	check(t, "start moved forward", start, int64(5))
	check(t, "messages", got, texts(msgs(7, 5)))
	_, got = read(t, l, 8, 0)
	check(t, "offset 8 is still message 8", got, texts(msgs(4, 8)))
}

func TestPurgingEverythingKeepsTheEndOffset(t *testing.T) {
	dir := t.TempDir()
	l := newLog(t, dir)
	l.Append(msgs(12, 0), stamps(12, 100))
	removed, _ := l.Purge(200)
	check(t, "removed", removed, int64(12))
	stats := l.Stats()
	check(t, "start/end", []int64{stats.Start, stats.End}, []int64{12, 12})
	first, _ := l.Append(msgs(1, 12), nil)
	check(t, "next offset", first, int64(12))
	reopened := newLog(t, dir)
	stats = reopened.Stats()
	check(t, "after reopen", []int64{stats.Start, stats.End}, []int64{12, 13})
}

func TestTruncateAndReset(t *testing.T) {
	l := newLog(t, t.TempDir())
	l.Append(msgs(12, 0), nil)
	check(t, "truncate", l.TruncateTo(7), nil)
	check(t, "bases", bases(l), []int64{0, 5})
	_, got := read(t, l, 0, 0)
	check(t, "messages", got, texts(msgs(7, 0)))
	l.Append([]json.RawMessage{json.RawMessage(`{"n":"new"}`)}, nil)
	_, got = read(t, l, 7, 0)
	check(t, "after truncate", got, []string{`{"n":"new"}`})

	check(t, "reset", l.Reset(40, msgs(3, 40), stamps(3, 1)), nil)
	start, got := read(t, l, 0, 0)
	check(t, "reset start", start, int64(40))
	check(t, "reset messages", got, texts(msgs(3, 40)))
}

func TestReplicateKeepsTheCopyIdenticalToTheLeader(t *testing.T) {
	l := newLog(t, t.TempDir())
	base := int64(5)
	end, behind, _ := l.Replicate(&base, false, msgs(1, 5), nil) // we have nothing: behind
	check(t, "behind", behind, true)
	check(t, "our end", end, int64(0))
	check(t, "nothing stored", l.Exists(), false)

	base = 0
	l.Replicate(&base, false, msgs(3, 0), nil)
	base = 1
	_, behind, _ = l.Replicate(&base, false, []json.RawMessage{json.RawMessage(`{"n":"new"}`)}, nil) // we are ahead: leader wins
	check(t, "not behind", behind, false)
	_, got := read(t, l, 0, 0)
	check(t, "tail dropped", got, []string{`{"n":0}`, `{"n":"new"}`})

	base = 12
	l.Replicate(&base, true, msgs(2, 12), nil) // reset
	start, got := read(t, l, 0, 0)
	check(t, "reset start", start, int64(12))
	check(t, "reset messages", got, texts(msgs(2, 12)))
}

func TestMemoryAndDiskAlwaysAgree(t *testing.T) {
	dir := t.TempDir()
	l := newLog(t, dir)
	l.recentLimit = 4
	l.Append(msgs(12, 0), stamps(12, 100))
	l.TruncateTo(10)
	l.Append(msgs(3, 50), nil)
	l.Purge(105)
	reopened := newLog(t, dir) // reads everything from disk
	for offset := int64(0); offset < 14; offset++ {
		for _, limit := range []int{0, 2} {
			startA, a := read(t, l, offset, limit)
			startB, b := read(t, reopened, offset, limit)
			check(t, fmt.Sprintf("start at %d limit %d", offset, limit), startA, startB)
			check(t, fmt.Sprintf("messages at %d limit %d", offset, limit), a, b)
		}
	}
}

// A partition written by the Python broker must be readable here, and the other way round.
func TestReadsFilesWrittenByThePythonBroker(t *testing.T) {
	dir := t.TempDir()
	partition := filepath.Join(dir, "partition_0")
	os.MkdirAll(partition, 0o755)
	python := `{"ts": 1700000000.5, "m": {"key": "user123", "value": "one"}}` + "\n" +
		`{"ts": 1700000001.25, "m": {"key": 42, "value": [1, 2, {"x": null}]}}` + "\n"
	os.WriteFile(filepath.Join(partition, "00000000000000000007.log"), []byte(python), 0o644)

	l := newLog(t, dir)
	start, messages, timestamps, end, _, err := l.Read(0, 0)
	check(t, "error", err, nil)
	check(t, "start", start, int64(7))
	check(t, "end", end, int64(9))
	check(t, "timestamps", timestamps, []float64{1700000000.5, 1700000001.25})
	var first map[string]any
	json.Unmarshal(messages[0], &first)
	check(t, "first message", first, map[string]any{"key": "user123", "value": "one"})

	l.Append([]json.RawMessage{json.RawMessage(`{ "key" : "k",  "value" : 3 }`)}, []float64{5})
	data, _ := os.ReadFile(filepath.Join(partition, "00000000000000000007.log"))
	lines := 0
	for _, b := range data {
		if b == '\n' {
			lines++
		}
	}
	check(t, "one line per record", lines, 3)
}

func TestOldSingleFileLogsAreConverted(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, "partition_0.log")
	content := ""
	for _, m := range msgs(7, 0) {
		content += string(m) + "\n"
	}
	os.WriteFile(legacy, []byte(content), 0o644)
	l := newLog(t, dir)
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Error("the old file should be gone")
	}
	check(t, "bases", bases(l), []int64{0, 5})
	_, got := read(t, l, 0, 0)
	check(t, "messages", got, texts(msgs(7, 0)))
}

func TestFollowersNeeded(t *testing.T) {
	three := &partitionMeta{PreviousISR: []string{"b2", "b3", "b1"}}
	check(t, "all", followersNeeded("all", three, []string{"b2", "b3"}), 2)
	check(t, "majority of 3", followersNeeded("majority", three, []string{"b2", "b3"}), 1)
	check(t, "leader", followersNeeded("leader", three, []string{"b2", "b3"}), 0)
	two := &partitionMeta{PreviousISR: []string{"b2", "b1"}}
	check(t, "majority of 2", followersNeeded("majority", two, []string{"b2"}), 1)
}

func TestMetadataListsAreNeverNull(t *testing.T) {
	leader := "b1"
	m := &topicMeta{Partitions: map[string]*partitionMeta{"0": {Leader: &leader}}}
	check(t, "encoded", string(m.encode()), `{"partitions":{"0":{"leader":"b1","isr":[],"followers":[],"previous_isr":[]}}}`)
	m.Partitions["0"].Leader = nil
	m.Acks = "majority"
	check(t, "offline", string(m.encode()), `{"partitions":{"0":{"leader":null,"isr":[],"followers":[],"previous_isr":[]}},"acks":"majority"}`)
}
