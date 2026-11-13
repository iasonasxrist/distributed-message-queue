package main

// On-disk log of one partition, stored as segment files. Same format as
// broker/partition_log.py, so a Go broker and a Python broker can read each
// other's data:
//
//	data/<topic>/partition_<n>/00000000000000000000.log    offsets 0 .. 999
//	data/<topic>/partition_<n>/00000000000000001000.log    offsets 1000 .. 1999
//
// A segment file is named after the offset of its first message and holds one
// JSON record per line: {"ts": <time stored>, "m": <the message>}.
//
// Old messages are removed by deleting whole files, which never changes the
// offset of the messages that remain.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type segment struct {
	path    string
	base    int64 // offset of the first message in this file
	count   int64
	firstTs float64
	lastTs  float64
}

func (s *segment) end() int64 { return s.base + s.count }

type record struct {
	Ts float64         `json:"ts"`
	M  json.RawMessage `json:"m"`
}

type recentRecord struct {
	offset int64
	record
}

// PartitionLog is safe for use by several goroutines.
type PartitionLog struct {
	mu          sync.Mutex
	dir         string
	segmentMax  int64
	segments    []*segment
	recent      []recentRecord // the newest messages, also kept in memory
	recentLimit int
	active      *os.File // the last segment, kept open for appending
}

func OpenPartitionLog(dir string, segmentMax int64) (*PartitionLog, error) {
	if segmentMax < 1 {
		segmentMax = 1
	}
	l := &PartitionLog{dir: dir, segmentMax: segmentMax, recentLimit: 2000}
	// The first versions stored a partition as a single file of bare messages
	if legacy := dir + ".log"; fileExists(legacy) && !dirExists(dir) {
		if err := l.importLegacy(legacy); err != nil {
			return nil, err
		}
	}
	if err := l.load(); err != nil {
		return nil, err
	}
	return l, nil
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// --- state ---------------------------------------------------------------------

// Exists is false until the first message is stored (or the log is reset).
func (l *PartitionLog) Exists() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.segments) > 0
}

func (l *PartitionLog) startOffset() int64 {
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].base
}

func (l *PartitionLog) endOffset() int64 {
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[len(l.segments)-1].end()
}

// End is the offset the next message will get.
func (l *PartitionLog) End() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.endOffset()
}

type logStats struct {
	Start    int64
	End      int64
	Segments int
	OldestTs *float64
}

func (l *PartitionLog) Stats() logStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	stats := logStats{Start: l.startOffset(), End: l.endOffset(), Segments: len(l.segments)}
	for _, s := range l.segments {
		if s.count > 0 {
			ts := s.firstTs
			stats.OldestTs = &ts
			break
		}
	}
	return stats
}

// --- reading -------------------------------------------------------------------

// Read returns the messages from offset to the end, or at most limit of them
// (limit <= 0 means no limit). start is the offset of the first message
// returned: it is moved forward if offset was already purged, and back to the
// end of the log if offset is past it.
func (l *PartitionLog) Read(offset int64, limit int) (start int64, messages []json.RawMessage, timestamps []float64, end int64, logStart int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	logStart, end = l.startOffset(), l.endOffset()
	start = offset
	if start < logStart {
		start = logStart
	}
	if start > end {
		start = end
	}
	messages, timestamps = []json.RawMessage{}, []float64{}

	if len(l.recent) > 0 && l.recent[0].offset <= start { // everything asked for is still in memory
		for _, r := range l.recent[start-l.recent[0].offset:] {
			if limit > 0 && len(messages) >= limit {
				break
			}
			messages = append(messages, r.M)
			timestamps = append(timestamps, r.Ts)
		}
		return
	}
	for _, s := range l.segments {
		if s.end() <= start {
			continue // entirely before the requested offset: do not open the file
		}
		records, readErr := readSegment(s.path)
		if readErr != nil {
			err = readErr
			return
		}
		skip := start - s.base
		if skip < 0 {
			skip = 0
		}
		for _, r := range records[skip:] {
			if limit > 0 && len(messages) >= limit {
				return
			}
			messages = append(messages, r.M)
			timestamps = append(timestamps, r.Ts)
		}
	}
	return
}

// --- writing -------------------------------------------------------------------

// Append stores messages at the end of the log and returns the offset of the
// first one. timestamps may be nil (now is used).
func (l *PartitionLog) Append(messages []json.RawMessage, timestamps []float64) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.append(messages, timestamps)
}

func (l *PartitionLog) append(messages []json.RawMessage, timestamps []float64) (int64, error) {
	first := l.endOffset()
	if len(l.segments) == 0 {
		if err := l.newSegment(first); err != nil {
			return 0, err
		}
	}
	if len(timestamps) != len(messages) {
		now := float64(time.Now().UnixNano()) / 1e9
		timestamps = make([]float64, len(messages))
		for i := range timestamps {
			timestamps[i] = now
		}
	}
	var buffer bytes.Buffer
	for i, message := range messages {
		s := l.segments[len(l.segments)-1]
		if s.count >= l.segmentMax {
			if err := l.flush(&buffer); err != nil {
				return 0, err
			}
			if err := l.newSegment(s.end()); err != nil {
				return 0, err
			}
			s = l.segments[len(l.segments)-1]
		}
		compact := new(bytes.Buffer)
		if err := json.Compact(compact, message); err != nil {
			return 0, fmt.Errorf("message %d is not valid JSON: %w", i, err)
		}
		message = compact.Bytes() // one line per record, whatever the sender's formatting
		line, err := json.Marshal(record{Ts: timestamps[i], M: message})
		if err != nil {
			return 0, err
		}
		buffer.Write(line)
		buffer.WriteByte('\n')
		l.remember(recentRecord{offset: s.end(), record: record{Ts: timestamps[i], M: message}})
		if s.count == 0 {
			s.firstTs = timestamps[i]
		}
		s.count++
		s.lastTs = timestamps[i]
	}
	return first, l.flush(&buffer)
}

func (l *PartitionLog) flush(buffer *bytes.Buffer) error {
	if buffer.Len() == 0 {
		return nil
	}
	_, err := l.active.Write(buffer.Bytes())
	buffer.Reset()
	return err
}

func (l *PartitionLog) remember(r recentRecord) {
	l.recent = append(l.recent, r)
	if len(l.recent) > 2*l.recentLimit { // trim in bulk, not on every message
		l.recent = append([]recentRecord(nil), l.recent[len(l.recent)-l.recentLimit:]...)
	}
}

// Replicate stores messages sent by the leader, keeping this copy identical to
// the leader's. baseOffset is the offset the first message has in the leader's
// log (nil = just append):
//   - behind the leader:  nothing is stored; behind is true and end says where we are
//   - ahead of the leader: the extra tail is dropped, the leader's log wins
//
// reset replaces this copy entirely, starting at baseOffset.
func (l *PartitionLog) Replicate(baseOffset *int64, reset bool, messages []json.RawMessage, timestamps []float64) (end int64, behind bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if reset && baseOffset != nil {
		return 0, false, l.reset(*baseOffset, messages, timestamps)
	}
	if baseOffset != nil {
		current := l.endOffset()
		if current < *baseOffset {
			return current, true, nil
		}
		if current > *baseOffset {
			if err := l.truncateTo(*baseOffset); err != nil {
				return 0, false, err
			}
		}
	}
	_, err = l.append(messages, timestamps)
	return l.endOffset(), false, err
}

// TruncateTo drops every message at offset and after (used when this copy is ahead of the leader).
func (l *PartitionLog) TruncateTo(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.truncateTo(offset)
}

func (l *PartitionLog) truncateTo(offset int64) error {
	if offset <= l.startOffset() {
		return l.reset(offset, nil, nil)
	}
	l.closeActive()
	for len(l.segments) > 0 && l.segments[len(l.segments)-1].base >= offset {
		last := l.segments[len(l.segments)-1]
		if err := os.Remove(last.path); err != nil {
			return err
		}
		l.segments = l.segments[:len(l.segments)-1]
	}
	s := l.segments[len(l.segments)-1]
	records, err := readSegment(s.path)
	if err != nil {
		return err
	}
	keep := records[:offset-s.base]
	var buffer bytes.Buffer
	for _, r := range keep {
		line, _ := json.Marshal(r)
		buffer.Write(line)
		buffer.WriteByte('\n')
	}
	if err := os.WriteFile(s.path, buffer.Bytes(), 0o644); err != nil {
		return err
	}
	s.count = int64(len(keep))
	if len(keep) > 0 {
		s.firstTs, s.lastTs = keep[0].Ts, keep[len(keep)-1].Ts
	}
	for len(l.recent) > 0 && l.recent[len(l.recent)-1].offset >= offset {
		l.recent = l.recent[:len(l.recent)-1]
	}
	return l.openActive()
}

// Reset replaces the whole log with messages, the first one at baseOffset.
func (l *PartitionLog) Reset(baseOffset int64, messages []json.RawMessage, timestamps []float64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reset(baseOffset, messages, timestamps)
}

func (l *PartitionLog) reset(baseOffset int64, messages []json.RawMessage, timestamps []float64) error {
	l.closeActive()
	if err := os.RemoveAll(l.dir); err != nil {
		return err
	}
	l.segments, l.recent = nil, nil
	if err := l.newSegment(baseOffset); err != nil {
		return err
	}
	if len(messages) > 0 {
		_, err := l.append(messages, timestamps)
		return err
	}
	return nil
}

// Purge deletes the oldest segments whose newest message was stored before
// olderThan and returns the number of messages removed. Only whole segments are
// removed, oldest first, so a segment holding even one newer message stays.
func (l *PartitionLog) Purge(olderThan float64) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var removed int64
	end := l.endOffset()
	for len(l.segments) > 0 {
		s := l.segments[0]
		if s.count == 0 || s.lastTs >= olderThan {
			break
		}
		if len(l.segments) == 1 {
			l.closeActive()
		}
		if err := os.Remove(s.path); err != nil {
			return removed, err
		}
		removed += s.count
		l.segments = l.segments[1:]
	}
	if removed > 0 && len(l.segments) == 0 {
		return removed, l.newSegment(end) // keeps the end offset when everything was purged
	}
	return removed, nil
}

// --- internals -----------------------------------------------------------------

func (l *PartitionLog) segmentPath(base int64) string {
	return filepath.Join(l.dir, fmt.Sprintf("%020d.log", base))
}

func (l *PartitionLog) newSegment(base int64) error {
	if err := os.MkdirAll(l.dir, 0o755); err != nil {
		return err
	}
	l.closeActive()
	l.segments = append(l.segments, &segment{path: l.segmentPath(base), base: base})
	return l.openActive()
}

func (l *PartitionLog) openActive() error {
	if len(l.segments) == 0 {
		return nil
	}
	file, err := os.OpenFile(l.segments[len(l.segments)-1].path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.active = file
	return nil
}

func (l *PartitionLog) closeActive() {
	if l.active != nil {
		l.active.Close()
		l.active = nil
	}
}

func readSegment(path string) ([]record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var records []record
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var r record
		if err := json.Unmarshal(line, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		records = append(records, r)
	}
	return records, scanner.Err()
}

func (l *PartitionLog) load() error {
	l.segments, l.recent = nil, nil
	entries, err := os.ReadDir(l.dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			continue
		}
		s := &segment{path: filepath.Join(l.dir, name), base: base}
		records, err := readSegment(s.path)
		if err != nil {
			return err
		}
		s.count = int64(len(records))
		if len(records) > 0 {
			s.firstTs, s.lastTs = records[0].Ts, records[len(records)-1].Ts
		}
		l.segments = append(l.segments, s)
	}
	sort.Slice(l.segments, func(i, j int) bool { return l.segments[i].base < l.segments[j].base })
	return l.openActive()
}

func (l *PartitionLog) importLegacy(legacy string) error {
	info, err := os.Stat(legacy)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(legacy)
	if err != nil {
		return err
	}
	var messages []json.RawMessage
	for _, line := range bytes.Split(data, []byte("\n")) {
		if line = bytes.TrimSpace(line); len(line) > 0 {
			messages = append(messages, json.RawMessage(append([]byte(nil), line...)))
		}
	}
	storedAt := float64(info.ModTime().UnixNano()) / 1e9
	timestamps := make([]float64, len(messages))
	for i := range timestamps {
		timestamps[i] = storedAt
	}
	if len(messages) == 0 {
		if err := l.newSegment(0); err != nil {
			return err
		}
	} else if _, err := l.append(messages, timestamps); err != nil {
		return err
	}
	l.closeActive()
	l.segments, l.recent = nil, nil
	return os.Remove(legacy)
}
