// Broker, Go implementation.
//
// Same HTTP API, same files on disk and same ZooKeeper data as broker/broker.py,
// so the producer, consumer and console cannot tell which one is running:
//
//	POST /create_topic/{topic}/{partitions}[?acks=]
//	POST /write/{topic}/{partition}              leader: store, then copy to followers
//	POST /replicate/{topic}/{partition}          follower: store what the leader sends
//	GET  /read/{topic}/{partition}[/offset/{n}]
//	GET  /offsets/{topic}/{partition}
//
// This file holds the settings and the HTTP side. cluster.go holds everything
// that involves ZooKeeper (membership, elections, rejoining, retention) and
// log.go the partition log.
package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-zookeeper/zk"
)

// --- settings (same variables as the Python broker) -----------------------------

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func envFloat(name string, fallback float64) float64 {
	v, err := strconv.ParseFloat(os.Getenv(name), 64)
	if err != nil {
		return fallback
	}
	return v
}

func seconds(v float64) time.Duration { return time.Duration(v * float64(time.Second)) }

var (
	brokerID       = env("HOSTNAME", "localhost")
	zookeeperHost  = env("ZOOKEEPER_HOST", "zookeeper:2181")
	listenAddr     = env("LISTEN_ADDR", ":5000")
	dataDir        = env("BROKER_DATA_DIR", "./data")
	requestTimeout = seconds(envFloat("BROKER_REQUEST_TIMEOUT", 5))
	sessionTimeout = seconds(envFloat("ZOOKEEPER_SESSION_TIMEOUT", 4))
	segmentMax     = int64(envFloat("SEGMENT_MAX_MESSAGES", 1000))
	retentionHours = envFloat("RETENTION_HOURS", 720)
	retentionEvery = seconds(envFloat("RETENTION_CHECK_SECONDS", 60))
	rejoinEvery    = seconds(envFloat("REJOIN_CHECK_SECONDS", 10))
	defaultAcks    = env("DEFAULT_ACKS", "all")
	batchMessages  = int(envFloat("REPLICATION_BATCH_MESSAGES", 500))
	internalToken  = os.Getenv("INTERNAL_TOKEN")
)

var (
	topicNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	validAcks   = map[string]bool{"all": true, "majority": true, "leader": true}
)

// One pool of open connections for all calls to other brokers.
var httpClient = &http.Client{
	Timeout: requestTimeout,
	Transport: &http.Transport{
		DialContext:         (&net.Dialer{Timeout: requestTimeout, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
	},
}

func call(method, target string, body any, timeout time.Duration) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if internalToken != "" {
		request.Header.Set("X-Internal-Token", internalToken)
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(response.Body)
	return response.StatusCode, answer, err
}

// --- replies in the same shape as the Python broker ----------------------------------

func reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func fail(w http.ResponseWriter, status int, detail any) {
	reply(w, status, map[string]any{"detail": detail})
}

// --- partition logs ------------------------------------------------------------------

var (
	logs   = map[string]*PartitionLog{}
	logsMu sync.Mutex
)

func partitionDir(topic string, partition int64) string {
	return filepath.Join(dataDir, topic, fmt.Sprintf("partition_%d", partition))
}

// partitionLog is this broker's copy of a partition.
func partitionLog(topic string, partition int64) (*PartitionLog, error) {
	key := fmt.Sprintf("%s/%d", topic, partition)
	logsMu.Lock()
	defer logsMu.Unlock()
	if existing := logs[key]; existing != nil {
		return existing, nil
	}
	opened, err := OpenPartitionLog(partitionDir(topic, partition), segmentMax)
	if err != nil {
		return nil, err
	}
	logs[key] = opened
	return opened, nil
}

// --- request helpers -----------------------------------------------------------------

func validTopic(w http.ResponseWriter, topic string) bool {
	if !topicNameRE.MatchString(topic) {
		fail(w, http.StatusBadRequest, "Invalid topic ID: use 1-128 letters, digits, '_' or '-'")
		return false
	}
	return true
}

func intArg(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	value, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, fmt.Sprintf("%s must be a whole number", name))
		return 0, false
	}
	return value, true
}

// internalOnly lets a request through only if it carries the cluster's shared secret.
func internalOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if internalToken != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Internal-Token")), []byte(internalToken)) != 1 {
			fail(w, http.StatusUnauthorized, "Brokers only answer the cluster's own services")
			return
		}
		next(w, r)
	}
}

// --- follower side ---------------------------------------------------------------------

type replicateRequest struct {
	Messages   []json.RawMessage `json:"messages"`
	BaseOffset *int64            `json:"base_offset"`
	Timestamps []float64         `json:"timestamps"`
	Reset      bool              `json:"reset"`
}

// replicate stores messages sent by the leader (see PartitionLog.Replicate).
func replicate(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	partition, ok := intArg(w, r, "partition")
	if !ok || !validTopic(w, topic) {
		return
	}
	var request replicateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Messages == nil {
		fail(w, http.StatusUnprocessableEntity, "Expected {\"messages\": [...]}")
		return
	}
	plog, err := partitionLog(topic, partition)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	end, behind, err := plog.Replicate(request.BaseOffset, request.Reset, request.Messages, request.Timestamps)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	if behind {
		fail(w, http.StatusConflict, map[string]any{"error": "Follower is behind the leader", "end_offset": end})
		return
	}
	reply(w, http.StatusOK, map[string]string{"status": "Replication successful", "broker": brokerID})
}

// --- leader side -----------------------------------------------------------------------

// followerLink is what the leader knows about one follower's copy of one partition.
//
// acked is the follower's end offset as far as the leader knows (-1 = unknown).
// The mutex lets one request at a time travel to that follower, so it always
// receives messages in log order. A write that finds it taken waits; when its
// turn comes, an earlier request has often carried its message already and
// there is nothing left to send. That is how messages get batched.
type followerLink struct {
	mu    sync.Mutex
	acked int64
}

var (
	links   = map[string]*followerLink{}
	linksMu sync.Mutex
)

func linkFor(topic string, partition int64, follower string) *followerLink {
	key := fmt.Sprintf("%s/%d/%s", topic, partition, follower)
	linksMu.Lock()
	defer linksMu.Unlock()
	if links[key] == nil {
		links[key] = &followerLink{acked: -1}
	}
	return links[key]
}

// forgetFollowers drops what we knew about followers of a partition (our own copy was just replaced).
func forgetFollowers(topic string, partition int64) {
	prefix := fmt.Sprintf("%s/%d/", topic, partition)
	linksMu.Lock()
	defer linksMu.Unlock()
	for key := range links {
		if strings.HasPrefix(key, prefix) {
			delete(links, key)
		}
	}
}

// replicateUpTo makes sure a follower has every message up to and including offset.
// It sends whatever the follower is missing, in log order, in batches.
func replicateUpTo(follower, topic string, partition, offset int64, plog *PartitionLog) bool {
	link := linkFor(topic, partition, follower)
	target := fmt.Sprintf("http://%s:5000/replicate/%s/%d", follower, topic, partition)
	link.mu.Lock()
	defer link.mu.Unlock()
	for range 100000 {
		if link.acked > offset {
			return true // an earlier request already carried this message
		}
		// Unknown position: assume the follower is right behind this message.
		// If it is further behind it answers 409 with where it really is.
		sendFrom := offset
		if link.acked >= 0 {
			sendFrom = link.acked
		}
		start, messages, timestamps, _, _, err := plog.Read(sendFrom, batchMessages)
		if err != nil || len(messages) == 0 {
			return false // nothing at that offset any more (purged meanwhile)
		}
		status, body, err := call(http.MethodPost, target, replicateRequest{
			Messages:   messages,
			Timestamps: timestamps,
			BaseOffset: &start,
			// what the follower lacks was purged here: it restarts from our oldest message
			Reset: start > sendFrom,
		}, requestTimeout)
		if err != nil {
			// An unreachable follower must not fail the write: it just leaves the ISR
			log.Printf("replication to %s failed for %s partition %d: %v", follower, topic, partition, err)
			link.acked = -1
			return false
		}
		var behind struct {
			Detail struct {
				EndOffset *int64 `json:"end_offset"`
			} `json:"detail"`
		}
		switch {
		case status == http.StatusOK:
			link.acked = start + int64(len(messages))
		case status == http.StatusConflict && json.Unmarshal(body, &behind) == nil &&
			behind.Detail.EndOffset != nil && *behind.Detail.EndOffset != sendFrom:
			link.acked = *behind.Detail.EndOffset // resend from where the follower really is
		default:
			log.Printf("replication to %s returned HTTP %d", follower, status)
			link.acked = -1
			return false
		}
	}
	return false
}

// followersNeeded is how many followers must have a message before the write is confirmed.
func followersNeeded(acks string, partition *partitionMeta, followers []string) int {
	switch acks {
	case "leader":
		return 0
	case "majority":
		replicas := max(len(partition.PreviousISR), 1+len(followers))
		return replicas / 2 // with the leader that makes a majority: 1 of 2 followers for 3 replicas
	}
	return len(followers)
}

func writeMessage(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	partition, ok := intArg(w, r, "partition")
	if !ok || !validTopic(w, topic) {
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<20))
	if err != nil || !json.Valid(raw) || !strings.HasPrefix(strings.TrimSpace(string(raw)), "{") {
		fail(w, http.StatusUnprocessableEntity, "The message must be a JSON object")
		return
	}

	// Validate everything before touching the log, so a rejected write leaves no trace
	metadata, err := metadataFor(topic)
	if errors.Is(err, zk.ErrNoNode) {
		fail(w, http.StatusNotFound, "Topic not found")
		return
	}
	if err != nil {
		fail(w, http.StatusServiceUnavailable, fmt.Sprintf("Error accessing ZooKeeper: %v", err))
		return
	}
	data := metadata.Partitions[strconv.FormatInt(partition, 10)]
	if data == nil {
		fail(w, http.StatusNotFound, "Partition not found")
		return
	}
	if data.Leader == nil {
		fail(w, http.StatusServiceUnavailable, "Partition has no leader")
		return
	}
	if *data.Leader != brokerID {
		fail(w, http.StatusConflict, map[string]string{"error": "Not the leader for this partition", "leader": *data.Leader})
		return
	}

	followers := without(data.Followers, brokerID)
	acks := metadata.Acks
	if acks == "" {
		acks = defaultAcks
	}
	needed := followersNeeded(acks, data, followers)
	if acks == "majority" && len(followers) < needed {
		// Consistency before availability: do not take a write that cannot reach a majority
		fail(w, http.StatusServiceUnavailable, "Too few replicas are available to confirm this write")
		return
	}

	plog, err := partitionLog(topic, partition)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	now := float64(time.Now().UnixNano()) / 1e9
	offset, err := plog.Append([]json.RawMessage{raw}, []float64{now})
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Every follower is sent the message at once. Each follower gets its messages in
	// log order (see followerLink), so writes do not have to queue behind each other.
	type outcome struct {
		index int
		ok    bool
	}
	outcomes := make(chan outcome, len(followers))
	for i, follower := range followers {
		go func() { outcomes <- outcome{i, replicateUpTo(follower, topic, partition, offset, plog)} }()
	}
	succeeded := make([]bool, len(followers))
	received, confirmed := 0, 0
	for received < len(followers) && confirmed < needed {
		result := <-outcomes
		received++
		succeeded[result.index] = result.ok
		if result.ok {
			confirmed++
		}
	}
	replicatedTo := []string{}
	for i, follower := range followers {
		if succeeded[i] {
			replicatedTo = append(replicatedTo, follower)
		}
	}

	// Wait for the followers that have not answered yet, then record who is in sync.
	finish := func(received int, succeeded []bool) {
		for ; received < len(followers); received++ {
			result := <-outcomes
			succeeded[result.index] = result.ok
		}
		inSync := []string{}
		for i, follower := range followers {
			if succeeded[i] {
				inSync = append(inSync, follower)
			}
		}
		// Update ISR only when it changed: followers that replicated stay in sync
		if equal(inSync, data.ISR) {
			return
		}
		key := strconv.FormatInt(partition, 10)
		err := updateTopicMetadata(topic, func(m *topicMeta) bool {
			p := m.Partitions[key]
			if p == nil || p.Leader == nil || *p.Leader != brokerID || equal(p.ISR, inSync) {
				return false
			}
			p.ISR = inSync
			return true
		})
		if err != nil {
			log.Printf("failed to update ISR for %s partition %d: %v", topic, partition, err)
		}
	}
	if received < len(followers) {
		go finish(received, append([]bool(nil), succeeded...)) // answer now; slower followers finish after the reply
	} else {
		finish(received, succeeded)
	}

	if acks == "majority" && confirmed < needed {
		// Stored here, but not on enough replicas to promise it survives a failure
		fail(w, http.StatusGatewayTimeout, "Too few replicas confirmed the message; it may or may not survive a failure")
		return
	}
	reply(w, http.StatusOK, map[string]any{
		"status":        "Message written and replicated",
		"broker":        brokerID,
		"offset":        offset,
		"acks":          acks,
		"replicated_to": replicatedTo,
	})
}

// --- reads -------------------------------------------------------------------------------

func readFrom(w http.ResponseWriter, topic string, partition, offset int64) {
	if !validTopic(w, topic) {
		return
	}
	plog, err := partitionLog(topic, partition)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !plog.Exists() {
		fail(w, http.StatusNotFound, "Partition not found")
		return
	}
	start, messages, timestamps, end, logStart, err := plog.Read(offset, 0)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	reply(w, http.StatusOK, map[string]any{
		"messages":         messages,
		"timestamps":       timestamps, // when each message was stored
		"start_offset":     start,      // offset of the first message returned
		"end_offset":       end,        // absolute offset to read from next
		"log_start_offset": logStart,   // oldest offset still stored (older ones were purged)
	})
}

func readFromOffset(w http.ResponseWriter, r *http.Request) {
	partition, ok := intArg(w, r, "partition")
	if !ok {
		return
	}
	offset, ok := intArg(w, r, "offset")
	if !ok {
		return
	}
	if offset < 0 {
		fail(w, http.StatusBadRequest, "Offset must not be negative")
		return
	}
	readFrom(w, r.PathValue("topic"), partition, offset)
}

func readAll(w http.ResponseWriter, r *http.Request) {
	if partition, ok := intArg(w, r, "partition"); ok {
		readFrom(w, r.PathValue("topic"), partition, 0)
	}
}

func partitionOffsets(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	partition, ok := intArg(w, r, "partition")
	if !ok || !validTopic(w, topic) {
		return
	}
	plog, err := partitionLog(topic, partition)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	stats := plog.Stats()
	reply(w, http.StatusOK, map[string]any{
		"broker": brokerID, "topic": topic, "partition": partition,
		"start_offset": stats.Start, "end_offset": stats.End, "segments": stats.Segments, "oldest_ts": stats.OldestTs,
	})
}

// --- topics ------------------------------------------------------------------------------

func createTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if !validTopic(w, topic) {
		return
	}
	partitions, ok := intArg(w, r, "partitions")
	if !ok {
		return
	}
	acks := r.URL.Query().Get("acks")
	if acks != "" && !validAcks[acks] {
		fail(w, http.StatusBadRequest, "acks must be one of: all, majority, leader")
		return
	}
	if partitions <= 0 {
		fail(w, http.StatusBadRequest, "Number of partitions must be a positive integer")
		return
	}
	if err := ensurePath("/topics"); err != nil {
		fail(w, http.StatusInternalServerError, fmt.Sprintf("Error accessing ZooKeeper: %v", err))
		return
	}
	path := "/topics/" + topic
	if exists, _, err := zkConn.Exists(path); err != nil {
		fail(w, http.StatusInternalServerError, fmt.Sprintf("Error accessing ZooKeeper: %v", err))
		return
	} else if exists {
		fail(w, http.StatusBadRequest, "Topic already exists")
		return
	}
	brokers, _, err := zkConn.Children("/brokers")
	if err != nil {
		fail(w, http.StatusInternalServerError, fmt.Sprintf("Error accessing ZooKeeper: %v", err))
		return
	}
	if len(brokers) == 0 {
		fail(w, http.StatusInternalServerError, "No available brokers to assign partitions")
		return
	}
	sort.Strings(brokers)

	metadata := &topicMeta{Partitions: map[string]*partitionMeta{}, Acks: acks}
	for p := range partitions {
		leader := brokers[int(p)%len(brokers)]
		followers := without(brokers, leader)
		metadata.Partitions[strconv.FormatInt(p, 10)] = &partitionMeta{
			Leader:      &leader,
			ISR:         append([]string{}, followers...), // all followers start in sync (leader excluded)
			Followers:   append([]string{}, followers...),
			PreviousISR: append(append([]string{}, followers...), leader),
		}
	}
	_, err = zkConn.Create(path, metadata.encode(), 0, zk.WorldACL(zk.PermAll))
	if errors.Is(err, zk.ErrNodeExists) {
		fail(w, http.StatusBadRequest, "Topic creation failed: Topic already exists")
		return
	}
	if err != nil {
		fail(w, http.StatusInternalServerError, fmt.Sprintf("Error creating topic in ZooKeeper: %v", err))
		return
	}
	reply(w, http.StatusOK, map[string]string{
		"status": fmt.Sprintf("Topic %s created with %d partitions and leader paths", topic, partitions),
	})
}

// --- small helpers -------------------------------------------------------------------------

func without(items []string, remove ...string) []string {
	kept := []string{}
	for _, item := range items {
		drop := false
		for _, r := range remove {
			if item == r {
				drop = true
			}
		}
		if !drop {
			kept = append(kept, item)
		}
	}
	return kept
}

func contains(items []string, wanted string) bool {
	for _, item := range items {
		if item == wanted {
			return true
		}
	}
	return false
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func main() {
	if !validAcks[defaultAcks] {
		log.Fatalf("DEFAULT_ACKS must be one of all, majority, leader (got %q)", defaultAcks)
	}
	if err := joinCluster(); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /create_topic/{topic}/{partitions}", internalOnly(createTopic))
	mux.HandleFunc("POST /write/{topic}/{partition}", internalOnly(writeMessage))
	mux.HandleFunc("POST /replicate/{topic}/{partition}", internalOnly(replicate))
	mux.HandleFunc("GET /read/{topic}/{partition}/offset/{offset}", internalOnly(readFromOffset))
	mux.HandleFunc("GET /read/{topic}/{partition}", internalOnly(readAll))
	mux.HandleFunc("GET /offsets/{topic}/{partition}", internalOnly(partitionOffsets))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		reply(w, http.StatusOK, map[string]string{"status": "ok", "broker": brokerID, "implementation": "go"})
	})
	server := &http.Server{Addr: listenAddr, Handler: mux}

	// On a normal stop (SIGTERM, `docker stop`), close the ZooKeeper session at once.
	// The other brokers then start the election immediately instead of waiting
	// for the session to time out.
	stop, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	go func() {
		<-stop.Done()
		zkConn.Close()
		// Keep answering for a moment: writes already on their way get a clean
		// "try the new leader" instead of a broken connection.
		time.Sleep(300 * time.Millisecond)
		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		server.Shutdown(ctx)
	}()

	log.Printf("Go broker %s listening on %s", brokerID, listenAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// used by cluster.go for calls that carry no body
func get(target string, timeout time.Duration) (int, []byte, error) {
	return call(http.MethodGet, target, nil, timeout)
}
