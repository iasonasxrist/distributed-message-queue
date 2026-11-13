// Producer, Go implementation.
//
// Same public API and behaviour as producer/producer.py, so either can stand in
// for the other:
//
//	POST   /create_topic/{topic}/{partitions}[?acks=all|majority|leader]   (admin)
//	POST   /write/{topic}                                                  (any user)
//	DELETE /delete_topic/{topic}                                           (admin)
//
// It picks the partition from the message key, forwards the message to that
// partition's leader, and retries while the partition changes leader, but only
// when the message certainly was not stored.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

// --- settings (same variables as the Python producer) ---------------------------

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func envSeconds(name string, fallback float64) time.Duration {
	v, err := strconv.ParseFloat(env(name, ""), 64)
	if err != nil {
		v = fallback
	}
	return time.Duration(v * float64(time.Second))
}

var (
	zookeeperHost  = env("ZOOKEEPER_HOST", "zookeeper:2181")
	listenAddr     = env("LISTEN_ADDR", ":5000")
	brokerURL      = env("BROKER_URL", "http://broker:5000") // any broker, for creating topics
	requestTimeout = envSeconds("BROKER_REQUEST_TIMEOUT", 10)
	connectTimeout = envSeconds("BROKER_CONNECT_TIMEOUT", 2)
	retryFor       = envSeconds("PRODUCER_RETRY_SECONDS", 15)
	internalToken  = os.Getenv("INTERNAL_TOKEN")
	authEnabled    = !isOff(env("AUTH_ENABLED", "1"))
	authURL        = env("AUTH_URL", "http://auth:7000")
	authCacheFor   = envSeconds("AUTH_CACHE_SECONDS", 30)
)

func isOff(v string) bool {
	switch strings.ToLower(v) {
	case "0", "false", "no", "":
		return true
	}
	return false
}

// One pool of open connections for all calls to brokers and the auth service.
var httpClient = &http.Client{
	Timeout: requestTimeout,
	Transport: &http.Transport{
		DialContext:         (&net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
	},
}

// --- replies in the same shape as the Python services ----------------------------

func reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func fail(w http.ResponseWriter, status int, detail string) {
	reply(w, status, map[string]string{"detail": detail})
}

func passThrough(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}

// --- sign-in -----------------------------------------------------------------------
// The auth service decides who a request belongs to; the answer is remembered
// for AUTH_CACHE_SECONDS so most requests cost no extra call.

type user struct {
	Username           *string `json:"username"`
	Role               string  `json:"role"`
	MustChangePassword bool    `json:"must_change_password"`
}

type remembered struct {
	until time.Time
	user  user
}

var (
	verified   = map[string]remembered{}
	verifiedMu sync.RWMutex
)

const challenge = `Basic realm="pubsub"`

// currentUser returns the signed-in user, or writes the error reply and returns false.
func currentUser(w http.ResponseWriter, r *http.Request) (user, bool) {
	if !authEnabled {
		return user{Role: "admin"}, true
	}
	authorization := r.Header.Get("Authorization")
	if authorization == "" {
		w.Header().Set("WWW-Authenticate", challenge)
		fail(w, http.StatusUnauthorized, "Sign in first")
		return user{}, false
	}
	sum := sha256.Sum256([]byte(authorization))
	key := hex.EncodeToString(sum[:])
	verifiedMu.RLock()
	hit, ok := verified[key]
	verifiedMu.RUnlock()
	if ok && time.Now().Before(hit.until) {
		return hit.user, true // the usual case: no call, no waiting
	}

	request, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, authURL+"/verify", nil)
	request.Header.Set("Authorization", authorization)
	response, err := httpClient.Do(request)
	if err != nil {
		fail(w, http.StatusServiceUnavailable, "The auth service is not reachable")
		return user{}, false
	}
	defer response.Body.Close()
	switch {
	case response.StatusCode == http.StatusTooManyRequests:
		fail(w, http.StatusTooManyRequests, "Too many wrong passwords. Wait a minute and try again.")
		return user{}, false
	case response.StatusCode != http.StatusOK:
		w.Header().Set("WWW-Authenticate", challenge)
		fail(w, http.StatusUnauthorized, "Wrong username or password, or the sign-in has expired")
		return user{}, false
	}
	var u user
	if err := json.NewDecoder(response.Body).Decode(&u); err != nil {
		fail(w, http.StatusServiceUnavailable, "The auth service gave an unreadable answer")
		return user{}, false
	}
	verifiedMu.Lock()
	if len(verified) > 5000 {
		verified = map[string]remembered{}
	}
	verified[key] = remembered{until: time.Now().Add(authCacheFor), user: u}
	verifiedMu.Unlock()
	return u, true
}

func adminUser(w http.ResponseWriter, r *http.Request) bool {
	u, ok := currentUser(w, r)
	if !ok {
		return false
	}
	if u.Role != "admin" {
		fail(w, http.StatusForbidden, "Only administrators can do this")
		return false
	}
	return true
}

// --- topic metadata ------------------------------------------------------------------
// Kept in memory and refreshed by a ZooKeeper watch, so a write does not have to
// ask ZooKeeper who the leader is every time.

type partition struct {
	Leader *string `json:"leader"`
}

type topicMetadata struct {
	Partitions map[string]partition `json:"partitions"`
}

var (
	zkConn   *zk.Conn
	topics   = map[string]*topicMetadata{}
	watching = map[string]bool{}
	topicsMu sync.RWMutex
)

func topicPath(topic string) string { return "/topics/" + topic }

// metadataFor returns a topic's metadata; zk.ErrNoNode if the topic does not exist.
func metadataFor(topic string, refresh bool) (*topicMetadata, error) {
	if !refresh && zkConn.State() == zk.StateHasSession { // never trust the copy while cut off from ZooKeeper
		topicsMu.RLock()
		cached := topics[topic]
		topicsMu.RUnlock()
		if cached != nil {
			return cached, nil
		}
	}

	topicsMu.Lock()
	startWatch := !watching[topic]
	watching[topic] = true
	topicsMu.Unlock()

	var data []byte
	var events <-chan zk.Event
	var err error
	if startWatch {
		data, _, events, err = zkConn.GetW(topicPath(topic))
	} else {
		data, _, err = zkConn.Get(topicPath(topic))
	}
	if err != nil {
		if startWatch {
			forget(topic)
		}
		return nil, err
	}
	metadata, err := store(topic, data)
	if err != nil {
		if startWatch {
			forget(topic)
		}
		return nil, err
	}
	if startWatch {
		go watch(topic, events)
	}
	return metadata, nil
}

func store(topic string, data []byte) (*topicMetadata, error) {
	metadata := &topicMetadata{}
	if err := json.Unmarshal(data, metadata); err != nil {
		return nil, err
	}
	topicsMu.Lock()
	topics[topic] = metadata
	topicsMu.Unlock()
	return metadata, nil
}

func forget(topic string) {
	topicsMu.Lock()
	delete(topics, topic)
	delete(watching, topic)
	topicsMu.Unlock()
}

// watch keeps one topic's metadata current until the topic is deleted.
func watch(topic string, events <-chan zk.Event) {
	for {
		event := <-events
		if event.Type == zk.EventNodeDeleted {
			forget(topic)
			return
		}
		data, _, next, err := zkConn.GetW(topicPath(topic))
		if err != nil {
			forget(topic) // deleted, or ZooKeeper trouble: the next write reads it afresh
			return
		}
		if _, err := store(topic, data); err != nil {
			forget(topic)
			return
		}
		events = next
	}
}

// --- partitioning: must match producer.py and consumer.py exactly ---------------------

// keyText is Python's str(key) for the JSON values a key can sensibly be.
func keyText(key any) (string, bool) {
	switch k := key.(type) {
	case nil:
		return "", false
	case string:
		return k, true
	case json.Number:
		return k.String(), true
	case bool:
		if k {
			return "True", true
		}
		return "False", true
	default: // objects and arrays: use their JSON text (use string keys to be portable)
		text, _ := json.Marshal(k)
		return string(text), true
	}
}

// partitionOf is int(sha256(key).hexdigest(), 16) % partitions.
func partitionOf(key string, partitions int) int {
	sum := sha256.Sum256([]byte(key))
	n := new(big.Int).SetBytes(sum[:])
	return int(n.Mod(n, big.NewInt(int64(partitions))).Int64())
}

// --- handlers -------------------------------------------------------------------------

// neverReachedBroker is true when no connection could be made (refused, host
// gone, connect timeout). Only then is it safe to send the message again:
// after any other failure the broker may already have stored it.
func neverReachedBroker(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

func post(ctx context.Context, target string, body []byte) (int, []byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
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

func createTopic(w http.ResponseWriter, r *http.Request) {
	if !adminUser(w, r) {
		return
	}
	target := fmt.Sprintf("%s/create_topic/%s/%s", brokerURL, url.PathEscape(r.PathValue("topic")), url.PathEscape(r.PathValue("partitions")))
	if acks := r.URL.Query().Get("acks"); acks != "" {
		target += "?acks=" + url.QueryEscape(acks)
	}
	status, body, err := post(r.Context(), target, nil)
	if err != nil {
		fail(w, http.StatusServiceUnavailable, fmt.Sprintf("Broker unavailable: %v", err))
		return
	}
	passThrough(w, status, body)
}

func writeMessage(w http.ResponseWriter, r *http.Request) {
	if _, ok := currentUser(w, r); !ok {
		return
	}
	topic := r.PathValue("topic")
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<20))
	if err != nil {
		fail(w, http.StatusBadRequest, "The message could not be read")
		return
	}
	var message map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&message); err != nil || message == nil {
		fail(w, http.StatusUnprocessableEntity, "The message must be a JSON object")
		return
	}
	// Use key-based hashing to determine partition
	key, ok := keyText(message["key"])
	if !ok {
		fail(w, http.StatusBadRequest, "Key is required for partition selection")
		return
	}

	// While a leader is being replaced the write is tried again for up to
	// PRODUCER_RETRY_SECONDS, so the caller sees one slower write instead of an error.
	deadline := time.Now().Add(retryFor)
	delay := 100 * time.Millisecond
	for attempt := 0; ; attempt++ {
		metadata, err := metadataFor(topic, attempt > 0) // after a failed attempt, read it from ZooKeeper again
		if errors.Is(err, zk.ErrNoNode) {
			fail(w, http.StatusNotFound, "Topic not found")
			return
		}
		if err != nil {
			fail(w, http.StatusServiceUnavailable, fmt.Sprintf("Error accessing ZooKeeper: %v", err))
			return
		}
		if len(metadata.Partitions) == 0 {
			fail(w, http.StatusNotFound, "Topic not found")
			return
		}
		partitionID := partitionOf(key, len(metadata.Partitions))
		leader := metadata.Partitions[strconv.Itoa(partitionID)].Leader

		var problem string
		if leader == nil {
			problem = fmt.Sprintf("Partition %d has no leader", partitionID)
		} else {
			// Write message to leader broker (the original bytes: no need to encode them again)
			target := fmt.Sprintf("http://%s:5000/write/%s/%d", *leader, url.PathEscape(topic), partitionID)
			status, body, err := post(r.Context(), target, raw)
			switch {
			case err != nil && neverReachedBroker(err):
				// Connection refused or host gone: the message certainly was not stored, so try again
				problem = fmt.Sprintf("Leader %s of partition %d is unreachable", *leader, partitionID)
			case err != nil:
				// The broker may have stored the message before the connection broke.
				// Sending it again could store it twice, so the caller decides.
				fail(w, http.StatusGatewayTimeout, fmt.Sprintf("No confirmation from %s; the message may or may not have been stored", *leader))
				return
			case status != http.StatusConflict && status != http.StatusServiceUnavailable:
				passThrough(w, status, body)
				return
			default:
				// 409: that broker is no longer the leader. 503: no leader yet. Nothing was stored.
				problem = fmt.Sprintf("Partition %d is changing leader", partitionID)
			}
		}

		if time.Now().Add(delay).After(deadline) {
			fail(w, http.StatusServiceUnavailable, problem)
			return
		}
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return // the caller went away
		}
		if delay *= 2; delay > time.Second {
			delay = time.Second
		}
	}
}

func deleteTopic(w http.ResponseWriter, r *http.Request) {
	if !adminUser(w, r) {
		return
	}
	topic := r.PathValue("topic")
	err := zkConn.Delete(topicPath(topic), -1)
	if errors.Is(err, zk.ErrNoNode) {
		fail(w, http.StatusNotFound, "Topic not found")
		return
	}
	if err != nil {
		fail(w, http.StatusServiceUnavailable, fmt.Sprintf("Error accessing ZooKeeper: %v", err))
		return
	}
	reply(w, http.StatusOK, map[string]string{"status": fmt.Sprintf("Topic %s deleted", topic)})
}

func main() {
	conn, _, err := zk.Connect(strings.Split(zookeeperHost, ","), 10*time.Second, zk.WithLogInfo(false))
	if err != nil {
		log.Fatalf("cannot connect to ZooKeeper at %s: %v", zookeeperHost, err)
	}
	zkConn = conn

	mux := http.NewServeMux()
	mux.HandleFunc("POST /create_topic/{topic}/{partitions}", createTopic)
	mux.HandleFunc("POST /write/{topic}", writeMessage)
	mux.HandleFunc("DELETE /delete_topic/{topic}", deleteTopic)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		reply(w, http.StatusOK, map[string]string{"status": "ok", "implementation": "go"})
	})

	log.Printf("Go producer listening on %s (sign-in %v)", listenAddr, authEnabled)
	log.Fatal(http.ListenAndServe(listenAddr, mux))
}
