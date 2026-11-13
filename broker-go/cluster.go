package main

// Everything that involves ZooKeeper: announcing this broker, noticing brokers
// that die or arrive, electing leaders, rejoining partitions, and retention.
//
// ZooKeeper layout (shared with the Python services):
//
//	/brokers/<id>                 one ephemeral entry per running broker
//	/topics/<topic>               {"partitions": {"0": {"leader", "followers", "isr", "previous_isr"}}, "acks"}
//	/events/dead_broker/<id>      exists only while a dead broker is being handled
//	/config/retention             {"default_hours", "topics", "purge_before"}
//
// The leader is never listed in "followers" or "isr". A leader of null means
// the partition is offline.

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

var (
	zkConn *zk.Conn
	acl    = zk.WorldACL(zk.PermAll)
)

// --- topic metadata --------------------------------------------------------------------

type partitionMeta struct {
	Leader      *string  `json:"leader"`
	ISR         []string `json:"isr"`
	Followers   []string `json:"followers"`
	PreviousISR []string `json:"previous_isr"`
}

type topicMeta struct {
	Partitions map[string]*partitionMeta `json:"partitions"`
	Acks       string                    `json:"acks,omitempty"`
}

// encode writes lists as [] rather than null, which the Python services expect.
func (m *topicMeta) encode() []byte {
	for _, p := range m.Partitions {
		if p.ISR == nil {
			p.ISR = []string{}
		}
		if p.Followers == nil {
			p.Followers = []string{}
		}
		if p.PreviousISR == nil {
			p.PreviousISR = []string{}
		}
	}
	data, _ := json.Marshal(m)
	return data
}

func decodeTopic(data []byte) (*topicMeta, error) {
	m := &topicMeta{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	if m.Partitions == nil {
		m.Partitions = map[string]*partitionMeta{}
	}
	return m, nil
}

// Topic metadata is kept in memory and refreshed by a ZooKeeper watch, so a
// write does not have to ask ZooKeeper who the leader is every time.
var (
	topics   = map[string]*topicMeta{}
	watching = map[string]bool{}
	topicsMu sync.RWMutex
)

// metadataFor returns a topic's metadata; zk.ErrNoNode if the topic does not exist.
func metadataFor(topic string) (*topicMeta, error) {
	if zkConn.State() == zk.StateHasSession { // never trust the copy while cut off from ZooKeeper
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
		data, _, events, err = zkConn.GetW("/topics/" + topic)
	} else {
		data, _, err = zkConn.Get("/topics/" + topic)
	}
	var metadata *topicMeta
	if err == nil {
		metadata, err = decodeTopic(data)
	}
	if err != nil {
		if startWatch {
			forgetTopic(topic)
		}
		return nil, err
	}
	topicsMu.Lock()
	topics[topic] = metadata
	topicsMu.Unlock()
	if startWatch {
		go watchTopic(topic, events)
	}
	return metadata, nil
}

func forgetTopic(topic string) {
	topicsMu.Lock()
	delete(topics, topic)
	delete(watching, topic)
	topicsMu.Unlock()
}

func watchTopic(topic string, events <-chan zk.Event) {
	for {
		event := <-events
		if event.Type == zk.EventNodeDeleted {
			forgetTopic(topic)
			return
		}
		data, _, next, err := zkConn.GetW("/topics/" + topic)
		if err != nil {
			forgetTopic(topic) // deleted, or ZooKeeper trouble: the next write reads it afresh
			return
		}
		metadata, err := decodeTopic(data)
		if err != nil {
			forgetTopic(topic)
			return
		}
		topicsMu.Lock()
		topics[topic] = metadata
		topicsMu.Unlock()
		events = next
	}
}

// updateTopicMetadata is a read-modify-write of a topic that cannot lose a
// concurrent update: the write only succeeds if nobody else changed the topic
// in the meantime, otherwise the whole thing is retried. mutate returns false
// when there is nothing to change.
func updateTopicMetadata(topic string, mutate func(*topicMeta) bool) error {
	path := "/topics/" + topic
	for range 20 {
		data, stat, err := zkConn.Get(path)
		if err != nil {
			return err
		}
		metadata, err := decodeTopic(data)
		if err != nil {
			return err
		}
		if !mutate(metadata) {
			return nil
		}
		_, err = zkConn.Set(path, metadata.encode(), stat.Version)
		if errors.Is(err, zk.ErrBadVersion) {
			continue // someone else updated the topic, start over
		}
		return err
	}
	return fmt.Errorf("could not update metadata for %s after 20 attempts", topic)
}

func ensurePath(path string) error {
	current := ""
	for _, part := range strings.Split(strings.Trim(path, "/"), "/") {
		current += "/" + part
		if _, err := zkConn.Create(current, nil, 0, acl); err != nil && !errors.Is(err, zk.ErrNodeExists) {
			return err
		}
	}
	return nil
}

// --- joining the cluster ---------------------------------------------------------------

var registerMu sync.Mutex

// register announces this broker with an ephemeral node. After a crash and a
// quick restart the node of the previous run can still be there until its
// session times out; wait for it to disappear instead of failing.
func register(wait time.Duration) error {
	registerMu.Lock()
	defer registerMu.Unlock()
	path := "/brokers/" + brokerID
	payload, _ := json.Marshal(map[string]string{"host": "http://" + brokerID + ":5000"})
	deadline := time.Now().Add(wait)
	for {
		_, err := zkConn.Create(path, payload, zk.FlagEphemeral, acl)
		if err == nil {
			return nil
		}
		if !errors.Is(err, zk.ErrNodeExists) {
			return err
		}
		if _, stat, statErr := zkConn.Exists(path); statErr == nil && stat != nil && stat.EphemeralOwner == zkConn.SessionID() {
			return nil // already registered by this session
		}
		if time.Now().After(deadline) {
			return err
		}
		log.Printf("%s still exists from a previous run, waiting for it to expire", path)
		time.Sleep(time.Second)
	}
}

func joinCluster() error {
	conn, sessionEvents, err := zk.Connect(strings.Split(zookeeperHost, ","), sessionTimeout, zk.WithLogInfo(false))
	if err != nil {
		return fmt.Errorf("cannot connect to ZooKeeper at %s: %w", zookeeperHost, err)
	}
	zkConn = conn
	if err := ensurePath("/brokers"); err != nil {
		return err
	}
	if err := register(30 * time.Second); err != nil {
		return err
	}
	if err := ensurePath("/config"); err != nil {
		return err
	}
	// If the session is ever lost and replaced, announce this broker again
	go func() {
		for event := range sessionEvents {
			if event.State == zk.StateHasSession {
				if err := register(30 * time.Second); err != nil {
					log.Printf("could not register again after reconnecting: %v", err)
				}
			}
		}
	}()
	go watchBrokers()
	go watchRetention()
	if retentionEvery > 0 {
		go every(retentionEvery, func() { applyRetention(now()) })
	}
	if rejoinEvery > 0 {
		go every(rejoinEvery, func() { monitorNewBroker("") })
	}
	return nil
}

func every(interval time.Duration, action func()) {
	for range time.Tick(interval) {
		action()
	}
}

func now() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// --- brokers arriving and dying ---------------------------------------------------------

var (
	brokersSet = map[string]bool{}
	brokersMu  sync.RWMutex
)

func alive(broker string) bool {
	brokersMu.RLock()
	defer brokersMu.RUnlock()
	return brokersSet[broker]
}

func watchBrokers() {
	for {
		children, _, events, err := zkConn.ChildrenW("/brokers")
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		brokersMu.Lock()
		var dead, arrived []string
		current := map[string]bool{}
		for _, broker := range children {
			current[broker] = true
			if !brokersSet[broker] {
				arrived = append(arrived, broker)
			}
		}
		for broker := range brokersSet {
			if !current[broker] {
				dead = append(dead, broker)
			}
		}
		brokersSet = current
		brokersMu.Unlock()

		for _, broker := range dead {
			go monitorDeadBroker(broker)
		}
		if len(arrived) > 0 {
			sort.Strings(arrived)
			who := arrived[0]
			if contains(arrived, brokerID) {
				who = brokerID
			}
			go monitorNewBroker(who) // one pass is enough however many brokers arrived at once
		}
		<-events
	}
}

// monitorDeadBroker makes sure a dead broker is handled exactly once at a time:
// the brokers race to create /events/dead_broker/<id>/master_executor, ZooKeeper
// lets one win, and that broker does the repair. The others wait in case it dies too.
func monitorDeadBroker(dead string) {
	deadPath := "/events/dead_broker/" + dead
	executorPath := deadPath + "/master_executor"
	if err := ensurePath(deadPath); err != nil {
		log.Printf("cannot register the death of %s: %v", dead, err)
		return
	}
	for {
		_, err := zkConn.Create(executorPath, []byte(brokerID), zk.FlagEphemeral, acl)
		if err == nil {
			handleDeadBroker(dead, executorPath)
			return
		}
		if !errors.Is(err, zk.ErrNodeExists) {
			return // the event was fully cleaned up by another broker, or ZooKeeper is unavailable
		}
		// Someone else is the executor: wait until it is done, or dies
		exists, _, events, err := zkConn.ExistsW(executorPath)
		if err != nil {
			return
		}
		if exists {
			<-events
		}
		if stillThere, _, _ := zkConn.Exists(deadPath); !stillThere {
			return
		}
	}
}

func handleDeadBroker(dead, executorPath string) {
	log.Printf("handling the death of %s", dead)
	topicNames, _, err := zkConn.Children("/topics")
	if err != nil {
		topicNames = nil
	}
	for _, topic := range topicNames {
		err := updateTopicMetadata(topic, func(m *topicMeta) bool {
			changed := false
			for partition, p := range m.Partitions {
				before, _ := json.Marshal(p)
				liveISR := without(p.ISR, dead)

				// If the current leader fails, a new leader must be elected from
				// the replicas that are still alive and in sync
				if p.Leader != nil && *p.Leader == dead {
					if len(liveISR) > 0 {
						id, _ := strconv.ParseInt(partition, 10, 64)
						leader := mostCompleteReplica(topic, id, liveISR)
						p.Leader = &leader
						log.Printf("%s is now leader for %s partition %s", leader, topic, partition)
					} else {
						p.Leader = nil
						log.Printf("no in-sync replica left for %s partition %s: partition is offline", topic, partition)
					}
				}
				// The leader is never listed as its own follower / ISR member
				leader := ""
				if p.Leader != nil {
					leader = *p.Leader
				}
				p.ISR = without(liveISR, leader)
				p.Followers = without(p.Followers, leader, dead)
				after, _ := json.Marshal(p)
				changed = changed || string(before) != string(after)
			}
			return changed
		})
		if err != nil {
			log.Printf("failed to update metadata for %s: %v", topic, err)
		}
	}
	// Remove the event: the executor node, then its parent
	if err := zkConn.Delete(executorPath, -1); err == nil {
		zkConn.Delete(filepath.Dir(executorPath), -1)
	}
}

// replicaEndOffset is how far a broker's copy of a partition goes, or -1 if it does not answer.
func replicaEndOffset(broker, topic string, partition int64) int64 {
	if broker == brokerID {
		plog, err := partitionLog(topic, partition)
		if err != nil {
			return -1
		}
		return plog.End()
	}
	timeout := min(requestTimeout, 2*time.Second)
	status, body, err := get(fmt.Sprintf("http://%s:5000/offsets/%s/%d", broker, topic, partition), timeout)
	var answer struct {
		EndOffset *int64 `json:"end_offset"`
	}
	if err != nil || status != 200 || json.Unmarshal(body, &answer) != nil || answer.EndOffset == nil {
		return -1
	}
	return *answer.EndOffset
}

// mostCompleteReplica picks, of the live in-sync followers, the one whose copy
// goes furthest. With acks=majority or acks=leader a follower can still be
// marked in sync while it is a few messages behind; promoting the longest copy
// keeps every message that was confirmed. Ties go to the first candidate.
func mostCompleteReplica(topic string, partition int64, candidates []string) string {
	best, bestEnd := candidates[0], int64(-1)
	for _, broker := range candidates {
		if end := replicaEndOffset(broker, topic, partition); end > bestEnd {
			best, bestEnd = broker, end
		}
	}
	return best
}

// --- rejoining partitions ---------------------------------------------------------------

var rejoinMu sync.Mutex

// needsRejoin is true if this broker should hold the partition but is neither
// its leader nor an in-sync follower.
func needsRejoin(p *partitionMeta, mayClaim bool) bool {
	if !contains(p.PreviousISR, brokerID) {
		return false
	}
	if p.Leader == nil {
		return mayClaim
	}
	if *p.Leader == brokerID {
		return false
	}
	if !alive(*p.Leader) {
		return false // the leader is down: nothing to copy from yet
	}
	return !(contains(p.Followers, brokerID) && contains(p.ISR, brokerID))
}

// monitorNewBroker (re)joins the partitions this broker should hold a copy of.
// It runs when this broker starts, whenever another broker arrives, and every
// REJOIN_CHECK_SECONDS: if a leader we could not copy from earlier is back
// (for example after the whole cluster was stopped), we can copy it now.
func monitorNewBroker(arrived string) {
	rejoinMu.Lock()
	defer rejoinMu.Unlock()
	// Only a broker that has just started may take over an offline partition.
	// Otherwise a broker with an older copy could grab it the moment the broker
	// holding the newest copy comes back.
	mayClaim := arrived == brokerID

	if err := ensurePath("/topics"); err != nil {
		return
	}
	topicNames, _, err := zkConn.Children("/topics")
	if err != nil {
		return
	}
	for _, topic := range topicNames {
		data, _, err := zkConn.Get("/topics/" + topic)
		if err != nil {
			continue
		}
		metadata, err := decodeTopic(data)
		if err != nil {
			continue
		}
		ids := make([]string, 0, len(metadata.Partitions))
		for id := range metadata.Partitions {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			p := metadata.Partitions[id]
			if !needsRejoin(p, mayClaim) {
				continue
			}
			partition, err := strconv.ParseInt(id, 10, 64)
			if err != nil {
				continue
			}
			if p.Leader == nil {
				// The partition went offline because every replica died.
				// This broker holds a copy, so it brings the partition back.
				err := updateTopicMetadata(topic, func(m *topicMeta) bool {
					current := m.Partitions[id]
					if current == nil || current.Leader != nil {
						return false
					}
					self := brokerID
					current.Leader = &self
					current.ISR = without(current.ISR, brokerID)
					current.Followers = without(current.Followers, brokerID)
					return true
				})
				if err == nil {
					log.Printf("%s took over offline partition %s of %s", brokerID, id, topic)
				}
				continue
			}
			//TODO : No need to get a full snapshot of the partition data, only the missing data (offset difference)
			if !syncFromLeader(topic, partition, *p.Leader) {
				continue // stay out of the ISR until a sync succeeds
			}
			err = updateTopicMetadata(topic, func(m *topicMeta) bool {
				current := m.Partitions[id]
				if current == nil || (current.Leader != nil && *current.Leader == brokerID) {
					return false
				}
				changed := false
				if !contains(current.Followers, brokerID) {
					current.Followers = append(current.Followers, brokerID)
					changed = true
				}
				if !contains(current.ISR, brokerID) {
					current.ISR = append(current.ISR, brokerID)
					changed = true
				}
				return changed
			})
			if err != nil {
				log.Printf("failed to rejoin %s partition %s: %v", topic, id, err)
			}
		}
	}
}

// syncFromLeader replaces the local copy of a partition with the leader's copy.
func syncFromLeader(topic string, partition int64, leader string) bool {
	status, body, err := get(fmt.Sprintf("http://%s:5000/read/%s/%d", leader, topic, partition), requestTimeout)
	if err != nil {
		log.Printf("failed to reach leader %s for %s partition %d: %v", leader, topic, partition, err)
		return false
	}
	var copyOf struct {
		Messages    []json.RawMessage `json:"messages"`
		Timestamps  []float64         `json:"timestamps"`
		StartOffset int64             `json:"start_offset"`
	}
	switch status {
	case 404: // the leader has no log yet, so there is nothing to copy
	case 200:
		if err := json.Unmarshal(body, &copyOf); err != nil {
			return false
		}
	default:
		log.Printf("failed to sync from leader %s for %s partition %d: HTTP %d", leader, topic, partition, status)
		return false
	}
	plog, err := partitionLog(topic, partition)
	if err != nil {
		return false
	}
	// Swap the local log for the leader's copy in one step (avoids duplicates)
	if err := plog.Reset(copyOf.StartOffset, copyOf.Messages, copyOf.Timestamps); err != nil {
		log.Printf("failed to store the leader's copy of %s partition %d: %v", topic, partition, err)
		return false
	}
	forgetFollowers(topic, partition)
	log.Printf("%s synchronized %s partition %d from %s", brokerID, topic, partition, leader)
	return true
}

// --- retention: deleting old messages ----------------------------------------------------
//
// The cluster setting lives in ZooKeeper at /config/retention and is edited from
// the console. Every broker applies it to its own copies, so no broker has to be
// told directly and a broker that was down applies it when it returns.

type retentionSetting struct {
	DefaultHours *float64           `json:"default_hours"`
	Topics       map[string]float64 `json:"topics"`
	PurgeBefore  map[string]float64 `json:"purge_before"`
}

var (
	retention   retentionSetting
	retentionMu sync.RWMutex
	partitionRE = regexp.MustCompile(`^partition_(\d+)(\.log)?$`)
)

// retentionCutoff: messages of this topic stored before the returned time may be deleted.
func retentionCutoff(topic string, at float64) float64 {
	retentionMu.RLock()
	defer retentionMu.RUnlock()
	hours := retentionHours
	if retention.DefaultHours != nil {
		hours = *retention.DefaultHours
	}
	if own, ok := retention.Topics[topic]; ok {
		hours = own
	}
	cutoff := 0.0
	if hours > 0 {
		cutoff = at - hours*3600
	}
	return max(cutoff, retention.PurgeBefore[topic])
}

// applyRetention deletes expired segments of every partition stored on this broker.
func applyRetention(at float64) int64 {
	var removed int64
	topicDirs, err := os.ReadDir(dataDir)
	if err != nil {
		return 0
	}
	for _, topicDir := range topicDirs {
		topic := topicDir.Name()
		if !topicDir.IsDir() || !topicNameRE.MatchString(topic) {
			continue
		}
		cutoff := retentionCutoff(topic, at)
		if cutoff <= 0 {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(dataDir, topic))
		if err != nil {
			continue
		}
		for _, entry := range entries {
			match := partitionRE.FindStringSubmatch(entry.Name())
			if match == nil {
				continue
			}
			partition, _ := strconv.ParseInt(match[1], 10, 64)
			plog, err := partitionLog(topic, partition)
			if err != nil {
				continue
			}
			count, err := plog.Purge(cutoff)
			if err != nil {
				log.Printf("retention failed for %s partition %d: %v", topic, partition, err)
			}
			if count > 0 {
				removed += count
				log.Printf("purged %d messages from %s partition %d", count, topic, partition)
			}
		}
	}
	return removed
}

// watchRetention applies the setting at startup and every time it is changed from the console.
func watchRetention() {
	const path = "/config/retention"
	for {
		data, _, events, err := zkConn.GetW(path)
		if errors.Is(err, zk.ErrNoNode) {
			exists, _, created, existsErr := zkConn.ExistsW(path)
			if existsErr != nil {
				time.Sleep(time.Second)
				continue
			}
			if exists {
				continue // created between the two calls
			}
			setRetention(nil)
			applyRetention(now())
			<-created
			continue
		}
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		setRetention(data)
		applyRetention(now())
		<-events
	}
}

func setRetention(data []byte) {
	var setting retentionSetting
	if len(data) > 0 {
		if err := json.Unmarshal(data, &setting); err != nil {
			log.Printf("ignoring invalid retention setting in ZooKeeper")
			return
		}
	}
	retentionMu.Lock()
	retention = setting
	retentionMu.Unlock()
}
