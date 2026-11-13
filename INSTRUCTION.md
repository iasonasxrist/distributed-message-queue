# Getting to know the project

A guided tour for someone opening this repository for the first time. Read it
top to bottom once; it takes about 20 minutes. By the end you will know what
each file does, how a message travels through the system, and what happens
when a broker dies.

## 1. The idea in one minute

Applications publish messages to a **topic** and other applications read them
later, each at its own pace. To survive machine failures, every message is
stored on several **brokers**. One broker is in charge of each slice of a topic
and the others copy it. If the one in charge dies, a copy takes over.

That is the whole project. Everything else is detail about doing it correctly.

## 2. Eight words you need

| Word | Meaning here |
|---|---|
| **Topic** | A named stream of messages, for example `orders`. |
| **Partition** | A topic is split into numbered partitions. Each partition is one append-only file. |
| **Key** | A field in every message. The same key always goes to the same partition, which keeps those messages in order. |
| **Offset** | The position of a message in its partition: 0, 1, 2 ... |
| **Broker** | A server that stores partitions. The default setup runs three. |
| **Leader** | The one broker that accepts writes for a partition. |
| **Follower** | A broker holding a copy of a partition. The leader pushes every message to it. |
| **In-sync replica (ISR)** | A follower whose copy is complete. Only these may become leader. |

## 3. The pieces

| Folder | What runs there | Read this first |
|---|---|---|
| `producer/producer.py` | The write API applications call. About 70 lines. | `write_message` |
| `producer-go/main.go` | The same producer in Go, off by default. About 490 lines. | `writeMessage` |
| `broker-go/` | The same broker in Go, off by default: `main.go` (HTTP side), `cluster.go` (ZooKeeper side), `log.go` (partition log). About 1,900 lines. | `writeMessage` in `main.go` |
| `.env` | Which broker and producer run: Python or Go. | the comments in the file |
| `consumer/consumer.py` | The read API applications call. About 110 lines. | `read_by_key` |
| `broker/broker.py` | Replication and all the cluster logic. About 800 lines. | `write_message`, then `handle_dead_broker` |
| `broker/partition_log.py` | How one partition is stored on disk as segment files. About 200 lines. | the docstring at the top |
| `settings.env` | The settings you are likely to change: retention, failover timing. | the comments in the file |
| `auth/auth.py` | Users, passwords and sign-in. About 350 lines. | `authenticate` |
| `console/` | The web console: `console.py` builds the data, `index.html` draws it. | `build_snapshot` |
| `tests/` | The test suite. `conftest.py` runs a full cluster in one process. | `test_failover.py` |
| `stress/stress_test.py` | Load and failure test against a running cluster. | the docstring at the top |

Two outside programs do the bookkeeping:

- **ZooKeeper** remembers which brokers are alive and who leads each partition. Every service reads it; only brokers change it.
- **Redis** remembers how far each subscriber has read.

Every public service asks the auth service who a request belongs to
(`current_user` in `producer.py`, `consumer.py` and `console.py`) and remembers
the answer for 30 seconds, so signing in adds almost nothing to a request.

## 4. What is stored where

**ZooKeeper**

```
/brokers/<broker id>          one entry per running broker; disappears when the broker dies
/topics/<topic>               one entry per topic, holding all its partitions:
    {"partitions": {
        "0": {"leader": "b1",
              "followers": ["b2", "b3"],     copies the leader pushes to
              "isr": ["b2", "b3"],           followers that have everything
              "previous_isr": ["b2", "b3", "b1"]}   the brokers this partition was created on
    }}
/events/dead_broker/<id>      exists only while a dead broker is being handled
/auth/users/<username>        one entry per user: role and a salted password hash
/auth/secret                  the key that signs sign-in tokens
/config/retention             how long messages are kept, written by the console:
    {"default_hours": 720, "topics": {"orders": 24}, "purge_before": {"orders": 1700000000}}
```

The leader is never listed in `followers` or `isr`. A `leader` of `null` means
the partition is offline.

**Broker disk**: each partition is a folder of segment files. A file is named
after the offset of its first message and holds one record per line: the time
the message was stored and the message itself.

```
data/<topic>/partition_<n>/00000000000000000000.log    offsets 0 to 999
data/<topic>/partition_<n>/00000000000000001000.log    offsets 1000 to 1999
```

**Redis**

```
offset:<email>:<topic>               marks the subscription
offset:<email>:<topic>:<partition>   next offset to read
```

## 5. The life of a message

A write, step by step (`POST http://localhost:4000/write/news`):

1. **Producer** checks who is calling, looks up the topic (kept in memory and refreshed by ZooKeeper whenever it changes), hashes the message's `key` to pick a partition, and forwards the message to that partition's leader.
2. **Leader broker** (`write_message` in `broker.py`) checks that the topic and partition exist and that it really is the leader. If not, it refuses before touching the disk.
3. The leader takes that partition's lock, appends the message to its file, and sends it to each follower together with the offset it was written at.
4. **Each follower** (`replicate_messages`) compares that offset with its own file:
   - same length: append.
   - shorter: it missed something, so it answers "I am behind, I have N messages" and the leader resends everything from N.
   - longer: it holds messages the leader does not have, so it drops them. The leader's log wins.
5. Followers that answered become the in-sync set. If that set changed, the leader updates ZooKeeper. If it did not change, ZooKeeper is not touched.
6. The leader replies; the producer passes the reply back to the application.

A follower that is down does not fail the write. It simply leaves the in-sync set.

How many followers the leader waits for in step 5 is the topic's confirmation
setting: all of them (the default), enough for a majority, or none. Followers
that were not waited for still get the message a moment later. The leader keeps
a note of how far each follower has got (`FollowerLink`) and sends each one
everything it is missing in a single request, so when many writes arrive
together they travel to the followers in batches.

These three steps (producer, leader, followers) are written as `async`
functions: while one write waits for a reply from another service, the same
process handles other writes instead of parking a thread. Calls go through
`AsyncHTTP`, which keeps connections open between services. Everything that
talks to ZooKeeper or reads files in bulk stays ordinary blocking code and is
run on a worker thread when the write path needs it.

A read (`GET /read/<email>/news/<key>` on the consumer):

1. **Consumer** checks the subscription, hashes the key to a partition, and looks up the subscriber's offset in Redis.
2. It asks an in-sync follower for everything from that offset. If that broker does not answer it tries the next one, and the leader last.
3. It stores the new offset in Redis and returns the messages.

## 6. When a broker dies

1. The broker's entry under `/brokers` disappears once ZooKeeper notices the silence (about 4 seconds, set by `ZOOKEEPER_SESSION_TIMEOUT`).
2. Every surviving broker is watching `/brokers` (`watch_broker_changes`) and sees the entry go.
3. They race to create `/events/dead_broker/<id>/master_executor`. ZooKeeper lets exactly one win; that broker does the repair (`handle_dead_broker`). The others wait in case it dies too.
4. For every partition the dead broker led, the live in-sync follower whose copy goes furthest becomes leader (`most_complete_replica`). If there is none, the partition is marked offline rather than promoting a broker with missing messages.
5. The dead broker is removed from every follower and in-sync list.
6. The producer reads the new leader from ZooKeeper on its next write.

Metadata changes go through `update_topic_metadata`, which only writes if nobody
else changed the topic in the meantime and otherwise starts over. That is what
stops two brokers from overwriting each other's updates.

## 7. When a broker comes back

On startup (`monitor_new_broker`), for each partition it used to hold:

- If the partition is offline, it becomes the leader: its copy is the best one available.
- Otherwise it copies the partition from the current leader, then adds itself to the followers and the in-sync set. Anything written in between is filled in by the catch-up in step 4 of section 5.

If it restarts before ZooKeeper has forgotten its previous run, it waits for
the old entry to expire first (`register_broker`).

Every broker repeats this check every few seconds (`rejoin_loop`). That covers
the cases where rejoining was not possible at startup: the leader was
unreachable at that moment, or the whole cluster was stopped and the broker
with the complete copy came back last.

## 8. How old messages are removed

Messages are not kept forever. Each broker regularly runs `apply_retention`,
which deletes the oldest segment files of a partition once every message in
them is older than the retention period. Deleting whole files is what keeps
offsets stable: message 1,500 is still message 1,500 after the first thousand
are gone, and the partition simply "starts" at offset 1,000.

There are three ways to control it:

- **`settings.env`**: `RETENTION_HOURS` is the default (720 hours, 30 days).
- **The console**: "Retention settings" changes the default for the whole cluster and "Change retention" on a topic gives that topic its own period. No restart is needed.
- **Purge now**: "Purge messages" on a topic deletes everything, or everything older than an age you pick, immediately.

The console writes the setting to `/config/retention` in ZooKeeper. Every broker
watches that entry (`watch_retention`) and applies it to its own files, so
nobody has to tell each broker separately and a broker that was down catches up
when it returns.

Two details worth knowing:

- A subscriber whose position was purged continues with the oldest message left.
- If a follower is missing messages that the leader has already purged, the leader tells it to start over from the leader's oldest message.

## 9. Hands-on: break it and watch

Start the system, open the console at http://localhost:8080 and sign in with
`admin` / `admin`. The console asks for a new password; pick one and use it in
place of `admin` in the commands below.

```bash
docker compose up --build -d
curl -u admin:admin -X POST http://localhost:4000/create_topic/orders/3
```

The console now shows three partitions, each with a blue leader and two green
followers.

**Publish some messages** and watch the bars grow:

```bash
for i in $(seq 1 20); do
  curl -s -u admin:admin -X POST http://localhost:4000/write/orders \
       -H "Content-Type: application/json" \
       -d "{\"key\": \"user-$i\", \"value\": \"order $i\"}" > /dev/null
done
```

Click any cell to see the messages stored on that replica.

**Kill a leader.** Copy the id of a broker from the console, then:

```bash
docker kill <broker id>
```

After about 5 seconds the console headline turns amber, the broker's column
shows "Replica lost", another broker turns blue for its partitions, and the
"What changed" feed lists the election. Writes to the other partitions never
stopped, and writes to the affected ones waited in the producer and then went
through. Try `docker stop` instead of `docker kill`: a broker that is stopped
normally says goodbye to ZooKeeper, and the election is immediate.

**Bring it back:**

```bash
docker start <broker id>
```

It reappears as a green follower with the same number of messages as the leader.

**Purge a topic.** In the console, press "Purge messages" on `orders`, choose
"Everything stored so far" and confirm. The bars empty on every broker and the
feed shows how many messages each partition dropped. Publish again and the new
messages continue from the old offsets.

**Read as a subscriber** and see that nothing was lost:

```bash
CONSUMER_URL="http://$(docker compose port consumer 5000)"
curl -u admin:admin -X POST "$CONSUMER_URL/subscribe/me@example.com/orders"
curl -u admin:admin "$CONSUMER_URL/read/me@example.com/orders/user-1"
```

## 10. Working on the code

Run the tests after every change. They need no Docker:

```bash
pip install -r requirements-dev.txt
pytest
```

A test reads like the scenario it checks:

```python
def test_leader_crash_promotes_an_in_sync_follower(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.kill("b1")
    assert three_brokers.leader("orders") == "b2"
```

The `three_brokers` fixture gives you a cluster you can `kill` (crash), `stop`
(planned shutdown), `disconnect`, `reconnect` and `start_broker` on. To test a
new scenario, copy a test in `tests/test_shutdown_scenarios.py` and change the
steps; tests there run twice, once as a crash and once as a planned stop.

Where to look for common changes:

| You want to change | Look at |
|---|---|
| How a partition is chosen for a key | `write_message` in `producer.py` and `read_by_key` in `consumer.py` (keep them identical) |
| What happens on a write | `write_message` and `replicate_messages` in `broker.py` |
| Who becomes leader | `handle_dead_broker` in `broker.py` |
| How messages are stored or purged | `broker/partition_log.py`, and `apply_retention` in `broker.py` |
| How long the producer retries | `write_message` in `producer.py` |
| Default retention, failover timing, sign-in | `settings.env` |
| Who may do what | `current_user` and `admin` in each service, and `auth/auth.py` |
| How a broker rejoins | `monitor_new_broker` and `sync_data_from_leader` in `broker.py` |
| Which replica a subscriber reads from | `read_by_key` in `consumer.py` |
| What the console shows | `build_snapshot` in `console/console.py`, then `console/index.html` |
| Number of brokers | `deploy.replicas` in `docker-compose.yml` |

To measure the effect of a change on speed or failover, run
`python stress/stress_test.py` against the running system.

## 11. Things that will surprise you

- **Broker ids are container hostnames**, so they look like `3f9a1c2b7d10`, and a container that is recreated (not just restarted) is a new broker.
- **A new broker does not get existing partitions.** Only topics created after it joined use it.
- **Reading by key returns the whole partition**, not only messages with that key.
- **Purging by age is approximate.** Only whole segment files are deleted, so a message stays until every message in its file is old enough.
- **A write can answer 504.** That means the leader may or may not have stored the message; the producer does not send it again because it could end up stored twice.
- **Nothing is on a volume.** `docker compose down` deletes the messages, the topics and the subscriber offsets.
- **Sign-in is not encryption.** Passwords and tokens cross the network in clear text until HTTPS is put in front, so do not expose the ports outside your machine yet.
- **The default password is `admin`.** The console nags until it is changed; the API does not.
- **The tests run with sign-in off** (`AUTH_ENABLED=0`); `tests/test_auth.py` switches it on.

The full list is under "Known limitations" in the [README](README.md).
