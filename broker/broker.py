import asyncio
import hmac
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import List, Optional

import aiohttp
import anyio
import requests
from fastapi import BackgroundTasks, Body, Depends, FastAPI, Header, HTTPException
from fastapi.concurrency import run_in_threadpool
from kazoo.client import KazooClient
from kazoo.exceptions import (
    BadVersionError,
    KazooException,
    NoNodeError,
    NodeExistsError,
)

from partition_log import PartitionLog

'''
/brokers/                   # Registered brokers (ephemeral)
│── broker1                 # {"host": "http://broker1:5000"}
│── broker2                 # {"host": "http://broker2:5000"}
│── broker3                 # {"host": "http://broker3:5000"}

/topics/
│── orders                  # One znode per topic holding all partition metadata:
│                           # {"partitions": {
│                           #    "0": {"leader": "broker1",
│                           #          "followers": ["broker2", "broker3"],
│                           #          "isr": ["broker2", "broker3"],
│                           #          "previous_isr": [...]},
│                           #    "1": {...}}}

Conventions
- "leader" is the broker accepting writes, or null when the partition is offline.
- "followers" are the replicas the leader pushes messages to.
- "isr" is the subset of followers that acknowledged the last write.
  The leader is never listed in "followers" or "isr".
'''

# Configure Logging
logging.basicConfig(
    level=logging.INFO,  # Change to INFO for less verbosity
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler("broker.log")  # Log to file
    ]
)

# Get broker hostname as BROKER_ID inside the Docker container
BROKER_ID = os.getenv("HOSTNAME", "localhost")
BROKER_HOST = f"http://{BROKER_ID}:5000"
ZOOKEEPER_HOST = os.getenv("ZOOKEEPER_HOST", "zookeeper:2181")
# Seconds to wait for another broker before treating it as unavailable
REQUEST_TIMEOUT = float(os.getenv("BROKER_REQUEST_TIMEOUT", "5"))
# Where partition logs are stored (mount a volume here to keep data across restarts)
DATA_DIR = os.getenv("BROKER_DATA_DIR", "./data")
# Seconds of silence after which ZooKeeper declares this broker dead. Lower means
# faster failover but more false alarms; ZooKeeper's own minimum is 2 x tickTime.
SESSION_TIMEOUT = float(os.getenv("ZOOKEEPER_SESSION_TIMEOUT", "4"))
# A partition log starts a new segment file every this many messages
SEGMENT_MAX_MESSAGES = int(os.getenv("SEGMENT_MAX_MESSAGES", "1000"))
# Messages are kept this long unless the cluster setting says otherwise (0 = forever)
DEFAULT_RETENTION_HOURS = float(os.getenv("RETENTION_HOURS", "720"))
# How often old segments are looked for (0 = only when the setting changes)
RETENTION_CHECK_SECONDS = float(os.getenv("RETENTION_CHECK_SECONDS", "60"))
# Cluster-wide retention setting, edited from the console
RETENTION_PATH = "/config/retention"
# When a write is confirmed to the sender, unless the topic says otherwise:
#   all       after every follower has it (safest)
#   majority  after the leader and enough followers for a majority of the replicas have it
#   leader    after the leader alone has it (fastest; a confirmed message can be lost)
ACKS = ("all", "majority", "leader")
DEFAULT_ACKS = os.getenv("DEFAULT_ACKS", "all")
if DEFAULT_ACKS not in ACKS:
    raise ValueError(f"DEFAULT_ACKS must be one of {ACKS}")
# Most messages sent to a follower in one request
REPLICATION_BATCH_MESSAGES = int(os.getenv("REPLICATION_BATCH_MESSAGES", "500"))
# How often this broker checks whether it should (re)join a partition, for
# example because the leader it could not reach earlier is back (0 = never)
REJOIN_CHECK_SECONDS = float(os.getenv("REJOIN_CHECK_SECONDS", "10"))

# Shared secret of the cluster's own services. When set, the broker only answers
# requests that carry it, so its internal API cannot be used to get around the login.
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "")

# One HTTP connection pool for all calls to other brokers (connections are reused)
http = requests.Session()
http.mount("http://", requests.adapters.HTTPAdapter(pool_connections=16, pool_maxsize=64))
if INTERNAL_TOKEN:
    http.headers["X-Internal-Token"] = INTERNAL_TOKEN

class PeerUnreachable(Exception):
    """The other service could not be reached at all, so it certainly did nothing."""


class PeerError(Exception):
    """The call failed after it may have reached the other service."""


class AsyncHTTP:
    """
    Calls to other services that do not block the event loop.

    One pool of open connections is shared by all calls. `post` returns
    (status code, decoded JSON body).
    """

    def __init__(self, headers):
        self.headers = headers
        self._session = None

    async def post(self, url, json=None, timeout=5.0, connect_timeout=None):
        if self._session is None:   # created on first use, inside the running event loop
            self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=256))
        limits = aiohttp.ClientTimeout(total=timeout, sock_connect=connect_timeout)
        try:
            async with self._session.post(url, json=json, headers=dict(self.headers), timeout=limits) as response:
                return response.status, await response.json(content_type=None)
        except (aiohttp.ClientConnectorError, aiohttp.ConnectionTimeoutError) as e:
            raise PeerUnreachable(str(e))
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            raise PeerError(str(e) or type(e).__name__)

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None


http_async = AsyncHTTP(http.headers)

# Kept so older tooling that shuts it down keeps working; replication is async now
replication_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="unused")

zk = KazooClient(hosts=ZOOKEEPER_HOST, timeout=SESSION_TIMEOUT)
zk.start()


@asynccontextmanager
async def lifespan(app):
    """
    On a normal stop (SIGTERM, `docker stop`), close the ZooKeeper session at once.
    The other brokers then start the election immediately instead of waiting
    for the session to time out.
    """
    # Writes that wait their turn for a follower do so on worker threads; allow plenty
    anyio.to_thread.current_default_thread_limiter().total_tokens = 256
    yield
    try:
        zk.stop()
    except Exception as e:
        logging.warning(f"Could not close the ZooKeeper session cleanly: {e}")
    await http_async.close()


async def require_internal_token(x_internal_token: Optional[str] = Header(None)):
    if INTERNAL_TOKEN and not hmac.compare_digest(x_internal_token or "", INTERNAL_TOKEN):
        raise HTTPException(status_code=401, detail="Brokers only answer the cluster's own services")


app = FastAPI(lifespan=lifespan, dependencies=[Depends(require_internal_token)])
# Register broker in Zookeeper
zk.ensure_path("/brokers")
broker_path = f"/brokers/{BROKER_ID}"
brokers_set = set()


def register_broker(wait_seconds: int = 30):
    """
    Announce this broker in ZooKeeper with an ephemeral node.

    After a crash and a quick restart the node of the previous run can still be
    there until its ZooKeeper session times out. Wait for it to disappear
    instead of failing, so the other brokers see a clean "died, then came back".
    """
    payload = json.dumps({"host": BROKER_HOST}).encode("utf-8")
    deadline = time.time() + wait_seconds
    while True:
        try:
            zk.create(broker_path, payload, ephemeral=True)
            return
        except NodeExistsError:
            if time.time() >= deadline:
                raise
            logging.info(f"{broker_path} still exists from a previous run, waiting for it to expire")
            time.sleep(1)


register_broker()

# Topic names end up in file paths and znode paths, so keep them to a safe alphabet
TOPIC_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

# One lock per partition so appends (and the replication that follows them)
# happen in a single, well-defined order on this broker.
_partition_locks = {}
_partition_locks_guard = threading.Lock()


def validate_topic_name(topic_ID: str):
    if not isinstance(topic_ID, str) or not TOPIC_NAME_RE.match(topic_ID):
        raise HTTPException(
            status_code=400,
            detail="Invalid topic ID: use 1-128 letters, digits, '_' or '-'",
        )


def partition_dir(topic_ID: str, partition_ID) -> str:
    """Directory holding the segment files of a partition (validates the topic name)."""
    validate_topic_name(topic_ID)
    return os.path.join(DATA_DIR, topic_ID, f"partition_{int(partition_ID)}")


def partition_lock(topic_ID: str, partition_ID) -> threading.Lock:
    key = (topic_ID, int(partition_ID))
    with _partition_locks_guard:
        if key not in _partition_locks:
            _partition_locks[key] = threading.Lock()
        return _partition_locks[key]


class FollowerLink:
    """
    What the leader knows about one follower's copy of one partition.

    `acked` is the follower's end offset as far as the leader knows (None =
    unknown). `lock` lets one request at a time travel to that follower, so it
    always receives messages in log order. A write that finds the lock taken
    waits; when its turn comes, an earlier request has often carried its message
    already and there is nothing left to send. That is how messages get batched.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.acked = None


_links = {}


def follower_link(topic_ID: str, partition_ID, follower: str) -> FollowerLink:
    key = (topic_ID, int(partition_ID), follower)
    with _partition_locks_guard:
        if key not in _links:
            _links[key] = FollowerLink()
        return _links[key]


def forget_followers(topic_ID: str, partition_ID) -> None:
    """Drop what we knew about followers of a partition (our own copy was just replaced)."""
    with _partition_locks_guard:
        for key in [k for k in _links if k[0] == topic_ID and k[1] == int(partition_ID)]:
            del _links[key]


async def acquire(lock: threading.Lock) -> None:
    """Take a lock from async code without blocking the event loop while waiting."""
    if not lock.acquire(blocking=False):
        await run_in_threadpool(lock.acquire)


async def with_log(topic_ID: str, partition_ID, action):
    """Run `action(log)` on this broker's copy of a partition, under the partition lock."""
    lock = partition_lock(topic_ID, partition_ID)
    await acquire(lock)
    try:
        return action(partition_log(topic_ID, partition_ID))
    finally:
        lock.release()


# Open partition logs, see partition_log.py
_logs = {}


def partition_log(topic_ID: str, partition_ID) -> PartitionLog:
    """This broker's copy of a partition. Caller must hold the partition lock."""
    key = (topic_ID, int(partition_ID))
    if key not in _logs:
        directory = partition_dir(topic_ID, partition_ID)
        _logs[key] = PartitionLog(directory, SEGMENT_MAX_MESSAGES, legacy_file=directory + ".log")
    return _logs[key]


def update_topic_metadata(topic_ID: str, mutate, retries: int = 20):
    """
    Read-modify-write of a topic znode that cannot lose concurrent updates.

    `mutate(metadata)` edits the dict in place and returns False when nothing
    changed. The write only succeeds if nobody else modified the znode in the
    meantime; otherwise the whole read-modify-write is retried.
    """
    topic_path = f"/topics/{topic_ID}"
    for _ in range(retries):
        data, stat = zk.get(topic_path)
        topic_metadata = json.loads(data.decode("utf-8"))
        if mutate(topic_metadata) is False:
            return topic_metadata
        try:
            zk.set(topic_path, json.dumps(topic_metadata).encode("utf-8"), version=stat.version)
            return topic_metadata
        except BadVersionError:
            continue  # someone else updated the topic, start over
    raise KazooException(f"Could not update metadata for {topic_ID} after {retries} attempts")


def topic_metadata(topic_ID: str) -> dict:
    data, _ = zk.get(f"/topics/{topic_ID}")
    return json.loads(data.decode("utf-8"))


def acquire_lock_with_retries(zk: KazooClient, lock_path: str, retries: int = 5, timeout: int = 5):
    """
    Attempt to acquire a ZooKeeper distributed lock with retries and exponential backoff.

    Returns:
        lock (kazoo.recipe.lock.Lock): the lock object (already acquired), or None if failed.
    """
    lock = zk.Lock(lock_path)
    for attempt in range(retries):
        try:
            if lock.acquire(timeout=timeout):
                logging.info(f"Acquired lock for {lock_path} on attempt {attempt + 1}")
                return lock
        except KazooException as e:
            logging.warning(f"Attempt {attempt + 1} failed to acquire lock {lock_path}: {e}")
        time.sleep(2 ** attempt)  # exponential backoff
    logging.error(f"Failed to acquire lock {lock_path} after {retries} attempts.")
    return None


def release_lock_safely(lock):
    """
    Safely release a ZooKeeper lock if it's acquired.
    """
    try:
        if lock is not None:
            lock.release()
            logging.info(f"Lock released for {lock.path}")
    except KazooException as e:
        logging.error(f"Failed to release lock {lock.path}: {e}")


@app.post("/replicate/{topic_ID}/{partition_ID}")
async def replicate_messages(
    topic_ID: str,
    partition_ID: int,
    messages: List[dict] = Body(..., embed=True),  # expects {"messages": [...], "base_offset": n}
    base_offset: Optional[int] = Body(None, embed=True),
    timestamps: Optional[List[float]] = Body(None, embed=True),
    reset: bool = Body(False, embed=True),
):
    """
    Append messages sent by the leader.

    `base_offset` is the offset the first message has in the leader's log. It
    keeps this copy identical to the leader's:
    - behind the leader  -> 409 with our end offset, so the leader resends what we miss
    - ahead of the leader -> drop the extra tail, the leader's log wins
    `reset` replaces this copy entirely: the leader uses it when the messages we
    are missing were already purged on its side.
    `timestamps` are the times the leader stored each message, so every replica
    purges the same messages at the same age.
    """
    validate_topic_name(topic_ID)

    def store(log):
        if reset and base_offset is not None:
            log.reset(base_offset, messages, timestamps)
            return
        if base_offset is not None:
            current = log.end_offset
            if current < base_offset:
                raise HTTPException(
                    status_code=409,
                    detail={"error": "Follower is behind the leader", "end_offset": current},
                )
            if current > base_offset:
                log.truncate_to(base_offset)
        log.append(messages, timestamps)

    await with_log(topic_ID, partition_ID, store)
    return {"status": "Replication successful", "broker": BROKER_ID}


def sync_data_from_leader(topic_ID, partition_ID, leader_broker):
    """
    Replaces the local copy of a partition with the leader's copy before rejoining ISR.
    """
    leader_url = f"http://{leader_broker}:5000/read/{topic_ID}/{partition_ID}"
    try:
        response = http.get(leader_url, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as e:
        logging.error(f"Failed to reach leader {leader_broker} for {topic_ID} partition {partition_ID}: {e}")
        return {"error": "Failed to sync from leader", "broker": BROKER_ID}

    if response.status_code == 404:
        # The leader has no log yet, so there is nothing to copy
        start, messages, timestamps = 0, [], None
    elif response.status_code == 200:
        body = response.json()
        start, messages, timestamps = body.get("start_offset", 0), body.get("messages", []), body.get("timestamps")
    else:
        logging.error(f"Failed to sync from leader {leader_broker} for {topic_ID} partition {partition_ID}")
        return {"error": "Failed to sync from leader", "broker": BROKER_ID}

    # Swap the local log for the leader's copy in one step (avoids duplicates)
    with partition_lock(topic_ID, partition_ID):
        partition_log(topic_ID, partition_ID).reset(start, messages, timestamps)
    forget_followers(topic_ID, partition_ID)

    logging.info(f"Broker {BROKER_ID} synchronized {topic_ID} partition {partition_ID} from {leader_broker}")
    return {"status": "Synchronization complete", "broker": BROKER_ID}


@app.post("/create_topic/{topic_ID}/{num_partitions}")
def create_topic(topic_ID: str, num_partitions: int, acks: Optional[str] = None):
    validate_topic_name(topic_ID)
    if acks is not None and acks not in ACKS:
        raise HTTPException(status_code=400, detail=f"acks must be one of: {', '.join(ACKS)}")

    if num_partitions <= 0:
        raise HTTPException(status_code=400, detail="Number of partitions must be a positive integer")

    topic_parent_path = "/topics"
    topic_path = f"{topic_parent_path}/{topic_ID}"

    try:
        zk.ensure_path(topic_parent_path)

        # Check if the topic already exists
        if zk.exists(topic_path):
            raise HTTPException(status_code=400, detail="Topic already exists")

        # Fetch available brokers
        brokers = sorted(zk.get_children("/brokers"))
    except NoNodeError:
        raise HTTPException(status_code=500, detail="ZooKeeper node '/brokers' does not exist")
    except KazooException as e:
        raise HTTPException(status_code=500, detail=f"Error accessing ZooKeeper: {str(e)}")

    if not brokers:
        raise HTTPException(status_code=500, detail="No available brokers to assign partitions")

    topic_metadata = {"partitions": {}}
    if acks is not None:
        topic_metadata["acks"] = acks   # otherwise the brokers' DEFAULT_ACKS applies

    try:
        for partition in range(num_partitions):
            leader = brokers[partition % len(brokers)]
            followers = [b for b in brokers if b != leader]

            # Add partition metadata
            topic_metadata["partitions"][partition] = {
                "leader": leader,
                "isr": list(followers),  # all followers start in sync (leader excluded)
                "followers": list(followers),
                "previous_isr": followers + [leader]  # Keep track of previous ISR for rejoinAsFollower
            }

        # Create the topic node in ZooKeeper with the partition metadata
        zk.create(topic_path, json.dumps(topic_metadata).encode("utf-8"))
        return {"status": f"Topic {topic_ID} created with {num_partitions} partitions and leader paths"}

    except NodeExistsError:
        raise HTTPException(status_code=400, detail="Topic creation failed: Topic already exists")
    except KazooException as e:
        raise HTTPException(status_code=500, detail=f"Error creating topic in ZooKeeper: {str(e)}")


def read_partition(topic_ID: str, partition_ID: int, offset: int):
    with partition_lock(topic_ID, partition_ID):
        log = partition_log(topic_ID, partition_ID)
        if not log.exists:
            raise HTTPException(status_code=404, detail="Partition not found")
        start, messages, timestamps = log.read(offset)
        return {
            "messages": messages,
            "timestamps": timestamps,          # when each message was stored
            "start_offset": start,             # offset of the first message returned
            "end_offset": log.end_offset,      # absolute offset to read from next
            "log_start_offset": log.start_offset,  # oldest offset still stored (older ones were purged)
        }


# ✅ Read from a specific offset (to end)
@app.get("/read/{topic_ID}/{partition_ID}/offset/{offset}")
def read_from_offset(
    topic_ID: str,
    partition_ID: int,
    offset: int
):
    if offset < 0:
        raise HTTPException(status_code=400, detail="Offset must not be negative")
    # A caught-up consumer gets an empty list, not an error. An offset that was
    # already purged is moved forward to the oldest message still stored.
    return read_partition(topic_ID, partition_ID, offset)


# ✅ Read all messages (no offset)
@app.get("/read/{topic_ID}/{partition_ID}")
def read_all(topic_ID: str, partition_ID: int):
    return read_partition(topic_ID, partition_ID, 0)


# Size of this broker's copy of a partition (used by the console to show replica lag)
@app.get("/offsets/{topic_ID}/{partition_ID}")
def partition_offsets(topic_ID: str, partition_ID: int):
    with partition_lock(topic_ID, partition_ID):
        stats = partition_log(topic_ID, partition_ID).stats()
    return {"broker": BROKER_ID, "topic": topic_ID, "partition": partition_ID, **stats}


def replica_end_offset(broker, topic_ID, partition_ID) -> int:
    """How far a broker's copy of a partition goes, or -1 if it does not answer."""
    if broker == BROKER_ID:
        with partition_lock(topic_ID, partition_ID):
            return partition_log(topic_ID, partition_ID).end_offset
    try:
        response = http.get(f"http://{broker}:5000/offsets/{topic_ID}/{partition_ID}", timeout=min(REQUEST_TIMEOUT, 2))
        if response.status_code == 200:
            return int(response.json()["end_offset"])
    except (requests.RequestException, KeyError, TypeError, ValueError):
        pass
    return -1


def most_complete_replica(topic_ID, partition_ID, candidates):
    """
    Of the live in-sync followers, the one whose copy goes furthest.

    With acks=majority or acks=leader a follower can still be marked in sync
    while it is a few messages behind. Promoting the longest copy keeps every
    message that was confirmed. Ties go to the first candidate.
    """
    best, best_end = candidates[0], -1
    for broker in candidates:
        end = replica_end_offset(broker, topic_ID, partition_ID)
        if end > best_end:
            best, best_end = broker, end
    return best


def handle_dead_broker(dead_broker, executor_broker, executor_broker_path):
    logging.info(f"Handling dead broker event for {dead_broker} by {executor_broker}")
    topics = []
    try:
        topics = zk.get_children("/topics")
    except KazooException:
        logging.error("No topics to retrieve")

    for topic in topics:

        def remove_dead_broker(topic_metadata, topic=topic):
            changed = False
            for partition, partition_data in topic_metadata["partitions"].items():
                before = json.dumps(partition_data, sort_keys=True)
                live_isr = [b for b in partition_data.get("isr", []) if b != dead_broker]

                # If the current leader fails, a new leader must be elected
                # from the replicas that are still alive and in sync
                if partition_data.get("leader") == dead_broker:
                    if live_isr:
                        new_leader = most_complete_replica(topic, partition, live_isr)
                        partition_data["leader"] = new_leader
                        logging.info(f"Broker {new_leader} is now leader for {topic} partition {partition}")
                    else:
                        partition_data["leader"] = None
                        logging.error(f"No in-sync replica left for {topic} partition {partition}: partition is offline")

                # The leader is never listed as its own follower / ISR member
                leader = partition_data.get("leader")
                partition_data["isr"] = [b for b in live_isr if b != leader]
                partition_data["followers"] = [
                    b for b in partition_data.get("followers", []) if b != leader and b != dead_broker
                ]
                changed = changed or json.dumps(partition_data, sort_keys=True) != before
            return changed

        try:
            update_topic_metadata(topic, remove_dead_broker)
            logging.info(f"Metadata for {topic} updated successfully.")
        except KazooException as e:
            logging.error(f"Failed to update metadata for {topic}: {str(e)}")

    # Remove the dead event from the zookeeper /events/dead_broker/{dead_broker}"
    try:
        # Delete the last znode (executor_broker_path)
        zk.delete(executor_broker_path)
        # Delete the parent znode if it exists and has no children
        parent_path = "/".join(executor_broker_path.split("/")[:-1])
        zk.delete(parent_path)
        logging.info(f"Dead broker event {executor_broker_path} deleted successfully.")
    except KazooException as e:
        logging.error(f"Failed to delete dead broker event {executor_broker_path}: {str(e)}")


def monitor_dead_broker(dead_broker: str):

    dead_broker_path = f"/events/dead_broker/{dead_broker}"

    # Ensure that dead broker event is registered
    try:
        zk.ensure_path(dead_broker_path)
    except KazooException as e:
        logging.info(f"Path {dead_broker_path} already exists")

    def attempt_to_become_leader_executor_of_dead_event(dead_broker: str):
        logging.info(f"Broker {dead_broker} is dead. Handling event...")
        # Only one broker will achieve to be leader of the dead broker event
        executor_broker_path = dead_broker_path + '/master_executor'
        try:
            zk.create(executor_broker_path, value=bytes(BROKER_ID, 'utf-8'),  ephemeral=True, sequence=False, makepath=True)
            logging.info(f"Broker {BROKER_ID} is the executor of the dead broker event.")
            handle_dead_broker(dead_broker, BROKER_ID, executor_broker_path)
        except NodeExistsError as e:
            #Someone else is the executor
            logging.info(f"Broker {BROKER_ID} failed to be the executor of the dead broker event.")
            @zk.DataWatch(executor_broker_path)
            def watch_dead_broker_event(data, stat, event):
                logging.info(f"Registered in watch_dead_broker_event")

                if event and event.type == "DELETED":
                    if not zk.exists(dead_broker_path):
                        logging.info(f"[{dead_broker}] Event fully cleaned up, not retrying.")
                        return
                    logging.info(f"Call attempt_to_become_leader_executor_of_dead_event")
                    attempt_to_become_leader_executor_of_dead_event(dead_broker)

    attempt_to_become_leader_executor_of_dead_event(dead_broker)


def needs_rejoin(partition_data, broker, may_claim):
    """True if `broker` should hold this partition but is neither its leader nor an in-sync follower."""
    if broker not in partition_data.get("previous_isr", []):
        return False
    leader = partition_data.get("leader")
    if leader == broker:
        return False
    if leader is None:
        return may_claim
    if leader not in brokers_set:
        return False   # the leader is down: nothing to copy from yet
    return not (broker in partition_data.get("followers", []) and broker in partition_data.get("isr", []))


def monitor_new_broker(arrived=None):
    """
    (Re)join the partitions this broker should hold a copy of.

    Runs when this broker starts, whenever another broker arrives, and every
    REJOIN_CHECK_SECONDS: if a leader we could not copy from earlier is back
    (for example after the whole cluster was stopped), we can copy it now.
    Partitions where this broker is already leader or an in-sync follower are skipped.
    """
    broker = BROKER_ID
    # Only a broker that has just started may take over an offline partition.
    # Otherwise a broker with an older copy could grab it the moment the broker
    # holding the newest copy comes back.
    may_claim = arrived == BROKER_ID

    if arrived:
        logging.info(f"Broker {arrived} arrived. Checking which partitions {broker} should rejoin.")
    zk.ensure_path("/topics")
    for topic in zk.get_children("/topics"):
        topic_path = f"/topics/{topic}"
        try:
            partitions = json.loads(zk.get(topic_path)[0].decode("utf-8"))["partitions"]
        except (KazooException, ValueError, KeyError):
            continue   # topic deleted meanwhile, or not valid metadata
        if not any(needs_rejoin(p, broker, may_claim) for p in partitions.values()):
            continue   # nothing to do for this topic: do not even take its lock
        lock = acquire_lock_with_retries(zk, f"/locks/{topic}")
        if lock is None:
            logging.error(f"Skipping rejoin for {topic}: could not acquire its lock")
            continue
        try:
            topic_metadata = json.loads(zk.get(topic_path)[0].decode("utf-8"))
            for partition, partition_data in topic_metadata["partitions"].items():
                if broker not in partition_data.get("previous_isr", []):
                    continue

                leader = partition_data.get("leader")
                if leader == broker:
                    continue  # still registered as leader: nothing to sync from

                if leader is None:
                    if not may_claim:
                        continue
                    # The partition went offline because every replica died.
                    # This broker holds a copy, so it brings the partition back.
                    def claim_leadership(metadata, partition=partition):
                        p = metadata["partitions"][partition]
                        if p.get("leader") is not None:
                            return False
                        p["leader"] = broker
                        p["isr"] = [b for b in p.get("isr", []) if b != broker]
                        p["followers"] = [b for b in p.get("followers", []) if b != broker]
                        return True

                    update_topic_metadata(topic, claim_leadership)
                    logging.warning(f"Broker {broker} took over offline partition {partition} of {topic}")
                    continue

                if broker in partition_data.get("followers", []) and broker in partition_data.get("isr", []):
                    continue  # already a follower in sync

                #TODO : No need to get a full snashot of the partition data, only the missing data (offset difference)
                result = sync_data_from_leader(topic, partition, leader)
                if "error" in result:
                    continue  # stay out of the ISR until a sync succeeds

                def join_as_follower(metadata, partition=partition):
                    p = metadata["partitions"][partition]
                    if p.get("leader") == broker:
                        return False
                    changed = False
                    for field in ("followers", "isr"):
                        members = p.setdefault(field, [])
                        if broker not in members:
                            members.append(broker)
                            changed = True
                    return changed

                update_topic_metadata(topic, join_as_follower)
                logging.info(f"Metadata for {topic} updated successfully.")
        except (KazooException, ValueError) as e:
            logging.error(f"Failed to rejoin {topic}: {e}")
        finally:
            release_lock_safely(lock)
    #What if node not in previous_isr list but partition followers are under user spesified threshold


@zk.ChildrenWatch("/brokers")
def watch_broker_changes(brokers):
    logging.info(f"Broker event detected: {brokers}")

    ''''
    /events/dead_broker/<HOSTNAME> is a new event that is created when a broker is dead.
    The first broker that creates the event is the executor of the event.
    The executor will handle the event and will be responsible to elect a new leader for the dead broker event.
    1. Broker is dead. Instead of handling death at each broker node and have mutliple locks
    is better to create an new event under /events/dead_broker (data : hostname of dead broker)

    '''
    global brokers_set
    current_brokers = set(brokers)
    dead_brokers = brokers_set - current_brokers
    new_brokers = current_brokers - brokers_set
    #Update new broker global list
    brokers_set = current_brokers
    for broker in dead_brokers:
        monitor_dead_broker(broker)

    for broker in new_brokers:
        logging.info(f"New broker detected: {broker}")
    if new_brokers:
        # one pass is enough however many brokers arrived at once
        monitor_new_broker(BROKER_ID if BROKER_ID in new_brokers else sorted(new_brokers)[0])


async def replicate_up_to(follower, topic_ID, partition_ID, offset) -> bool:
    """
    Make sure a follower has every message up to and including `offset`.

    Sends whatever the follower is missing, in log order, up to
    REPLICATION_BATCH_MESSAGES per request. Returns False if it could not be done.
    """
    link = follower_link(topic_ID, partition_ID, follower)
    follower_url = f"http://{follower}:5000/replicate/{topic_ID}/{partition_ID}"
    await acquire(link.lock)
    try:
        for _ in range(100000):
            if link.acked is not None and link.acked > offset:
                return True        # an earlier request already carried this message
            # Unknown position: assume the follower is right behind this message.
            # If it is further behind it answers 409 with where it really is.
            send_from = offset if link.acked is None else link.acked
            start, messages, stamps = await with_log(
                topic_ID, partition_ID, lambda log: log.read(send_from, REPLICATION_BATCH_MESSAGES)
            )
            if not messages:
                return False       # nothing at that offset any more (purged meanwhile)
            status, body = await http_async.post(
                follower_url,
                json={
                    "messages": messages,
                    "timestamps": stamps,
                    "base_offset": start,
                    # what the follower lacks was purged here: it restarts from our oldest message
                    "reset": start > send_from,
                },
                timeout=REQUEST_TIMEOUT,
            )
            if status == 200:
                link.acked = start + len(messages)
            elif status == 409 and int(body["detail"]["end_offset"]) != send_from:
                link.acked = int(body["detail"]["end_offset"])   # resend from where the follower really is
            else:
                logging.warning(f"Replication to {follower} returned HTTP {status}")
                link.acked = None
                return False
        return False
    except Exception as e:
        # An unreachable follower must not fail the write: it just leaves the ISR
        logging.warning(f"Replication to {follower} failed for {topic_ID} partition {partition_ID}: {e}")
        link.acked = None
        return False
    finally:
        link.lock.release()


def followers_needed(acks: str, partition_data: dict, followers: List[str]) -> int:
    """How many followers must have a message before the write is confirmed."""
    if acks == "leader":
        return 0
    if acks == "majority":
        replicas = max(len(partition_data.get("previous_isr", [])), 1 + len(followers))
        return replicas // 2       # with the leader that makes a majority: 1 of 2 followers for 3 replicas
    return len(followers)


# API: Write a message to a partition and replicate to followers
@app.post("/write/{topic_ID}/{partition_ID}")
async def write_message(topic_ID: str, partition_ID: int, background: BackgroundTasks, message: dict = Body(...)):
    validate_topic_name(topic_ID)

    # Validate everything before touching the log, so a rejected write leaves no trace
    try:
        metadata = await run_in_threadpool(topic_metadata, topic_ID)
    except NoNodeError:
        raise HTTPException(status_code=404, detail="Topic not found")
    except KazooException as e:
        raise HTTPException(status_code=503, detail=f"Error accessing ZooKeeper: {str(e)}")

    partition_data = metadata["partitions"].get(str(partition_ID))
    if partition_data is None:
        raise HTTPException(status_code=404, detail="Partition not found")

    leader = partition_data.get("leader")
    if leader is None:
        raise HTTPException(status_code=503, detail="Partition has no leader")
    if leader != BROKER_ID:
        raise HTTPException(status_code=409, detail={"error": "Not the leader for this partition", "leader": leader})

    followers = [f for f in partition_data.get("followers", []) if f != BROKER_ID]
    acks = metadata.get("acks", DEFAULT_ACKS)
    needed = followers_needed(acks, partition_data, followers)
    if acks == "majority" and len(followers) < needed:
        # Consistency before availability: do not take a write that cannot reach a majority
        raise HTTPException(status_code=503, detail="Too few replicas are available to confirm this write")

    timestamp = time.time()

    def append(log):
        offset = log.end_offset
        log.append([message], [timestamp])
        return offset

    offset = await with_log(topic_ID, partition_ID, append)

    # Every follower is sent the message at once. Each follower gets its messages in
    # log order (see FollowerLink), so writes do not have to queue behind each other.
    tasks = [asyncio.ensure_future(replicate_up_to(f, topic_ID, partition_ID, offset)) for f in followers]
    pending, confirmed = set(tasks), 0
    while pending and confirmed < needed:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        confirmed += sum(1 for task in done if task.result())
    replicated_to = [f for f, task in zip(followers, tasks) if task.done() and task.result()]

    async def finish():
        """Wait for the followers that have not answered yet, then record who is in sync."""
        results = await asyncio.gather(*tasks)
        successful_followers = [follower for follower, ok in zip(followers, results) if ok]
        # Update ISR only when it changed: followers that replicated stay in sync
        if successful_followers != partition_data.get("isr", []):

            def set_isr(topic_metadata):
                p = topic_metadata["partitions"].get(str(partition_ID))
                if p is None or p.get("leader") != BROKER_ID or p.get("isr") == successful_followers:
                    return False
                p["isr"] = successful_followers
                return True

            try:
                await run_in_threadpool(update_topic_metadata, topic_ID, set_isr)
            except KazooException as e:
                logging.error(f"Failed to update ISR for {topic_ID} partition {partition_ID}: {str(e)}")

    if pending:
        background.add_task(finish)    # answer now; slower followers finish after the reply
    else:
        await finish()

    if acks == "majority" and confirmed < needed:
        # Stored here, but not on enough replicas to promise it survives a failure
        raise HTTPException(
            status_code=504,
            detail="Too few replicas confirmed the message; it may or may not survive a failure",
        )

    return {
        "status": "Message written and replicated",
        "broker": BROKER_ID,
        "offset": offset,
        "acks": acks,
        "replicated_to": replicated_to
    }


# --- Retention: deleting old messages ---------------------------------------
#
# The cluster setting lives in ZooKeeper at /config/retention and is edited
# from the console:
#   {"default_hours": 720,                 keep messages this long (0 = forever)
#    "topics": {"orders": 24},             per-topic override
#    "purge_before": {"orders": 1700000000}}   "purge now": drop what was stored before this time
# Every broker applies it to its own copies, so no broker has to be told directly
# and a broker that was down applies it when it returns.

_retention = {"default_hours": DEFAULT_RETENTION_HOURS, "topics": {}, "purge_before": {}}


def retention_cutoff(topic_ID: str, now: float) -> float:
    """Messages of this topic stored before the returned time may be deleted."""
    hours = _retention["topics"].get(topic_ID, _retention["default_hours"])
    by_age = now - float(hours) * 3600 if hours and float(hours) > 0 else 0
    return max(by_age, float(_retention["purge_before"].get(topic_ID, 0) or 0))


def apply_retention(now: Optional[float] = None) -> int:
    """Delete expired segments of every partition stored on this broker. Returns messages removed."""
    now = time.time() if now is None else now
    removed = 0
    if not os.path.isdir(DATA_DIR):
        return 0
    for topic_ID in sorted(os.listdir(DATA_DIR)):
        topic_dir = os.path.join(DATA_DIR, topic_ID)
        if not os.path.isdir(topic_dir) or not TOPIC_NAME_RE.match(topic_ID):
            continue
        cutoff = retention_cutoff(topic_ID, now)
        if cutoff <= 0:
            continue
        for name in sorted(os.listdir(topic_dir)):
            match = re.match(r"^partition_(\d+)(\.log)?$", name)
            if not match:
                continue
            with partition_lock(topic_ID, match.group(1)):
                count = partition_log(topic_ID, match.group(1)).purge(cutoff)
            if count:
                removed += count
                logging.info(f"Purged {count} messages from {topic_ID} partition {match.group(1)}")
    return removed


zk.ensure_path("/config")


@zk.DataWatch(RETENTION_PATH)
def watch_retention(data, stat, event=None):
    """Runs at startup and every time the setting is changed from the console."""
    setting = {}
    if data:
        try:
            setting = json.loads(data.decode("utf-8"))
        except ValueError:
            logging.error("Ignoring invalid retention setting in ZooKeeper")
            return
    default = setting.get("default_hours")
    _retention["default_hours"] = DEFAULT_RETENTION_HOURS if default is None else float(default)
    _retention["topics"] = dict(setting.get("topics") or {})
    _retention["purge_before"] = dict(setting.get("purge_before") or {})
    try:
        apply_retention()
    except OSError as e:
        logging.error(f"Retention run failed: {e}")


def retention_loop():
    while True:
        time.sleep(RETENTION_CHECK_SECONDS)
        try:
            apply_retention()
        except Exception as e:  # keep the loop alive whatever happens
            logging.error(f"Retention run failed: {e}")


if RETENTION_CHECK_SECONDS > 0:
    threading.Thread(target=retention_loop, name="retention", daemon=True).start()


def rejoin_loop():
    while True:
        time.sleep(REJOIN_CHECK_SECONDS)
        try:
            monitor_new_broker()
        except Exception as e:  # keep the loop alive whatever happens
            logging.error(f"Rejoin check failed: {e}")


if REJOIN_CHECK_SECONDS > 0:
    threading.Thread(target=rejoin_loop, name="rejoin", daemon=True).start()
