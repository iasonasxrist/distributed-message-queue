"""
Cluster console: a live view of the pub-sub cluster, plus the retention settings.

It reads broker and topic metadata from ZooKeeper, asks each live replica for
its log offsets, and serves one page (index.html) that polls /api/cluster.

The only things it changes are the retention setting and purge requests, both
stored in ZooKeeper at /config/retention where every broker picks them up.
Set CONSOLE_READ_ONLY=1 to disable those.
"""
import hashlib
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from typing import Optional

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, KazooException, NodeExistsError, NoNodeError

ZOOKEEPER_HOST = os.getenv("ZOOKEEPER_HOST", "zookeeper:2181")
# Seconds to wait for a broker before showing its replica as "unknown"
REQUEST_TIMEOUT = float(os.getenv("BROKER_REQUEST_TIMEOUT", "1.5"))
TOPIC_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
# Cluster-wide retention setting, read by every broker
RETENTION_PATH = "/config/retention"
# Retention used until someone changes it here (same variable the brokers read)
DEFAULT_RETENTION_HOURS = float(os.getenv("RETENTION_HOURS", "720"))
MAX_RETENTION_HOURS = 24 * 365 * 10
# When writes are confirmed unless a topic says otherwise (same variable the brokers read)
ACKS = ("all", "majority", "leader")
DEFAULT_ACKS = os.getenv("DEFAULT_ACKS", "all")
# Set to 1 to make the console view-only (no retention changes, no purges)
READ_ONLY = os.getenv("CONSOLE_READ_ONLY", "").lower() in ("1", "true", "yes")
INDEX_HTML = Path(__file__).resolve().parent / "index.html"

zk = KazooClient(hosts=ZOOKEEPER_HOST)
zk.start()
app = FastAPI(title="Pub-Sub Console")
pool = ThreadPoolExecutor(max_workers=16)
http = requests.Session()
# Shared secret of the cluster's own services; brokers refuse calls without it
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "")
if INTERNAL_TOKEN:
    http.headers["X-Internal-Token"] = INTERNAL_TOKEN


# --- Sign-in -----------------------------------------------------------------
# Requests must carry `Authorization: Basic ...` (curl -u user:password) or
# `Authorization: Bearer <token from the auth service>`. This service does not
# check passwords itself: it asks the auth service and remembers the answer
# for AUTH_CACHE_SECONDS, so signing in costs nothing on most requests.
AUTH_ENABLED = os.getenv("AUTH_ENABLED", "1").lower() not in ("0", "false", "no", "")
AUTH_URL = os.getenv("AUTH_URL", "http://auth:7000")
AUTH_CACHE_SECONDS = float(os.getenv("AUTH_CACHE_SECONDS", "30"))
_verified = {}   # hash of the Authorization header -> (valid until, user)


def current_user(authorization: Optional[str] = Header(None)) -> dict:
    """The signed-in user, or 401."""
    if not AUTH_ENABLED:
        return {"username": None, "role": "admin", "must_change_password": False}
    if not authorization:
        raise HTTPException(status_code=401, detail="Sign in first")
    key = hashlib.sha256(authorization.encode("utf-8")).hexdigest()
    remembered = _verified.get(key)
    if remembered and remembered[0] > time.monotonic():
        return remembered[1]
    try:
        response = http.get(f"{AUTH_URL}/verify", headers={"Authorization": authorization}, timeout=5)
    except requests.RequestException:
        raise HTTPException(status_code=503, detail="The auth service is not reachable")
    if response.status_code == 429:
        raise HTTPException(status_code=429, detail="Too many wrong passwords. Wait a minute and try again.")
    if response.status_code != 200:
        raise HTTPException(
            status_code=401, detail="Wrong username or password, or the sign-in has expired"
        )
    user = response.json()
    if len(_verified) > 5000:
        _verified.clear()
    _verified[key] = (time.monotonic() + AUTH_CACHE_SECONDS, user)
    return user


def admin(user: dict = Depends(current_user)) -> dict:
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Only administrators can do this")
    return user


def children(path):
    try:
        return sorted(zk.get_children(path))
    except NoNodeError:
        return []


def partition_key(partition_id):
    return int(partition_id) if str(partition_id).isdigit() else partition_id


def fetch_offsets(broker, topic, partition):
    """Offsets of one replica ({"start_offset", "end_offset", ...}), or None when the broker does not answer."""
    try:
        response = http.get(
            f"http://{broker}:5000/offsets/{topic}/{partition}", timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            body = response.json()
            return {
                "start_offset": int(body.get("start_offset", 0)),
                "end_offset": int(body["end_offset"]),
                "segments": body.get("segments"),
                "oldest_ts": body.get("oldest_ts"),
            }
    except (requests.RequestException, KeyError, TypeError, ValueError):
        pass
    return None


# --- retention setting ----------------------------------------------------------

def read_retention():
    """The cluster retention setting with defaults filled in, and its ZooKeeper version (None if unset)."""
    setting, version = {}, None
    try:
        data, stat = zk.get(RETENTION_PATH)
        version = stat.version
        setting = json.loads(data.decode("utf-8")) if data else {}
    except (NoNodeError, ValueError):
        pass
    default = setting.get("default_hours")
    return {
        "default_hours": DEFAULT_RETENTION_HOURS if default is None else float(default),
        "topics": dict(setting.get("topics") or {}),
        "purge_before": dict(setting.get("purge_before") or {}),
    }, version


def update_retention(mutate, retries=20):
    """Change the retention setting without losing a change made by someone else at the same time."""
    if READ_ONLY:
        raise HTTPException(status_code=403, detail="This console is read-only")
    for _ in range(retries):
        setting, version = read_retention()
        mutate(setting)
        payload = json.dumps(setting).encode("utf-8")
        try:
            if version is None:
                zk.create(RETENTION_PATH, payload, makepath=True)
            else:
                zk.set(RETENTION_PATH, payload, version=version)
            return setting
        except (BadVersionError, NodeExistsError):
            continue
    raise HTTPException(status_code=503, detail="The setting keeps changing, try again")


def check_hours(hours):
    if hours < 0 or hours > MAX_RETENTION_HOURS:
        raise HTTPException(status_code=400, detail=f"Hours must be between 0 and {MAX_RETENTION_HOURS}")
    return float(hours)


def check_topic(topic):
    if not TOPIC_NAME_RE.match(topic):
        raise HTTPException(status_code=400, detail="Invalid topic name")
    if not zk.exists(f"/topics/{topic}"):
        raise HTTPException(status_code=404, detail="Topic not found")


def public_settings(setting):
    return {
        "read_only": READ_ONLY,
        "retention": {"default_hours": setting["default_hours"], "topics": setting["topics"]},
    }


def build_snapshot():
    retention, _ = read_retention()
    alive = children("/brokers")
    alive_set = set(alive)
    known = set(alive)
    topics = []
    lookups = {}  # (topic, partition, broker) -> future

    for topic in children("/topics"):
        try:
            metadata = json.loads(zk.get(f"/topics/{topic}")[0].decode("utf-8"))
        except (NoNodeError, ValueError):
            continue  # deleted while we were reading, or not valid metadata
        partitions = []
        for partition_id in sorted(metadata.get("partitions", {}), key=partition_key):
            data = metadata["partitions"][partition_id]
            leader = data.get("leader")
            followers = [b for b in data.get("followers", []) if b != leader]
            isr = [b for b in data.get("isr", []) if b != leader]
            assigned = data.get("previous_isr", []) or ([leader] if leader else []) + followers
            known.update(b for b in assigned + followers + ([leader] if leader else []) if b)

            replicas = []
            for broker in ([leader] if leader else []) + followers:
                role = "leader" if broker == leader else "follower"
                replicas.append({
                    "broker": broker,
                    "role": role,
                    "in_sync": role == "leader" or broker in isr,
                    "alive": broker in alive_set,
                })
                if broker in alive_set:
                    lookups[(topic, partition_id, broker)] = pool.submit(
                        fetch_offsets, broker, topic, partition_id
                    )

            in_sync_count = sum(1 for r in replicas if r["in_sync"])
            if leader is None:
                status = "offline"
            elif in_sync_count < max(len(assigned), 1):
                status = "under_replicated"
            else:
                status = "healthy"
            partitions.append({
                "id": partition_id,
                "leader": leader,
                "status": status,
                "replicas": replicas,
                "assigned": assigned,
                "in_sync_count": in_sync_count,
                "replica_target": len(assigned),
            })
        topics.append({
            "name": topic,
            "partitions": partitions,
            # how long this topic keeps messages, and whether that is its own setting
            "retention_hours": retention["topics"].get(topic, retention["default_hours"]),
            "retention_is_default": topic not in retention["topics"],
            # when a write to this topic is confirmed: all, majority or leader
            "acks": metadata.get("acks", DEFAULT_ACKS),
        })

    # Attach offsets and lag once all lookups have answered
    for topic in topics:
        for partition in topic["partitions"]:
            for replica in partition["replicas"]:
                future = lookups.get((topic["name"], partition["id"], replica["broker"]))
                offsets = future.result() if future else None
                replica["end_offset"] = offsets["end_offset"] if offsets else None
                replica["start_offset"] = offsets["start_offset"] if offsets else None
            leader = next((r for r in partition["replicas"] if r["role"] == "leader"), None)
            leader_end = leader["end_offset"] if leader else None
            partition["end_offset"] = leader_end
            # offsets below start_offset were purged; "stored" is what the leader still holds
            partition["start_offset"] = leader["start_offset"] if leader else None
            partition["stored"] = (
                leader_end - leader["start_offset"]
                if leader and leader_end is not None and leader["start_offset"] is not None else None
            )
            for replica in partition["replicas"]:
                known_both = leader_end is not None and replica["end_offset"] is not None
                replica["lag"] = max(leader_end - replica["end_offset"], 0) if known_both else None

    brokers = []
    for broker in sorted(known):
        leads = sum(1 for t in topics for p in t["partitions"] if p["leader"] == broker)
        follows = sum(
            1 for t in topics for p in t["partitions"]
            for r in p["replicas"] if r["broker"] == broker and r["role"] == "follower"
        )
        brokers.append({"id": broker, "alive": broker in alive_set, "leader_of": leads, "follower_of": follows})

    all_partitions = [p for t in topics for p in t["partitions"]]
    return {
        "generated_at": time.time(),
        "brokers": brokers,
        "topics": topics,
        "recovering": children("/events/dead_broker"),
        "settings": public_settings(retention),
        "summary": {
            "brokers_alive": len(alive),
            "brokers_known": len(known),
            "topics": len(topics),
            "partitions": len(all_partitions),
            "offline": sum(1 for p in all_partitions if p["status"] == "offline"),
            "under_replicated": sum(1 for p in all_partitions if p["status"] == "under_replicated"),
        },
    }


@app.get("/api/cluster")
def cluster(user: dict = Depends(current_user)):
    try:
        snapshot = build_snapshot()
        # only administrators get the retention and purge controls
        snapshot["settings"]["read_only"] = READ_ONLY or user["role"] != "admin"
        snapshot["user"] = user if AUTH_ENABLED else None
        return snapshot
    except KazooException as e:
        raise HTTPException(status_code=503, detail=f"ZooKeeper is not reachable: {e}")


@app.get("/api/messages/{topic}/{partition}", dependencies=[Depends(current_user)])
def messages(
    topic: str,
    partition: int,
    broker: str = Query(None, description="Replica to read from; defaults to the leader"),
    limit: int = Query(20, ge=1, le=200),
):
    """The last `limit` messages stored by one replica of a partition."""
    if not TOPIC_NAME_RE.match(topic):
        raise HTTPException(status_code=400, detail="Invalid topic name")
    try:
        metadata = json.loads(zk.get(f"/topics/{topic}")[0].decode("utf-8"))
    except NoNodeError:
        raise HTTPException(status_code=404, detail="Topic not found")
    data = metadata.get("partitions", {}).get(str(partition))
    if data is None:
        raise HTTPException(status_code=404, detail="Partition not found")

    # Only ever call brokers that are replicas of this partition and registered in ZooKeeper
    replicas = ([data["leader"]] if data.get("leader") else []) + data.get("followers", [])
    broker = broker or data.get("leader")
    if broker is None:
        raise HTTPException(status_code=503, detail="Partition has no leader")
    if broker not in replicas:
        raise HTTPException(status_code=404, detail="That broker does not hold this partition")
    if broker not in children("/brokers"):
        raise HTTPException(status_code=503, detail="That broker is not running")

    offsets = fetch_offsets(broker, topic, partition)
    if offsets is None:
        raise HTTPException(status_code=503, detail="The broker did not answer")
    end_offset = offsets["end_offset"]
    start = max(end_offset - limit, offsets["start_offset"])
    items = []
    if end_offset > start:
        try:
            response = http.get(
                f"http://{broker}:5000/read/{topic}/{partition}/offset/{start}", timeout=REQUEST_TIMEOUT
            )
        except requests.RequestException:
            raise HTTPException(status_code=503, detail="The broker did not answer")
        if response.status_code != 200:
            raise HTTPException(status_code=502, detail="The broker could not read the partition")
        body = response.json()
        start = body.get("start_offset", start)
        items = [{"offset": start + i, "value": m} for i, m in enumerate(body.get("messages", []))]
    return {"topic": topic, "partition": partition, "broker": broker, "end_offset": end_offset, "messages": items}


@app.get("/api/settings", dependencies=[Depends(current_user)])
def settings():
    setting, _ = read_retention()
    return public_settings(setting)


@app.put("/api/settings/retention", dependencies=[Depends(admin)])
def set_default_retention(default_hours: float = Body(..., embed=True)):
    """How long topics keep messages unless they have their own setting. 0 keeps them forever."""
    hours = check_hours(default_hours)
    return public_settings(update_retention(lambda setting: setting.update(default_hours=hours)))


@app.put("/api/settings/retention/{topic}", dependencies=[Depends(admin)])
def set_topic_retention(topic: str, hours: Optional[float] = Body(None, embed=True)):
    """Retention for one topic. `null` goes back to the cluster default, 0 keeps messages forever."""
    check_topic(topic)
    if hours is not None:
        hours = check_hours(hours)

    def mutate(setting):
        if hours is None:
            setting["topics"].pop(topic, None)
        else:
            setting["topics"][topic] = hours

    return public_settings(update_retention(mutate))


@app.put("/api/settings/acks/{topic}", dependencies=[Depends(admin)])
def set_topic_acks(topic: str, acks: str = Body(..., embed=True)):
    """
    When a write to this topic is confirmed: after `all` followers have it, after
    a `majority` of the replicas have it, or after the `leader` alone has it.
    """
    if READ_ONLY:
        raise HTTPException(status_code=403, detail="This console is read-only")
    check_topic(topic)
    if acks not in ACKS:
        raise HTTPException(status_code=400, detail=f"acks must be one of: {', '.join(ACKS)}")
    for _ in range(20):
        try:
            data, stat = zk.get(f"/topics/{topic}")
        except NoNodeError:
            raise HTTPException(status_code=404, detail="Topic not found")
        metadata = json.loads(data.decode("utf-8"))
        metadata["acks"] = acks
        try:
            zk.set(f"/topics/{topic}", json.dumps(metadata).encode("utf-8"), version=stat.version)
            return {"topic": topic, "acks": acks}
        except BadVersionError:
            continue   # a broker updated the topic at the same moment: try again
    raise HTTPException(status_code=503, detail="The topic keeps changing, try again")


@app.post("/api/purge/{topic}", dependencies=[Depends(admin)])
def purge(topic: str, older_than_hours: float = Body(0, embed=True)):
    """
    Purge now: every broker deletes what it stored for this topic more than
    `older_than_hours` ago (0 = everything stored up to this moment).
    """
    check_topic(topic)
    before = time.time() - check_hours(older_than_hours) * 3600

    def mutate(setting):
        setting["purge_before"][topic] = max(before, float(setting["purge_before"].get(topic, 0) or 0))

    update_retention(mutate)
    return {"topic": topic, "purge_before": before}


def ask_auth_service(path, authorization=None, **body):
    """Pass a sign-in request from the page on to the auth service and return its answer."""
    try:
        response = http.post(
            f"{AUTH_URL}{path}", json=body, timeout=10,
            headers={"Authorization": authorization} if authorization else None,
        )
    except requests.RequestException:
        raise HTTPException(status_code=503, detail="The auth service is not reachable")
    try:
        content = response.json()
    except ValueError:
        content = {"detail": response.text}
    return JSONResponse(status_code=response.status_code, content=content)


@app.post("/api/login")
def login(username: str = Body(...), password: str = Body(...)):
    return ask_auth_service("/login", username=username, password=password)


@app.post("/api/change_password")
def change_password(
    current_password: str = Body(...),
    new_password: str = Body(...),
    authorization: Optional[str] = Header(None),
):
    answer = ask_auth_service(
        "/change_password", authorization, current_password=current_password, new_password=new_password
    )
    _verified.clear()   # forget remembered sign-ins: the old password and tokens are no longer valid
    return answer


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(INDEX_HTML, media_type="text/html")
