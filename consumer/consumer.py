from typing import Optional
import time

from fastapi import Depends, FastAPI, Header, HTTPException
import redis
import hashlib
import json
import requests
import os
import random
from kazoo.client import KazooClient


app = FastAPI()

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
# Seconds to wait for a broker before trying the next replica
REQUEST_TIMEOUT = float(os.getenv("BROKER_REQUEST_TIMEOUT", "5"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
# One connection pool for all calls to brokers (connections are reused)
http = requests.Session()
http.mount("http://", requests.adapters.HTTPAdapter(pool_connections=16, pool_maxsize=64))
# Shared secret of the cluster's own services; brokers refuse calls without it
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "")
if INTERNAL_TOKEN:
    http.headers["X-Internal-Token"] = INTERNAL_TOKEN

# Connect to Zookeeper
ZOOKEEPER_HOST = os.getenv("ZOOKEEPER_HOST", "zookeeper:2181")
zk = KazooClient(hosts=ZOOKEEPER_HOST)
zk.start()


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
        raise HTTPException(status_code=401, detail="Sign in first", headers={"WWW-Authenticate": 'Basic realm="pubsub"'})
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
            status_code=401, detail="Wrong username or password, or the sign-in has expired", headers={"WWW-Authenticate": 'Basic realm="pubsub"'}
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


@app.post("/subscribe/{email}/{topic_ID}", dependencies=[Depends(current_user)])
def subscribe(email:str, topic_ID: str):
    offset_key = f"offset:{email}:{topic_ID}"
    if not r.exists(offset_key):
        r.set(offset_key, 0)
    return {"status": f"{email} subscribed to {topic_ID}"}


@app.post("/unsubscribe/{email}/{topic_ID}", dependencies=[Depends(current_user)])
def unsubscribe(email:str, topic_ID: str):
    
    offset_key = f"offset:{email}:{topic_ID}"
    #Check that user has subscribed to the topic
    if not r.exists(offset_key):
        raise HTTPException(status_code=403, detail="Not subscribed to topic")
    
    r.delete(offset_key)
    return {"status": f"{email} unsubscribed from {topic_ID}"}



@app.get("/read/{email}/{topic_ID}/{key}", dependencies=[Depends(current_user)])
def read_by_key(email: str, topic_ID: str, key: str):
    
    # If email has not subscribed to the topic, return an error
    offset_key = f"offset:{email}:{topic_ID}"
    if not r.exists(offset_key):
        raise HTTPException(status_code=403, detail="Not subscribed to topic")
    
    # 1. Get topic metadata from Zookeeper
    topic_path = f"/topics/{topic_ID}"
    if not zk.exists(topic_path):
        raise HTTPException(status_code=404, detail="Topic not found")

    topic_metadata = json.loads(zk.get(topic_path)[0].decode("utf-8"))
    partitions = list(topic_metadata["partitions"].keys())
    num_partitions = len(partitions)

    # 2. Compute the partition based on key
    partition_id = int(hashlib.sha256(key.encode()).hexdigest(), 16) % num_partitions
    partition_info = topic_metadata["partitions"][str(partition_id)]
    # Read from an in-sync follower (random, to spread load) and fall back to
    # the other in-sync followers and finally the leader if one is unreachable.
    leader = partition_info.get("leader")
    candidates = [b for b in partition_info.get("isr", []) if b != leader]
    random.shuffle(candidates)
    if leader is not None:
        candidates.append(leader)
    if not candidates:
        raise HTTPException(status_code=503, detail="Partition has no available replica")

    # 3. Get current offset from Redis
    offset_key = f"offset:{email}:{topic_ID}:{partition_id}"
    offset = int(r.get(offset_key) or 0)
    

    # 4. Read from broker partition API (with offset)
    messages, start_offset, end_offset, answered = [], offset, offset, False
    for selected_broker in candidates:
        broker_read_url = f"http://{selected_broker}:5000/read/{topic_ID}/{partition_id}/offset/{offset}"
        try:
            response = http.get(broker_read_url, timeout=REQUEST_TIMEOUT)
        except requests.RequestException:
            continue  # replica unreachable, try the next one
        if response.status_code == 404:
            answered = True  # nothing has been written to this partition yet
            break
        if response.status_code == 200:
            body = response.json()
            messages = body.get("messages", [])
            end_offset = body.get("end_offset", offset)
            # later than `offset` when older messages were purged before this subscriber read them
            start_offset = body.get("start_offset", offset)
            answered = True
            break
    if not answered:
        raise HTTPException(status_code=503, detail="Failed to read from any replica")

    # 5. Update the offset in Redis
    r.set(offset_key, end_offset)

    return {
        "email": email,
        "key": key,
        "partition_id": partition_id,
        "start_offset": start_offset,
        "end_offset": end_offset,
        "messages": messages
    }
