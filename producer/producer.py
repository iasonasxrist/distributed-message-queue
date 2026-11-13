from typing import Optional

import aiohttp
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
import requests
import asyncio
import os
import json
import hashlib
import time
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError


ZOOKEEPER_HOST = os.getenv("ZOOKEEPER_HOST", "zookeeper:2181")
# Seconds to wait for a broker before giving up
REQUEST_TIMEOUT = float(os.getenv("BROKER_REQUEST_TIMEOUT", "10"))
# Seconds to wait for a connection to a broker (a dead broker is noticed this fast)
CONNECT_TIMEOUT = float(os.getenv("BROKER_CONNECT_TIMEOUT", "2"))
# How long a write keeps trying while a partition has no reachable leader,
# for example during a failover (0 = fail at once)
RETRY_SECONDS = float(os.getenv("PRODUCER_RETRY_SECONDS", "15"))
zk = KazooClient(hosts=ZOOKEEPER_HOST)
# One connection pool for all calls to brokers (connections are reused)
http = requests.Session()
http.mount("http://", requests.adapters.HTTPAdapter(pool_connections=16, pool_maxsize=64))
# Shared secret of the cluster's own services; brokers refuse calls without it
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "")
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

zk.start()
app = FastAPI()


async def wait(seconds: float) -> None:
    """Pause between two attempts of a write without holding up other requests."""
    await asyncio.sleep(seconds)


def topic_metadata(topic_ID: str) -> dict:
    data, _ = zk.get(f"/topics/{topic_ID}")
    return json.loads(data.decode("utf-8"))


# --- Sign-in -----------------------------------------------------------------
# Requests must carry `Authorization: Basic ...` (curl -u user:password) or
# `Authorization: Bearer <token from the auth service>`. This service does not
# check passwords itself: it asks the auth service and remembers the answer
# for AUTH_CACHE_SECONDS, so signing in costs nothing on most requests.
AUTH_ENABLED = os.getenv("AUTH_ENABLED", "1").lower() not in ("0", "false", "no", "")
AUTH_URL = os.getenv("AUTH_URL", "http://auth:7000")
AUTH_CACHE_SECONDS = float(os.getenv("AUTH_CACHE_SECONDS", "30"))
_verified = {}   # hash of the Authorization header -> (valid until, user)


def verify_with_auth_service(authorization: str):
    return http.get(f"{AUTH_URL}/verify", headers={"Authorization": authorization}, timeout=5)


async def current_user(authorization: Optional[str] = Header(None)) -> dict:
    """The signed-in user, or 401."""
    if not AUTH_ENABLED:
        return {"username": None, "role": "admin", "must_change_password": False}
    if not authorization:
        raise HTTPException(status_code=401, detail="Sign in first", headers={"WWW-Authenticate": 'Basic realm="pubsub"'})
    key = hashlib.sha256(authorization.encode("utf-8")).hexdigest()
    remembered = _verified.get(key)
    if remembered and remembered[0] > time.monotonic():
        return remembered[1]   # the usual case: no call, no waiting
    try:
        response = await run_in_threadpool(verify_with_auth_service, authorization)
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


def as_response(response):
    try:
        body = response.json()
    except ValueError:
        body = {"detail": response.text}
    return JSONResponse(status_code=response.status_code, content=body)


def forward_to_broker(url: str, **kwargs):
    """POST to a broker and pass its answer (body and status code) back to the caller."""
    try:
        response = http.post(url, timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT), **kwargs)
    except requests.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Broker unavailable: {e}")
    return as_response(response)


# API: Create a new topic
@app.post("/create_topic/{topic_ID}/{num_partitions}", dependencies=[Depends(admin)])
def create_topic(topic_ID: str, num_partitions: int, acks: Optional[str] = None):
    broker_url = f"http://broker:5000/create_topic/{topic_ID}/{num_partitions}"  # Send to one broker
    return forward_to_broker(broker_url, params={"acks": acks} if acks else None)


# API: Write a message to a topic using key-based partitioning
@app.post("/write/{topic_ID}", dependencies=[Depends(current_user)])
async def write_message(topic_ID: str, message: dict):
    # Use key-based hashing to determine partition
    key = message.get("key")
    if key is None:
        raise HTTPException(status_code=400, detail="Key is required for partition selection")

    # While a leader is being replaced the write is tried again for up to
    # RETRY_SECONDS, so the caller sees one slower write instead of an error.
    deadline = time.monotonic() + RETRY_SECONDS
    delay = 0.1
    while True:
        try:
            metadata = await run_in_threadpool(topic_metadata, topic_ID)
        except NoNodeError:
            raise HTTPException(status_code=404, detail="Topic not found")
        partitions = list(metadata["partitions"].keys())

        # str() so numeric keys work and hash like the same key in the consumer's URL
        partition_id = int(hashlib.sha256(str(key).encode()).hexdigest(), 16) % len(partitions)
        leader_broker = metadata["partitions"][str(partition_id)]["leader"]

        if leader_broker is None:
            problem = f"Partition {partition_id} has no leader"
        else:
            # Write message to leader broker
            write_url = f"http://{leader_broker}:5000/write/{topic_ID}/{partition_id}"
            try:
                status, body = await http_async.post(
                    write_url, json=message, timeout=REQUEST_TIMEOUT, connect_timeout=CONNECT_TIMEOUT
                )
            except PeerUnreachable:
                # Connection refused or host gone: the message certainly was not stored, so try again
                problem = f"Leader {leader_broker} of partition {partition_id} is unreachable"
            except PeerError:
                # The broker may have stored the message before the connection broke.
                # Sending it again could store it twice, so the caller decides.
                raise HTTPException(
                    status_code=504,
                    detail=f"No confirmation from {leader_broker}; the message may or may not have been stored",
                )
            else:
                # 409: that broker is no longer the leader. 503: no leader yet. Nothing was stored.
                if status not in (409, 503):
                    return JSONResponse(status_code=status, content=body)
                problem = f"Partition {partition_id} is changing leader"

        if time.monotonic() + delay > deadline:
            raise HTTPException(status_code=503, detail=problem)
        await wait(delay)
        delay = min(delay * 2, 1.0)


# API: Delete a topic
@app.delete("/delete_topic/{topic_ID}", dependencies=[Depends(admin)])
def delete_topic(topic_ID: str):
    topic_path = f"/topics/{topic_ID}"
    if zk.exists(topic_path):
        zk.delete(topic_path, recursive=True)
        return {"status": f"Topic {topic_ID} deleted"}
    raise HTTPException(status_code=404, detail="Topic not found")
