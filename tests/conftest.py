"""
Test harness: a whole pub-sub cluster running inside one Python process.

- `FakeZooKeeper` is an in-memory ZooKeeper with the features the services use:
  versioned znodes, ephemeral nodes tied to a session, children/data watches.
- `Cluster` loads broker.py once per broker (b1, b2, b3 ...), each with its own
  data directory and ZooKeeper session, and routes the HTTP calls the services
  make with `requests` to the right broker. Brokers can be killed, cut off from
  the network and restarted, so failover scenarios run without Docker.
"""
import collections
import importlib.util
import itertools
import json
import os
import sys
import threading
import types
from pathlib import Path
from urllib.parse import urlparse

import anyio
import kazoo.client
import pytest
import requests
from fastapi.testclient import TestClient
from kazoo.exceptions import BadVersionError, NoNodeError, NodeExistsError, NotEmptyError
from urllib3.exceptions import MaxRetryError, NewConnectionError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "broker"))  # broker.py imports partition_log from its own folder

Stat = collections.namedtuple("Stat", "version")
Event = collections.namedtuple("Event", "type path")


class FakeZooKeeper:
    """State shared by every client session of one test."""

    def __init__(self):
        self.nodes = {"/": [b"", 0, None]}  # path -> [data, version, owning session]
        self.child_watches = collections.defaultdict(list)  # path -> [(session, callback)]
        self.data_watches = collections.defaultdict(list)
        self.mutex = threading.RLock()
        self.sessions = itertools.count(1)
        self.writes = 0  # number of successful set() calls, for assertions

    def client(self, *args, **kwargs):
        self.last_client_options = kwargs  # what the service passed to KazooClient(...)
        return FakeZKClient(self, next(self.sessions))

    def children(self, path):
        prefix = path.rstrip("/") + "/"
        return sorted(p[len(prefix):] for p in self.nodes if p.startswith(prefix) and "/" not in p[len(prefix):])

    def _parent(self, path):
        return path.rsplit("/", 1)[0] or "/"

    def _fire_children(self, parent):
        children = self.children(parent)
        for _, callback in list(self.child_watches.get(parent, [])):
            callback(children)

    def _fire_data(self, path, event_type):
        node = self.nodes.get(path)
        data, stat = (node[0], Stat(node[1])) if node else (None, None)
        for watch in list(self.data_watches.get(path, [])):
            if watch[1](data, stat, Event(event_type, path)) is False:   # like kazoo: False ends the watch
                if watch in self.data_watches[path]:
                    self.data_watches[path].remove(watch)

    def expire(self, session):
        """End a session: drop its watches, then remove its ephemeral nodes."""
        with self.mutex:
            for watches in (self.child_watches, self.data_watches):
                for path in list(watches):
                    watches[path] = [w for w in watches[path] if w[0] != session]
            owned = [p for p, node in self.nodes.items() if node[2] == session]
        for path in owned:
            with self.mutex:
                self.nodes.pop(path, None)
            self._fire_data(path, "DELETED")
            self._fire_children(self._parent(path))


class FakeLock:
    def __init__(self, path):
        self.path = path

    def acquire(self, timeout=None):
        return True

    def release(self):
        pass


class FakeZKClient:
    """The subset of kazoo.client.KazooClient used by the services."""

    connected = True

    def __init__(self, store, session):
        self.store = store
        self.session = session

    def start(self):
        pass

    def stop(self):
        self.store.expire(self.session)

    def exists(self, path):
        return Stat(self.store.nodes[path][1]) if path in self.store.nodes else None

    def ensure_path(self, path):
        current = ""
        for part in [p for p in path.split("/") if p]:
            current += "/" + part
            if current not in self.store.nodes:
                self.create(current)

    def create(self, path, value=b"", ephemeral=False, sequence=False, makepath=False):
        with self.store.mutex:
            if path in self.store.nodes:
                raise NodeExistsError()
            parent = self.store._parent(path)
            if parent not in self.store.nodes:
                if not makepath:
                    raise NoNodeError()
                self.ensure_path(parent)
            self.store.nodes[path] = [value, 0, self.session if ephemeral else None]
        self.store._fire_data(path, "CREATED")
        self.store._fire_children(parent)
        return path

    def get(self, path):
        with self.store.mutex:
            if path not in self.store.nodes:
                raise NoNodeError()
            data, version, _ = self.store.nodes[path]
            return data, Stat(version)

    def set(self, path, value, version=-1):
        with self.store.mutex:
            if path not in self.store.nodes:
                raise NoNodeError()
            node = self.store.nodes[path]
            if version != -1 and version != node[1]:
                raise BadVersionError()
            node[0], node[1] = value, node[1] + 1
            self.store.writes += 1
        self.store._fire_data(path, "CHANGED")

    def delete(self, path, recursive=False):
        with self.store.mutex:
            if path not in self.store.nodes:
                raise NoNodeError()
            descendants = [p for p in self.store.nodes if p.startswith(path.rstrip("/") + "/")]
            if descendants and not recursive:
                raise NotEmptyError()
            for p in descendants + [path]:
                del self.store.nodes[p]
        self.store._fire_data(path, "DELETED")
        self.store._fire_children(self.store._parent(path))

    def get_children(self, path):
        with self.store.mutex:
            if path not in self.store.nodes:
                raise NoNodeError()
            return self.store.children(path)

    def Lock(self, path):
        return FakeLock(path)

    def ChildrenWatch(self, path):
        def register(callback):
            self.store.child_watches[path].append((self.session, callback))
            callback(self.store.children(path))
            return callback
        return register

    def DataWatch(self, path):
        def register(callback):
            self.store.data_watches[path].append((self.session, callback))
            node = self.store.nodes.get(path)
            callback(node[0] if node else None, Stat(node[1]) if node else None, None)
            return callback
        return register


class FakeRedis:
    """The subset of redis.Redis used by the consumer."""

    def __init__(self, *args, **kwargs):
        self.data = {}

    def exists(self, key):
        return int(key in self.data)

    def set(self, key, value):
        self.data[key] = str(value)

    def get(self, key):
        return self.data.get(key)

    def delete(self, key):
        self.data.pop(key, None)


class FakeSession:
    """Stands in for requests.Session: every call is routed to a broker of the test cluster."""

    def __init__(self, cluster):
        self.cluster = cluster
        self.headers = {}   # sent with every request, like requests.Session.headers

    def mount(self, prefix, adapter):
        pass

    def _send(self, method, url, **kwargs):
        kwargs["headers"] = {**self.headers, **(kwargs.get("headers") or {})}
        return self.cluster.route(method, url, **kwargs)

    def get(self, url, **kwargs):
        return self._send("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self._send("POST", url, **kwargs)


class FakeAsyncHTTP:
    """Stands in for a service's AsyncHTTP: the call is routed to a broker of the test cluster."""

    def __init__(self, cluster, module):
        self.cluster = cluster
        self.module = module

    async def post(self, url, json=None, timeout=None, connect_timeout=None):
        def send():
            return self.cluster.route("POST", url, json=json, headers=dict(self.module.http.headers))
        try:
            response = await anyio.to_thread.run_sync(send)
        except requests.exceptions.ConnectionError as error:
            reason = getattr(error.args[0], "reason", None) if error.args else None
            if isinstance(reason, NewConnectionError):   # connection refused: nothing happened
                raise self.module.PeerUnreachable(str(error))
            raise self.module.PeerError(str(error))      # broke off mid-way
        return response.status_code, response.json()

    async def close(self):
        pass


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Cluster:
    def __init__(self, tmp_path, monkeypatch):
        self.tmp_path = tmp_path
        self.monkeypatch = monkeypatch
        self.zookeeper = FakeZooKeeper()
        self.zk = self.zookeeper.client()  # a client for the tests themselves
        self.brokers = {}        # broker id -> module (only brokers that are running)
        self.clients = {}        # broker id -> TestClient
        self.unreachable = set() # brokers that are running but cut off from the network
        self.delays = {}         # broker id -> seconds every call to it takes (a slow broker)
        self.requests_log = []   # (method, host, path) of every broker-to-broker / client-to-broker call
        self._loads = itertools.count()
        self._producer = self._consumer = self._console = self._auth = None
        self._loaded = []        # every broker module ever started, to release its threads afterwards

        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("RETENTION_CHECK_SECONDS", "0")   # no background thread: tests trigger retention
        monkeypatch.setenv("REJOIN_CHECK_SECONDS", "0")      # same for the periodic rejoin check: see tick()
        monkeypatch.setenv("PRODUCER_RETRY_SECONDS", "0")    # fail at once unless a test asks for retries
        monkeypatch.setenv("AUTH_ENABLED", "0")              # sign-in is tested in test_auth.py
        monkeypatch.setenv("AUTH_HASH_ITERATIONS", "1000")   # fast password hashes for tests
        monkeypatch.setattr(kazoo.client, "KazooClient", self.zookeeper.client)
        monkeypatch.setattr(requests, "Session", lambda: FakeSession(self))
        monkeypatch.setattr(requests, "post", lambda url, **kw: self.route("POST", url, **kw))
        monkeypatch.setattr(requests, "get", lambda url, **kw: self.route("GET", url, **kw))

    # --- brokers -----------------------------------------------------------
    def start_broker(self, broker_id):
        """Start (or restart) a broker. Its data directory survives restarts."""
        self.monkeypatch.setenv("HOSTNAME", broker_id)
        self.monkeypatch.setenv("BROKER_DATA_DIR", str(self.tmp_path / broker_id / "data"))
        module = load_module(f"broker_{broker_id}_{next(self._loads)}", ROOT / "broker" / "broker.py")
        module.http_async = FakeAsyncHTTP(self, module)
        self.brokers[broker_id] = module
        self.clients[broker_id] = TestClient(module.app, raise_server_exceptions=False)
        self._loaded.append(module)
        return module

    def close(self):
        for module in self._loaded:
            module.replication_pool.shutdown(wait=False)

    def start(self, *broker_ids):
        for broker_id in broker_ids:
            self.start_broker(broker_id)
        return self

    def kill(self, broker_id):
        """Crash a broker: it stops answering and its ZooKeeper session ends."""
        module = self.brokers.pop(broker_id)
        self.clients.pop(broker_id)
        self.unreachable.discard(broker_id)
        self.zookeeper.expire(module.zk.session)

    def skip_producer_waits(self, on_wait=lambda seconds: None):
        """Make the producer's pauses between retries instant; `on_wait(seconds)` runs in their place."""
        self.producer   # make sure the producer is loaded

        async def no_wait(seconds):
            on_wait(seconds)

        self.monkeypatch.setattr(self.producer_module, "wait", no_wait)

    def tick(self):
        """Let a few seconds pass: every running broker runs its periodic rejoin check."""
        for module in list(self.brokers.values()):
            module.monitor_new_broker()

    def stop(self, broker_id):
        """Planned stop (SIGTERM, `docker stop`): the broker says goodbye to ZooKeeper before it exits."""
        self.brokers.pop(broker_id)
        client = self.clients.pop(broker_id)
        self.unreachable.discard(broker_id)
        with client:   # starting and leaving the app runs its shutdown hook
            pass

    def disconnect(self, broker_id):
        """Network fault: the broker keeps its ZooKeeper session but nobody can reach it."""
        self.unreachable.add(broker_id)

    def reconnect(self, broker_id):
        self.unreachable.discard(broker_id)

    def route(self, method, url, **kwargs):
        parsed = urlparse(url)
        host = parsed.hostname
        if host == "broker":  # docker-compose service name: any running broker
            host = next((b for b in sorted(self.brokers) if b not in self.unreachable), None)
        if host == "auth":   # the auth service
            if self._auth is None or "auth" in self.unreachable:
                raise requests.exceptions.ConnectionError("auth is unreachable")
            kwargs.pop("timeout", None)
            return self._auth.request(method, parsed.path, **kwargs)
        self.requests_log.append((method, host, parsed.path))
        if host not in self.clients or host in self.unreachable:
            # the same error requests raises for "connection refused"
            refused = NewConnectionError(None, f"{parsed.hostname} is unreachable")
            raise requests.exceptions.ConnectionError(MaxRetryError(None, url, reason=refused))
        kwargs.pop("timeout", None)
        if host in self.delays:
            threading.Event().wait(self.delays[host])
        return self.clients[host].request(method, parsed.path, **kwargs)

    def broker(self, broker_id):
        return self.clients[broker_id]

    # --- other services ----------------------------------------------------
    @property
    def producer(self):
        if self._producer is None:
            module = load_module("producer_under_test", ROOT / "producer" / "producer.py")
            self.producer_module = module
            module.http_async = FakeAsyncHTTP(self, module)
            self._producer = TestClient(module.app, raise_server_exceptions=False)
        return self._producer

    @property
    def consumer(self):
        if self._consumer is None:
            if "redis" not in sys.modules:
                try:
                    import redis  # noqa: F401
                except ImportError:  # the tests do not need the real package
                    self.monkeypatch.setitem(sys.modules, "redis", types.SimpleNamespace(Redis=FakeRedis))
            self.monkeypatch.setattr(sys.modules["redis"], "Redis", FakeRedis)
            module = load_module("consumer_under_test", ROOT / "consumer" / "consumer.py")
            self.redis = module.r
            self._consumer = TestClient(module.app, raise_server_exceptions=False)
        return self._consumer

    @property
    def auth(self):
        if self._auth is None:
            self.auth_module = load_module("auth_under_test", ROOT / "auth" / "auth.py")
            self._auth = TestClient(self.auth_module.app, raise_server_exceptions=False)
        return self._auth

    @property
    def console(self):
        if self._console is None:
            module = load_module("console_under_test", ROOT / "console" / "console.py")
            self.console_module = module
            self._console = TestClient(module.app, raise_server_exceptions=False)
        return self._console

    # --- helpers -----------------------------------------------------------
    def create_topic(self, topic, partitions=1):
        response = self.producer.post(f"/create_topic/{topic}/{partitions}")
        assert response.status_code == 200, response.text
        return self.metadata(topic)

    def metadata(self, topic):
        return json.loads(self.zk.get(f"/topics/{topic}")[0])["partitions"]

    def partition(self, topic, partition=0):
        return self.metadata(topic)[str(partition)]

    def leader(self, topic, partition=0):
        return self.partition(topic, partition)["leader"]

    def write(self, topic, partition, message):
        """Write straight to the current leader of a partition."""
        return self.broker(self.leader(topic, partition)).post(f"/write/{topic}/{partition}", json=message)

    def log(self, broker_id, topic, partition=0):
        """Messages stored on disk by one broker (works for stopped brokers too)."""
        records = self.records(broker_id, topic, partition)
        return None if records is None else [record["m"] for record in records]

    def records(self, broker_id, topic, partition=0):
        """Stored records ({"ts": ..., "m": message}) of one broker, oldest first."""
        directory = self.tmp_path / broker_id / "data" / topic / f"partition_{partition}"
        if not directory.is_dir():
            return None
        return [
            json.loads(line)
            for segment in sorted(directory.glob("*.log"))
            for line in segment.read_text().splitlines() if line.strip()
        ]

    def segments(self, broker_id, topic, partition=0):
        """First offset of each segment file one broker holds for a partition."""
        directory = self.tmp_path / broker_id / "data" / topic / f"partition_{partition}"
        return [int(path.stem) for path in sorted(directory.glob("*.log"))]

    def set_retention(self, setting):
        """Write the cluster retention setting, as the console does."""
        if self.zk.exists("/config/retention"):
            self.zk.set("/config/retention", json.dumps(setting).encode())
        else:
            self.zk.create("/config/retention", json.dumps(setting).encode(), makepath=True)


@pytest.fixture
def cluster(tmp_path, monkeypatch):
    """An empty cluster; call cluster.start("b1", ...) to add brokers."""
    cluster = Cluster(tmp_path, monkeypatch)
    yield cluster
    cluster.close()


@pytest.fixture
def three_brokers(cluster):
    """Three running brokers b1, b2, b3."""
    return cluster.start("b1", "b2", "b3")
