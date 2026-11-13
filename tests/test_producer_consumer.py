"""The public APIs: producer (port 4000) and consumer (port 5000 in docker-compose)."""
import hashlib
import json

import requests

import pytest

USER = "ada@example.com"


def partition_of(key, partitions):
    return int(hashlib.sha256(str(key).encode()).hexdigest(), 16) % partitions


def publish(cluster, topic, key, value):
    response = cluster.producer.post(f"/write/{topic}", json={"key": key, "value": value})
    assert response.status_code == 200, response.text
    return response.json()


def values(response):
    assert response.status_code == 200, response.text
    return [m["value"] for m in response.json()["messages"]]


# --- producer ----------------------------------------------------------------

def test_producer_creates_a_topic_through_any_broker(three_brokers):
    response = three_brokers.producer.post("/create_topic/news/3")
    assert response.status_code == 200
    assert len(three_brokers.metadata("news")) == 3


def test_producer_passes_broker_errors_through(three_brokers):
    three_brokers.create_topic("news")
    response = three_brokers.producer.post("/create_topic/news/1")
    assert response.status_code == 400
    assert response.json()["detail"] == "Topic already exists"


def test_producer_routes_a_key_to_its_partition_leader(three_brokers):
    three_brokers.create_topic("news", 3)
    body = publish(three_brokers, "news", "user-1", "hello")

    partition = partition_of("user-1", 3)
    leader = three_brokers.leader("news", partition)
    assert body["broker"] == leader
    assert three_brokers.log(leader, "news", partition) == [{"key": "user-1", "value": "hello"}]


def test_same_key_always_lands_in_the_same_partition(three_brokers):
    three_brokers.create_topic("news", 3)
    for n in range(5):
        publish(three_brokers, "news", "user-1", n)

    partition = partition_of("user-1", 3)
    stored = three_brokers.log(three_brokers.leader("news", partition), "news", partition)
    assert [m["value"] for m in stored] == [0, 1, 2, 3, 4]


def test_keys_spread_over_partitions(three_brokers):
    three_brokers.create_topic("news", 3)
    for n in range(30):
        publish(three_brokers, "news", f"user-{n}", n)

    used = [p for p in range(3) if three_brokers.log(three_brokers.leader("news", p), "news", p)]
    assert used == [0, 1, 2]


def test_producer_accepts_numeric_keys(three_brokers):
    three_brokers.create_topic("news")
    assert publish(three_brokers, "news", 123, "x")["broker"] == "b1"


@pytest.mark.parametrize("body, status", [
    ({"value": "no key"}, 400),
    ({"key": None, "value": "null key"}, 400),
])
def test_producer_requires_a_key(three_brokers, body, status):
    three_brokers.create_topic("news")
    assert three_brokers.producer.post("/write/news", json=body).status_code == status


def test_producer_unknown_topic_is_404(three_brokers):
    assert three_brokers.producer.post("/write/missing", json={"key": "k"}).status_code == 404
    assert three_brokers.producer.delete("/delete_topic/missing").status_code == 404


def test_producer_reports_an_unreachable_leader_as_503(three_brokers):
    three_brokers.create_topic("news")
    three_brokers.disconnect("b1")
    assert three_brokers.producer.post("/write/news", json={"key": "k"}).status_code == 503


def test_producer_follows_the_leader_after_failover(three_brokers):
    three_brokers.create_topic("news")
    publish(three_brokers, "news", "k", "before")
    three_brokers.kill("b1")

    body = publish(three_brokers, "news", "k", "after")

    assert body["broker"] == "b2"
    assert [m["value"] for m in three_brokers.log("b2", "news")] == ["before", "after"]


def test_producer_deletes_a_topic(three_brokers):
    three_brokers.create_topic("news")
    assert three_brokers.producer.delete("/delete_topic/news").status_code == 200
    assert three_brokers.producer.post("/write/news", json={"key": "k"}).status_code == 404


# --- consumer ----------------------------------------------------------------

def test_consumer_must_subscribe_before_reading(three_brokers):
    three_brokers.create_topic("news")
    assert three_brokers.consumer.get(f"/read/{USER}/news/k").status_code == 403


def test_consumer_unknown_topic_is_404(three_brokers):
    three_brokers.consumer.post(f"/subscribe/{USER}/missing")
    assert three_brokers.consumer.get(f"/read/{USER}/missing/k").status_code == 404


def test_consumer_reads_what_the_producer_published(three_brokers):
    three_brokers.create_topic("news")
    three_brokers.consumer.post(f"/subscribe/{USER}/news")
    for value in ("a", "b", "c"):
        publish(three_brokers, "news", "k", value)

    response = three_brokers.consumer.get(f"/read/{USER}/news/k")

    assert values(response) == ["a", "b", "c"]
    assert response.json()["start_offset"] == 0
    assert response.json()["end_offset"] == 3


def test_consumer_gets_each_message_exactly_once(three_brokers):
    three_brokers.create_topic("news")
    consumer = three_brokers.consumer
    consumer.post(f"/subscribe/{USER}/news")
    seen = []

    for batch in (["a", "b"], [], ["c"], [], ["d", "e"]):
        for value in batch:
            publish(three_brokers, "news", "k", value)
        seen += values(consumer.get(f"/read/{USER}/news/k"))

    assert seen == ["a", "b", "c", "d", "e"]


def test_consumer_read_before_any_message_is_empty(three_brokers):
    three_brokers.create_topic("news")
    three_brokers.consumer.post(f"/subscribe/{USER}/news")

    assert values(three_brokers.consumer.get(f"/read/{USER}/news/k")) == []
    publish(three_brokers, "news", "k", "first")
    assert values(three_brokers.consumer.get(f"/read/{USER}/news/k")) == ["first"]


def test_each_subscriber_has_its_own_position(three_brokers):
    three_brokers.create_topic("news")
    consumer = three_brokers.consumer
    consumer.post(f"/subscribe/{USER}/news")
    consumer.post("/subscribe/bob@example.com/news")
    publish(three_brokers, "news", "k", "a")
    assert values(consumer.get(f"/read/{USER}/news/k")) == ["a"]
    publish(three_brokers, "news", "k", "b")

    assert values(consumer.get(f"/read/{USER}/news/k")) == ["b"]
    assert values(consumer.get("/read/bob@example.com/news/k")) == ["a", "b"]


def test_unsubscribed_user_cannot_read(three_brokers):
    three_brokers.create_topic("news")
    consumer = three_brokers.consumer
    consumer.post(f"/subscribe/{USER}/news")
    assert consumer.post(f"/unsubscribe/{USER}/news").status_code == 200

    assert consumer.get(f"/read/{USER}/news/k").status_code == 403
    assert consumer.post(f"/unsubscribe/{USER}/news").status_code == 403


def test_consumer_reads_from_followers_not_the_leader(three_brokers):
    three_brokers.create_topic("news")
    three_brokers.consumer.post(f"/subscribe/{USER}/news")
    publish(three_brokers, "news", "k", "a")
    three_brokers.requests_log.clear()

    three_brokers.consumer.get(f"/read/{USER}/news/k")

    readers = [host for method, host, path in three_brokers.requests_log if path.startswith("/read/")]
    assert readers and readers[0] in ("b2", "b3")


def test_consumer_skips_an_unreachable_replica(three_brokers):
    three_brokers.create_topic("news")
    three_brokers.consumer.post(f"/subscribe/{USER}/news")
    publish(three_brokers, "news", "k", "a")
    three_brokers.disconnect("b2")
    three_brokers.disconnect("b3")

    assert values(three_brokers.consumer.get(f"/read/{USER}/news/k")) == ["a"]   # served by the leader


def test_consumer_never_reads_from_an_out_of_sync_follower(three_brokers):
    three_brokers.create_topic("news")
    three_brokers.consumer.post(f"/subscribe/{USER}/news")
    three_brokers.disconnect("b2")
    publish(three_brokers, "news", "k", "a")      # b2 misses it and leaves the ISR
    three_brokers.reconnect("b2")

    for _ in range(10):
        three_brokers.redis.set(f"offset:{USER}:news:0", 0)
        assert values(three_brokers.consumer.get(f"/read/{USER}/news/k")) == ["a"]


def test_consumer_keeps_its_position_across_a_leader_failover(three_brokers):
    three_brokers.create_topic("news")
    consumer = three_brokers.consumer
    consumer.post(f"/subscribe/{USER}/news")
    publish(three_brokers, "news", "k", "a")
    assert values(consumer.get(f"/read/{USER}/news/k")) == ["a"]

    three_brokers.kill("b1")
    publish(three_brokers, "news", "k", "b")

    assert values(consumer.get(f"/read/{USER}/news/k")) == ["b"]


def test_consumer_reports_503_when_no_replica_answers(three_brokers):
    three_brokers.create_topic("news")
    three_brokers.consumer.post(f"/subscribe/{USER}/news")
    publish(three_brokers, "news", "k", "a")
    for broker in ("b1", "b2", "b3"):
        three_brokers.disconnect(broker)

    assert three_brokers.consumer.get(f"/read/{USER}/news/k").status_code == 503


# --- producer retries --------------------------------------------------------

def test_producer_waits_for_the_new_leader_instead_of_failing(three_brokers, monkeypatch):
    three_brokers.create_topic("news")
    three_brokers.producer_module.RETRY_SECONDS = 30
    three_brokers.disconnect("b1")                     # the leader stops answering ...
    waits = []

    def election_happens_while_waiting(seconds):
        waits.append(seconds)
        if len(waits) == 2:
            three_brokers.kill("b1")                   # ... and is declared dead a moment later

    three_brokers.skip_producer_waits(election_happens_while_waiting)

    body = publish(three_brokers, "news", "k", "survives the failover")

    assert body["broker"] == "b2"
    assert len(waits) == 2
    assert [m["value"] for m in three_brokers.log("b2", "news")] == ["survives the failover"]


def test_producer_retries_when_a_stale_leader_refuses_the_write(three_brokers, monkeypatch):
    three_brokers.create_topic("news")
    three_brokers.producer_module.RETRY_SECONDS = 30
    stale = json.loads(three_brokers.zk.get("/topics/news")[0])
    real_get = three_brokers.producer_module.zk.get
    reads = []

    def metadata_is_stale_once(path):
        reads.append(path)
        if len(reads) == 1:                            # first look still names b2 as leader
            wrong = json.loads(json.dumps(stale))
            wrong["partitions"]["0"]["leader"] = "b2"
            return json.dumps(wrong).encode(), None
        return real_get(path)

    monkeypatch.setattr(three_brokers.producer_module.zk, "get", metadata_is_stale_once)
    three_brokers.skip_producer_waits()

    assert publish(three_brokers, "news", "k", "x")["broker"] == "b1"
    assert three_brokers.log("b2", "news") == [{"key": "k", "value": "x"}]   # only as a follower's copy


def test_producer_gives_up_after_the_retry_period(three_brokers, monkeypatch):
    three_brokers.create_topic("news")
    three_brokers.producer_module.RETRY_SECONDS = 0.2
    three_brokers.disconnect("b1")
    three_brokers.skip_producer_waits()

    response = three_brokers.producer.post("/write/news", json={"key": "k"})

    assert response.status_code == 503
    assert "unreachable" in response.json()["detail"]


def test_producer_does_not_resend_when_the_write_may_have_been_stored(three_brokers, monkeypatch):
    three_brokers.create_topic("news")
    three_brokers.producer_module.RETRY_SECONDS = 30
    attempts = []

    def connection_breaks_after_sending(method, url, **kwargs):
        attempts.append(url)
        raise requests.exceptions.ConnectionError("Connection aborted: remote end closed connection")

    monkeypatch.setattr(three_brokers, "route", connection_breaks_after_sending)

    response = three_brokers.producer.post("/write/news", json={"key": "k"})

    assert response.status_code == 504
    assert len(attempts) == 1
