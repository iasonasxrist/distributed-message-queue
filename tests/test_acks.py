"""
When is a write confirmed?  acks = all | majority | leader

    all       after every follower has the message (default, safest)
    majority  after the leader and enough followers for a majority have it
    leader    after the leader alone has it (fastest)

Plus the rule that makes `majority` safe: when a leader dies, the follower with
the most complete copy takes over.
"""
import json
import threading

import pytest


def messages(count, start=0):
    return [{"n": n} for n in range(start, start + count)]


def create(cluster, acks=None, topic="orders"):
    path = f"/create_topic/{topic}/1" + (f"?acks={acks}" if acks else "")
    response = cluster.producer.post(path)
    assert response.status_code == 200, response.text


def write(cluster, n=0, topic="orders"):
    return cluster.write(topic, 0, {"n": n})


# --- the setting -------------------------------------------------------------------

def test_default_is_to_wait_for_every_follower(three_brokers):
    create(three_brokers)
    assert "acks" not in json.loads(three_brokers.zk.get("/topics/orders")[0])
    assert write(three_brokers).json()["acks"] == "all"


def test_default_comes_from_the_settings_file(cluster, monkeypatch):
    monkeypatch.setenv("DEFAULT_ACKS", "majority")
    cluster.start("b1", "b2", "b3")
    create(cluster)
    assert write(cluster).json()["acks"] == "majority"


@pytest.mark.parametrize("acks", ["all", "majority", "leader"])
def test_a_topic_can_be_created_with_its_own_setting(three_brokers, acks):
    create(three_brokers, acks)
    assert write(three_brokers).json()["acks"] == acks


def test_unknown_setting_is_rejected(three_brokers):
    assert three_brokers.producer.post("/create_topic/orders/1?acks=sometimes").status_code == 400
    assert not three_brokers.zk.exists("/topics/orders")


def test_invalid_default_stops_the_broker_from_starting(cluster, monkeypatch):
    monkeypatch.setenv("DEFAULT_ACKS", "maybe")
    with pytest.raises(ValueError):
        cluster.start("b1")


def test_setting_can_be_changed_from_the_console(three_brokers):
    create(three_brokers)
    console = three_brokers.console

    assert console.put("/api/settings/acks/orders", json={"acks": "majority"}).status_code == 200

    assert console.get("/api/cluster").json()["topics"][0]["acks"] == "majority"
    assert write(three_brokers).json()["acks"] == "majority"          # the leader picked it up at once
    assert three_brokers.partition("orders")["leader"] == "b1"        # the rest of the topic is untouched


@pytest.mark.parametrize("path, body, status", [
    ("/api/settings/acks/orders", {"acks": "never"}, 400),
    ("/api/settings/acks/missing", {"acks": "all"}, 404),
    ("/api/settings/acks/bad.name", {"acks": "all"}, 400),
])
def test_console_rejects_invalid_changes(three_brokers, path, body, status):
    create(three_brokers)
    assert three_brokers.console.put(path, json=body).status_code == status


# --- all ---------------------------------------------------------------------------

def test_all_waits_for_a_slow_follower(three_brokers):
    create(three_brokers, "all")
    three_brokers.delays["b3"] = 0.2

    body = write(three_brokers).json()

    assert body["replicated_to"] == ["b2", "b3"]


def test_all_confirms_with_whatever_followers_are_left(three_brokers):
    create(three_brokers, "all")
    three_brokers.disconnect("b2")
    three_brokers.disconnect("b3")

    assert write(three_brokers).status_code == 200                    # the leader alone is enough
    assert three_brokers.partition("orders")["isr"] == []


# --- majority ----------------------------------------------------------------------

def test_majority_answers_without_waiting_for_the_slow_follower(three_brokers):
    create(three_brokers, "majority")
    three_brokers.delays["b3"] = 0.3

    body = write(three_brokers).json()

    assert body["replicated_to"] == ["b2"]                            # confirmed as soon as b2 had it
    assert three_brokers.log("b3", "orders") == messages(1)          # b3 still received it afterwards
    assert sorted(three_brokers.partition("orders")["isr"]) == ["b2", "b3"]


def test_majority_survives_one_unreachable_follower(three_brokers):
    create(three_brokers, "majority")
    three_brokers.disconnect("b3")

    response = write(three_brokers)

    assert response.status_code == 200
    assert response.json()["replicated_to"] == ["b2"]
    assert three_brokers.partition("orders")["isr"] == ["b2"]


def test_majority_does_not_confirm_when_no_follower_answers(three_brokers):
    create(three_brokers, "majority")
    three_brokers.disconnect("b2")
    three_brokers.disconnect("b3")

    response = write(three_brokers)

    assert response.status_code == 504                                # stored on the leader only: not promised
    assert "may or may not" in response.json()["detail"]


def test_majority_refuses_writes_when_a_majority_is_impossible(three_brokers):
    create(three_brokers, "majority")
    write(three_brokers, 0)
    three_brokers.kill("b2")
    three_brokers.kill("b3")                                          # the leader is the only replica left

    response = write(three_brokers, 1)

    assert response.status_code == 503
    assert three_brokers.log("b1", "orders") == messages(1)          # nothing was stored
    assert three_brokers.producer.post("/write/orders", json={"key": "k"}).status_code == 503


def test_majority_works_again_when_a_follower_returns(three_brokers):
    create(three_brokers, "majority")
    three_brokers.kill("b2")
    three_brokers.kill("b3")
    assert write(three_brokers, 0).status_code == 503

    three_brokers.start_broker("b2")

    assert write(three_brokers, 0).status_code == 200
    assert three_brokers.log("b2", "orders") == messages(1)


def test_majority_with_two_replicas_needs_both(cluster):
    cluster.start("b1", "b2")
    create(cluster, "majority")
    assert write(cluster, 0).json()["replicated_to"] == ["b2"]
    cluster.disconnect("b2")
    assert write(cluster, 1).status_code == 504


# --- leader ------------------------------------------------------------------------

def test_leader_answers_before_any_follower(three_brokers):
    create(three_brokers, "leader")
    three_brokers.delays["b2"] = 0.2
    three_brokers.delays["b3"] = 0.2

    body = write(three_brokers).json()

    assert body["replicated_to"] == []
    for broker in ("b1", "b2", "b3"):                                 # followers still get it afterwards
        assert three_brokers.log(broker, "orders") == messages(1)


def test_leader_confirms_with_every_follower_unreachable(three_brokers):
    create(three_brokers, "leader")
    three_brokers.disconnect("b2")
    three_brokers.disconnect("b3")
    assert write(three_brokers).status_code == 200


# --- order and batching, whatever the setting ----------------------------------------

@pytest.mark.parametrize("acks", ["all", "majority", "leader"])
def test_concurrent_writes_reach_every_replica_in_the_same_order(three_brokers, acks):
    create(three_brokers, acks)

    def writer(start):
        for n in range(start, start + 25):
            assert write(three_brokers, n).status_code == 200

    threads = [threading.Thread(target=writer, args=(i * 25,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    write(three_brokers, 999)                                         # flush anything still on its way

    leader_log = three_brokers.log("b1", "orders")
    assert sorted(m["n"] for m in leader_log) == list(range(200)) + [999]
    assert three_brokers.log("b2", "orders") == leader_log
    assert three_brokers.log("b3", "orders") == leader_log


def test_messages_waiting_for_a_slow_follower_travel_together(three_brokers):
    create(three_brokers, "all")
    three_brokers.delays["b2"] = 0.02
    three_brokers.delays["b3"] = 0.02

    def writer(start):
        for n in range(start, start + 10):
            write(three_brokers, n)

    threads = [threading.Thread(target=writer, args=(i * 10,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    requests_to_b2 = [r for r in three_brokers.requests_log if r[1] == "b2" and r[2].startswith("/replicate/")]
    assert len(three_brokers.log("b2", "orders")) == 80
    assert len(requests_to_b2) < 60                                   # fewer requests than messages: batching


def test_a_follower_far_behind_is_caught_up_in_batches(cluster, monkeypatch):
    monkeypatch.setenv("REPLICATION_BATCH_MESSAGES", "10")
    cluster.start("b1", "b2", "b3")
    create(cluster)
    cluster.disconnect("b2")
    for n in range(35):
        write(cluster, n)
    cluster.reconnect("b2")
    cluster.requests_log.clear()

    write(cluster, 35)

    batches = [r for r in cluster.requests_log if r[1] == "b2" and r[2].startswith("/replicate/")]
    assert cluster.log("b2", "orders") == messages(36)
    assert len(batches) == 5                                          # asked where b2 is, then 10 + 10 + 10 + 6


# --- election: the most complete copy wins -------------------------------------------

def mark_in_sync(cluster, brokers, topic="orders"):
    """Pretend ZooKeeper still lists these followers as in sync (it lags behind reality for a moment)."""
    metadata = json.loads(cluster.zk.get(f"/topics/{topic}")[0])
    metadata["partitions"]["0"]["isr"] = brokers
    cluster.zk.set(f"/topics/{topic}", json.dumps(metadata).encode())


def test_follower_with_the_longest_log_becomes_leader(three_brokers):
    create(three_brokers, "majority")
    write(three_brokers, 0)
    three_brokers.disconnect("b2")
    assert write(three_brokers, 1).status_code == 200                 # confirmed: b1 and b3 have it, b2 does not
    three_brokers.reconnect("b2")
    mark_in_sync(three_brokers, ["b2", "b3"])                         # b2 is listed first and still looks in sync

    three_brokers.kill("b1")

    assert three_brokers.leader("orders") == "b3"                     # not b2, which would have dropped message 1
    write(three_brokers, 2)
    assert three_brokers.log("b3", "orders") == messages(3)
    assert three_brokers.log("b2", "orders") == messages(3)          # b2 was caught up by the new leader


def test_confirmed_messages_survive_a_leader_crash_with_majority(three_brokers):
    create(three_brokers, "majority")
    three_brokers.delays["b2"] = 0.05                                 # b2 is always a little behind
    for n in range(10):
        assert write(three_brokers, n).status_code == 200
    del three_brokers.delays["b2"]

    three_brokers.kill("b1")
    write(three_brokers, 10)

    new_leader = three_brokers.leader("orders")
    assert three_brokers.log(new_leader, "orders") == messages(11)


def test_equal_logs_keep_the_first_follower(three_brokers):
    create(three_brokers)
    write(three_brokers, 0)
    three_brokers.kill("b1")
    assert three_brokers.leader("orders") == "b2"


def test_unreachable_candidate_loses_to_one_that_answers(three_brokers):
    create(three_brokers)
    write(three_brokers, 0)
    three_brokers.disconnect("b2")
    choose = three_brokers.brokers["b3"].most_complete_replica        # b3 runs the election

    assert choose("orders", 0, ["b2", "b3"]) == "b3"


def test_election_still_works_when_no_candidate_answers(three_brokers):
    create(three_brokers)
    write(three_brokers, 0)
    three_brokers.disconnect("b3")
    mark_in_sync(three_brokers, ["b2", "b3"])
    three_brokers.kill("b1")                                          # b2 runs the election and is itself a candidate
    assert three_brokers.leader("orders") == "b2"
