"""Brokers crashing, losing the network and coming back."""
import json

import pytest
from kazoo.exceptions import KazooException


def messages(count, start=0):
    return [{"n": n} for n in range(start, start + count)]


def write_many(cluster, topic, count, start=0, partition=0):
    for message in messages(count, start):
        assert cluster.write(topic, partition, message).status_code == 200


# --- leader failure ----------------------------------------------------------

def test_leader_crash_promotes_an_in_sync_follower(three_brokers):
    three_brokers.create_topic("orders")
    write_many(three_brokers, "orders", 3)

    three_brokers.kill("b1")

    partition = three_brokers.partition("orders")
    assert partition["leader"] == "b2"
    assert partition["followers"] == ["b3"]
    assert partition["isr"] == ["b3"]


def test_writes_continue_on_the_new_leader_without_losing_messages(three_brokers):
    three_brokers.create_topic("orders")
    write_many(three_brokers, "orders", 3)
    three_brokers.kill("b1")

    write_many(three_brokers, "orders", 2, start=3)

    assert three_brokers.log("b2", "orders") == messages(5)
    assert three_brokers.log("b3", "orders") == messages(5)


def test_only_partitions_led_by_the_dead_broker_change_leader(three_brokers):
    three_brokers.create_topic("orders", 3)

    three_brokers.kill("b2")

    partitions = three_brokers.metadata("orders")
    assert partitions["0"]["leader"] == "b1"
    assert partitions["1"]["leader"] in ("b1", "b3")
    assert partitions["2"]["leader"] == "b3"
    for partition in partitions.values():
        assert "b2" not in partition["followers"] + partition["isr"]


def test_every_topic_is_repaired_after_a_crash(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.create_topic("payments")

    three_brokers.kill("b1")

    assert three_brokers.leader("orders") == "b2"
    assert three_brokers.leader("payments") == "b2"


def test_two_crashes_in_a_row_leave_the_last_broker_in_charge(three_brokers):
    three_brokers.create_topic("orders")
    write_many(three_brokers, "orders", 2)

    three_brokers.kill("b1")
    three_brokers.kill("b2")
    write_many(three_brokers, "orders", 2, start=2)

    partition = three_brokers.partition("orders")
    assert partition == {**partition, "leader": "b3", "followers": [], "isr": []}
    assert three_brokers.log("b3", "orders") == messages(4)


def test_out_of_sync_follower_is_never_promoted(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.disconnect("b2")          # b2 misses the next write and leaves the ISR
    write_many(three_brokers, "orders", 1)
    assert three_brokers.partition("orders")["isr"] == ["b3"]

    three_brokers.kill("b1")

    assert three_brokers.leader("orders") == "b3"


def test_partition_goes_offline_when_no_in_sync_replica_is_left(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.disconnect("b2")
    three_brokers.disconnect("b3")
    write_many(three_brokers, "orders", 1)   # only b1 has this message
    assert three_brokers.partition("orders")["isr"] == []

    three_brokers.kill("b1")
    three_brokers.reconnect("b2")
    three_brokers.reconnect("b3")

    assert three_brokers.leader("orders") is None
    response = three_brokers.producer.post("/write/orders", json={"key": "k"})
    assert response.status_code == 503
    assert three_brokers.broker("b2").post("/write/orders/0", json={"n": 1}).status_code == 503


def test_dead_broker_event_is_cleaned_up(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.kill("b1")
    assert three_brokers.zk.get_children("/events/dead_broker") == []


def test_crash_with_no_topics_does_not_break_the_survivors(three_brokers):
    three_brokers.kill("b1")
    partitions = three_brokers.create_topic("orders", 2)
    assert {p["leader"] for p in partitions.values()} == {"b2", "b3"}


# --- follower failure --------------------------------------------------------

def test_follower_crash_removes_it_from_the_replica_lists(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.kill("b3")

    partition = three_brokers.partition("orders")
    assert partition["leader"] == "b1"
    assert partition["followers"] == ["b2"]
    assert partition["isr"] == ["b2"]


def test_writes_succeed_while_a_follower_is_unreachable(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.disconnect("b3")

    response = three_brokers.write("orders", 0, {"n": 0})

    assert response.status_code == 200
    assert response.json()["replicated_to"] == ["b2"]
    assert three_brokers.partition("orders")["isr"] == ["b2"]
    assert three_brokers.log("b3", "orders") is None


def test_write_succeeds_with_every_follower_down(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.disconnect("b2")
    three_brokers.disconnect("b3")

    assert three_brokers.write("orders", 0, {"n": 0}).status_code == 200
    assert three_brokers.partition("orders")["isr"] == []
    assert three_brokers.log("b1", "orders") == messages(1)


def test_reconnected_follower_is_caught_up_before_it_rejoins_the_isr(three_brokers):
    three_brokers.create_topic("orders")
    write_many(three_brokers, "orders", 1)
    three_brokers.disconnect("b2")
    write_many(three_brokers, "orders", 3, start=1)   # b2 misses these three

    three_brokers.reconnect("b2")
    write_many(three_brokers, "orders", 1, start=4)

    assert three_brokers.log("b2", "orders") == messages(5)
    assert three_brokers.partition("orders")["isr"] == ["b2", "b3"]


# --- brokers coming back -----------------------------------------------------

def test_restarted_follower_copies_what_it_missed_and_rejoins(three_brokers):
    three_brokers.create_topic("orders")
    write_many(three_brokers, "orders", 2)
    three_brokers.kill("b3")
    write_many(three_brokers, "orders", 3, start=2)

    three_brokers.start_broker("b3")

    assert three_brokers.log("b3", "orders") == messages(5)
    partition = three_brokers.partition("orders")
    assert partition["leader"] == "b1"
    assert sorted(partition["followers"]) == ["b2", "b3"]
    assert sorted(partition["isr"]) == ["b2", "b3"]


def test_restarted_follower_receives_new_writes(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.kill("b3")
    three_brokers.start_broker("b3")

    write_many(three_brokers, "orders", 2)

    assert three_brokers.log("b3", "orders") == messages(2)


def test_old_leader_comes_back_as_a_follower(three_brokers):
    three_brokers.create_topic("orders")
    write_many(three_brokers, "orders", 2)
    three_brokers.kill("b1")
    write_many(three_brokers, "orders", 2, start=2)

    three_brokers.start_broker("b1")

    partition = three_brokers.partition("orders")
    assert partition["leader"] == "b2"
    assert "b1" in partition["followers"] and "b1" in partition["isr"]
    assert three_brokers.log("b1", "orders") == messages(4)
    assert three_brokers.broker("b1").post("/write/orders/0", json={"n": 9}).status_code == 409


def test_old_leader_discards_messages_that_were_never_replicated(three_brokers):
    three_brokers.create_topic("orders")
    write_many(three_brokers, "orders", 1)
    three_brokers.disconnect("b2")
    three_brokers.disconnect("b3")
    three_brokers.write("orders", 0, {"n": "only-on-b1"})
    # b1 crashes; pretend b2 was still considered in sync when it happened
    metadata = {"partitions": three_brokers.metadata("orders")}
    metadata["partitions"]["0"]["isr"] = ["b2", "b3"]
    three_brokers.zk.set("/topics/orders", json.dumps(metadata).encode())
    three_brokers.reconnect("b2")
    three_brokers.reconnect("b3")
    three_brokers.kill("b1")
    write_many(three_brokers, "orders", 1, start=1)

    three_brokers.start_broker("b1")

    assert three_brokers.log("b1", "orders") == three_brokers.log("b2", "orders") == messages(2)


def test_restarting_twice_does_not_duplicate_the_broker_in_the_lists(three_brokers):
    three_brokers.create_topic("orders")
    for _ in range(2):
        three_brokers.kill("b3")
        three_brokers.start_broker("b3")

    partition = three_brokers.partition("orders")
    assert partition["followers"].count("b3") == 1
    assert partition["isr"].count("b3") == 1


def test_offline_partition_recovers_when_a_replica_returns(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.disconnect("b2")
    three_brokers.disconnect("b3")
    write_many(three_brokers, "orders", 2)
    three_brokers.kill("b1")
    assert three_brokers.leader("orders") is None

    three_brokers.start_broker("b1")
    three_brokers.reconnect("b2")
    three_brokers.reconnect("b3")
    write_many(three_brokers, "orders", 1, start=2)

    assert three_brokers.leader("orders") == "b1"
    for broker in ("b1", "b2", "b3"):
        assert three_brokers.log(broker, "orders") == messages(3)


def test_single_broker_restart_keeps_data_and_leadership(cluster):
    cluster.start("b1")
    cluster.create_topic("orders")
    write_many(cluster, "orders", 2)

    cluster.kill("b1")           # nobody is left to notice
    cluster.start_broker("b1")
    write_many(cluster, "orders", 1, start=2)

    assert cluster.leader("orders") == "b1"
    assert cluster.log("b1", "orders") == messages(3)


def test_rejoin_is_skipped_while_the_leader_is_unreachable(three_brokers):
    three_brokers.create_topic("orders")
    write_many(three_brokers, "orders", 2)
    three_brokers.kill("b3")
    three_brokers.disconnect("b1")

    three_brokers.start_broker("b3")   # must not crash on startup

    assert "b3" not in three_brokers.partition("orders")["isr"]


def test_brand_new_broker_does_not_join_existing_topics(three_brokers):
    """Current behaviour: only original replicas rejoin (see the TODO in broker.py)."""
    three_brokers.create_topic("orders")
    three_brokers.start_broker("b4")

    partition = three_brokers.partition("orders")
    assert "b4" not in partition["followers"] + partition["isr"]
    assert three_brokers.create_topic("later", 4)["3"]["leader"] == "b4"


# --- metadata updates --------------------------------------------------------

def test_metadata_update_retries_when_another_broker_wrote_first(three_brokers):
    three_brokers.create_topic("orders", 2)
    broker = three_brokers.brokers["b1"]
    real_get = broker.zk.get
    interfered = []

    def get_then_interfere(path):
        result = real_get(path)
        if not interfered:   # another broker updates partition 1 between our read and our write
            interfered.append(True)
            metadata = json.loads(result[0])
            metadata["partitions"]["1"]["isr"] = ["changed-by-someone-else"]
            three_brokers.zk.set(path, json.dumps(metadata).encode())
        return result

    broker.zk.get = get_then_interfere

    def shrink_partition_0(metadata):
        metadata["partitions"]["0"]["isr"] = []

    broker.update_topic_metadata("orders", shrink_partition_0)
    broker.zk.get = real_get

    partitions = three_brokers.metadata("orders")
    assert partitions["0"]["isr"] == []
    assert partitions["1"]["isr"] == ["changed-by-someone-else"]


def test_metadata_update_gives_up_after_too_many_conflicts(three_brokers):
    three_brokers.create_topic("orders")
    broker = three_brokers.brokers["b1"]
    real_get = broker.zk.get

    def always_conflict(path):
        result = real_get(path)
        three_brokers.zk.set(path, result[0])   # bumps the version every time
        return result

    broker.zk.get = always_conflict
    with pytest.raises(KazooException):
        broker.update_topic_metadata("orders", lambda metadata: None, retries=3)
    broker.zk.get = real_get


def test_metadata_update_that_changes_nothing_does_not_write(three_brokers):
    three_brokers.create_topic("orders")
    writes_before = three_brokers.zookeeper.writes
    three_brokers.brokers["b1"].update_topic_metadata("orders", lambda metadata: False)
    assert three_brokers.zookeeper.writes == writes_before


def test_quick_restart_waits_for_the_old_registration_to_expire(three_brokers, monkeypatch):
    """A broker restarted before its old ZooKeeper session expired must not crash on startup."""
    three_brokers.create_topic("orders")
    old_session = three_brokers.brokers["b3"].zk.session
    three_brokers.brokers.pop("b3")
    three_brokers.clients.pop("b3")       # process is gone, but /brokers/b3 is still registered

    def session_expires_during_the_wait(seconds):
        three_brokers.zookeeper.expire(old_session)

    monkeypatch.setattr("time.sleep", session_expires_during_the_wait)
    three_brokers.start_broker("b3")

    assert "b3" in three_brokers.zk.get_children("/brokers")
    assert "b3" in three_brokers.partition("orders")["isr"]


# --- faster failover ---------------------------------------------------------

def test_session_timeout_comes_from_the_settings(cluster, monkeypatch):
    monkeypatch.setenv("ZOOKEEPER_SESSION_TIMEOUT", "6")
    cluster.start("b1")
    assert cluster.zookeeper.last_client_options["timeout"] == 6.0


def test_clean_shutdown_triggers_the_election_immediately(three_brokers):
    three_brokers.create_topic("orders")

    with three_brokers.clients["b1"]:      # start, then stop the app as `docker stop` does
        pass

    assert "b1" not in three_brokers.zk.get_children("/brokers")
    assert three_brokers.leader("orders") == "b2"
