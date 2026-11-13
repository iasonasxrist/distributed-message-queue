"""
What happens when a broker goes away, scenario by scenario.

Every scenario runs twice:
- "crash":        the broker dies without warning (power loss, `docker kill`)
- "planned stop": the broker is shut down normally (`docker stop`, a deploy)

In both cases the cluster must keep every acknowledged message, keep accepting
writes where a replica is left, and take the broker back when it returns.
"""
import threading

import pytest

USER = "ada@example.com"


@pytest.fixture(params=["crash", "planned stop"])
def shut_down(request, three_brokers):
    """A function that takes one broker of the cluster down, in the way named by the parameter."""
    return three_brokers.kill if request.param == "crash" else three_brokers.stop


def messages(count, start=0):
    return [{"n": n} for n in range(start, start + count)]


def write_many(cluster, count, start=0, topic="orders"):
    for message in messages(count, start):
        assert cluster.write(topic, 0, message).status_code == 200


def publish(cluster, value, topic="orders"):
    response = cluster.producer.post(f"/write/{topic}", json={"key": "k", "value": value})
    assert response.status_code == 200, response.text


def read(cluster, topic="orders"):
    response = cluster.consumer.get(f"/read/{USER}/{topic}/k")
    assert response.status_code == 200, response.text
    return [m["value"] for m in response.json()["messages"]]


def status(cluster, topic="orders"):
    snapshot = cluster.console.get("/api/cluster").json()
    return next(t for t in snapshot["topics"] if t["name"] == topic)["partitions"][0]["status"]


def assert_all_replicas_equal(cluster, expected, topic="orders", brokers=("b1", "b2", "b3")):
    for broker in brokers:
        assert cluster.log(broker, topic) == expected, f"{broker} differs"


# === The leader goes away ==========================================================

def test_leader_down_a_follower_takes_over(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    write_many(three_brokers, 3)

    shut_down("b1")

    partition = three_brokers.partition("orders")
    assert partition["leader"] == "b2"
    assert partition["followers"] == ["b3"] and partition["isr"] == ["b3"]
    assert "b1" not in three_brokers.zk.get_children("/brokers")


def test_leader_down_writes_continue_and_nothing_is_lost(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    write_many(three_brokers, 3)

    shut_down("b1")
    write_many(three_brokers, 3, start=3)

    assert_all_replicas_equal(three_brokers, messages(6), brokers=("b2", "b3"))
    assert three_brokers.log("b1", "orders") == messages(3)        # what it had when it went down


def test_leader_down_applications_do_not_notice(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    three_brokers.consumer.post(f"/subscribe/{USER}/orders")
    for value in range(3):
        publish(three_brokers, value)
    assert read(three_brokers) == [0, 1, 2]

    shut_down("b1")
    for value in range(3, 6):
        publish(three_brokers, value)

    assert read(three_brokers) == [3, 4, 5]                         # no gap, no repeat


def test_leader_down_then_back_it_returns_as_a_follower(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    write_many(three_brokers, 3)
    shut_down("b1")
    write_many(three_brokers, 3, start=3)
    assert status(three_brokers) == "under_replicated"

    three_brokers.start_broker("b1")
    write_many(three_brokers, 1, start=6)

    partition = three_brokers.partition("orders")
    assert partition["leader"] == "b2"                              # leadership does not move back
    assert sorted(partition["isr"]) == ["b1", "b3"]
    assert_all_replicas_equal(three_brokers, messages(7))
    assert status(three_brokers) == "healthy"


def test_leader_down_while_one_follower_is_behind(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    three_brokers.disconnect("b2")
    write_many(three_brokers, 2)                                    # b2 misses these
    three_brokers.reconnect("b2")

    shut_down("b1")

    assert three_brokers.leader("orders") == "b3"                   # the complete copy wins
    write_many(three_brokers, 1, start=2)
    assert_all_replicas_equal(three_brokers, messages(3), brokers=("b2", "b3"))   # b2 was caught up


def test_leader_down_with_no_complete_copy_left_the_partition_stops(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    three_brokers.disconnect("b2")
    three_brokers.disconnect("b3")
    write_many(three_brokers, 2)                                    # only the leader has these
    three_brokers.reconnect("b2")
    three_brokers.reconnect("b3")

    shut_down("b1")

    assert three_brokers.leader("orders") is None                   # rather stop than lose messages
    assert status(three_brokers) == "offline"
    assert three_brokers.producer.post("/write/orders", json={"key": "k"}).status_code == 503

    three_brokers.start_broker("b1")                                # the complete copy returns
    write_many(three_brokers, 1, start=2)
    assert three_brokers.leader("orders") == "b1"
    assert_all_replicas_equal(three_brokers, messages(3))


def test_two_leaders_in_a_row_go_down(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    write_many(three_brokers, 2)

    shut_down("b1")
    write_many(three_brokers, 2, start=2)
    shut_down("b2")
    write_many(three_brokers, 2, start=4)

    assert three_brokers.leader("orders") == "b3"
    assert three_brokers.log("b3", "orders") == messages(6)

    three_brokers.start_broker("b1")
    three_brokers.start_broker("b2")
    write_many(three_brokers, 1, start=6)
    assert_all_replicas_equal(three_brokers, messages(7))
    assert status(three_brokers) == "healthy"


def test_leader_of_several_topics_and_partitions_goes_down(three_brokers, shut_down):
    three_brokers.create_topic("orders", 3)
    three_brokers.create_topic("audit", 3)

    shut_down("b1")

    for topic in ("orders", "audit"):
        for partition in three_brokers.metadata(topic).values():
            assert partition["leader"] in ("b2", "b3")
            assert "b1" not in partition["followers"] + partition["isr"]


# === A follower goes away ===========================================================

def test_follower_down_the_leader_stays(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    write_many(three_brokers, 3)

    shut_down("b3")

    partition = three_brokers.partition("orders")
    assert partition["leader"] == "b1"
    assert partition["followers"] == ["b2"] and partition["isr"] == ["b2"]
    assert status(three_brokers) == "under_replicated"


def test_follower_down_writes_and_reads_continue(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    three_brokers.consumer.post(f"/subscribe/{USER}/orders")
    publish(three_brokers, 0)

    shut_down("b3")
    publish(three_brokers, 1)
    response = three_brokers.write("orders", 0, {"key": "k", "value": 2})

    assert response.json()["replicated_to"] == ["b2"]
    assert read(three_brokers) == [0, 1, 2]


def test_follower_down_then_back_it_catches_up(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    write_many(three_brokers, 2)
    shut_down("b3")
    write_many(three_brokers, 4, start=2)                           # written while b3 was away

    three_brokers.start_broker("b3")

    assert three_brokers.log("b3", "orders") == messages(6)
    assert sorted(three_brokers.partition("orders")["isr"]) == ["b2", "b3"]
    assert status(three_brokers) == "healthy"
    write_many(three_brokers, 1, start=6)
    assert_all_replicas_equal(three_brokers, messages(7))


def test_both_followers_down_the_leader_carries_on_alone(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    write_many(three_brokers, 2)

    shut_down("b2")
    shut_down("b3")
    write_many(three_brokers, 2, start=2)

    partition = three_brokers.partition("orders")
    assert (partition["leader"], partition["followers"], partition["isr"]) == ("b1", [], [])

    three_brokers.start_broker("b2")
    three_brokers.start_broker("b3")
    assert_all_replicas_equal(three_brokers, messages(4))
    assert status(three_brokers) == "healthy"


def test_follower_that_was_already_behind_goes_down_and_returns(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    write_many(three_brokers, 1)
    three_brokers.disconnect("b3")
    write_many(three_brokers, 2, start=1)                           # b3 is now behind
    three_brokers.reconnect("b3")

    shut_down("b3")
    three_brokers.start_broker("b3")

    assert three_brokers.log("b3", "orders") == messages(3)
    assert "b3" in three_brokers.partition("orders")["isr"]


def test_follower_down_then_the_leader_too(three_brokers, shut_down):
    three_brokers.create_topic("orders")
    write_many(three_brokers, 2)

    shut_down("b3")
    write_many(three_brokers, 2, start=2)
    shut_down("b1")
    write_many(three_brokers, 2, start=4)

    assert three_brokers.leader("orders") == "b2"
    assert three_brokers.log("b2", "orders") == messages(6)


# === The whole cluster ================================================================

def test_rolling_restart_keeps_every_message(three_brokers, shut_down):
    """Restart the brokers one at a time, as a deploy does, while messages keep coming."""
    three_brokers.create_topic("orders", 3)
    three_brokers.consumer.post(f"/subscribe/{USER}/orders")
    sent = 0
    for broker in ("b1", "b2", "b3"):
        shut_down(broker)
        for _ in range(2):
            publish(three_brokers, sent)
            sent += 1
        three_brokers.start_broker(broker)
        publish(three_brokers, sent)
        sent += 1

    assert read(three_brokers) == list(range(sent))
    snapshot = three_brokers.console.get("/api/cluster").json()
    assert snapshot["summary"]["under_replicated"] == snapshot["summary"]["offline"] == 0
    for partition in snapshot["topics"][0]["partitions"]:
        assert len({r["end_offset"] for r in partition["replicas"]}) == 1
        assert len(partition["replicas"]) == 3


def test_full_shutdown_and_cold_start(three_brokers, shut_down):
    """Every broker goes down; they come back in a different order."""
    three_brokers.create_topic("orders")
    write_many(three_brokers, 2)
    shut_down("b1")
    write_many(three_brokers, 2, start=2)
    shut_down("b2")
    write_many(three_brokers, 2, start=4)
    shut_down("b3")                                                 # b3 was the last leader and has everything

    three_brokers.start_broker("b1")                                # older copies come back first ...
    three_brokers.start_broker("b2")
    assert three_brokers.leader("orders") == "b3"                   # ... and wait for the complete one
    assert three_brokers.producer.post("/write/orders", json={"key": "k"}).status_code == 503

    three_brokers.start_broker("b3")
    three_brokers.tick()                                            # the others notice within seconds and copy from it
    write_many(three_brokers, 1, start=6)

    assert three_brokers.leader("orders") == "b3"
    assert sorted(three_brokers.partition("orders")["isr"]) == ["b1", "b2"]
    assert_all_replicas_equal(three_brokers, messages(7))
    assert status(three_brokers) == "healthy"


def test_single_broker_cluster_goes_down_and_returns(cluster, request):
    cluster.start("b1")
    cluster.create_topic("orders")
    write_many(cluster, 3)

    cluster.stop("b1")
    cluster.start_broker("b1")
    write_many(cluster, 1, start=3)

    assert cluster.leader("orders") == "b1"
    assert cluster.log("b1", "orders") == messages(4)


def test_leader_goes_down_in_the_middle_of_concurrent_writes(three_brokers, shut_down, monkeypatch):
    three_brokers.create_topic("orders")
    three_brokers.producer_module.RETRY_SECONDS = 30
    three_brokers.skip_producer_waits()                             # retry without really waiting
    acknowledged, lock = [], threading.Lock()
    started = threading.Barrier(5)

    def writer(number):
        started.wait()
        for n in range(25):
            response = three_brokers.producer.post("/write/orders", json={"key": "k", "id": f"{number}-{n}"})
            if response.status_code == 200:
                with lock:
                    acknowledged.append(f"{number}-{n}")

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    for thread in threads:
        thread.start()
    started.wait()
    shut_down("b1")                                                 # while the writers are running
    for thread in threads:
        thread.join()

    stored = [m["id"] for m in three_brokers.log(three_brokers.leader("orders"), "orders")]
    assert len(acknowledged) == 100                                 # every write got through
    assert sorted(stored) == sorted(acknowledged)                   # each exactly once
    assert three_brokers.log("b2", "orders") == three_brokers.log("b3", "orders")


def test_shutdown_scenarios_with_sign_in_and_internal_token(cluster, monkeypatch):
    """The same failover works when every call between services needs credentials."""
    import base64
    monkeypatch.setenv("AUTH_ENABLED", "1")
    monkeypatch.setenv("INTERNAL_TOKEN", "cluster-secret")
    cluster.start("b1", "b2", "b3")
    cluster.auth
    admin = {"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode()}
    cluster.producer.post("/create_topic/orders/1", headers=admin)
    cluster.producer.post("/write/orders", json={"key": "k", "value": 0}, headers=admin)

    cluster.stop("b1")                                              # leader, planned
    assert cluster.producer.post("/write/orders", json={"key": "k", "value": 1}, headers=admin).status_code == 200
    cluster.kill("b3")                                              # follower, crash
    assert cluster.producer.post("/write/orders", json={"key": "k", "value": 2}, headers=admin).status_code == 200
    cluster.start_broker("b1")
    cluster.start_broker("b3")

    expected = [{"key": "k", "value": v} for v in range(3)]
    assert_all_replicas_equal(cluster, expected)


def test_broker_that_started_while_its_leader_was_unreachable_joins_later(three_brokers):
    three_brokers.create_topic("orders")
    write_many(three_brokers, 2)
    three_brokers.kill("b3")
    three_brokers.disconnect("b1")                                  # leader unreachable while b3 starts
    three_brokers.start_broker("b3")
    assert "b3" not in three_brokers.partition("orders")["isr"]

    three_brokers.reconnect("b1")
    three_brokers.tick()

    assert three_brokers.log("b3", "orders") == messages(2)
    assert "b3" in three_brokers.partition("orders")["isr"]


def test_follower_that_fell_behind_is_repaired_without_waiting_for_a_write(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.disconnect("b2")
    write_many(three_brokers, 3)                                    # b2 misses these and leaves the ISR
    three_brokers.reconnect("b2")

    three_brokers.tick()

    assert three_brokers.log("b2", "orders") == messages(3)
    assert sorted(three_brokers.partition("orders")["isr"]) == ["b2", "b3"]


def test_periodic_check_does_nothing_on_a_healthy_cluster(three_brokers):
    three_brokers.create_topic("orders", 3)
    write_many(three_brokers, 2)
    writes_before = three_brokers.zookeeper.writes
    three_brokers.requests_log.clear()

    three_brokers.tick()

    assert three_brokers.zookeeper.writes == writes_before
    assert three_brokers.requests_log == []


def test_periodic_check_never_takes_over_an_offline_partition(three_brokers):
    """Only a broker that restarts may bring an offline partition back, not one with an older copy."""
    three_brokers.create_topic("orders")
    three_brokers.disconnect("b2")
    three_brokers.disconnect("b3")
    write_many(three_brokers, 2)
    three_brokers.reconnect("b2")
    three_brokers.reconnect("b3")
    three_brokers.kill("b1")
    assert three_brokers.leader("orders") is None

    three_brokers.tick()

    assert three_brokers.leader("orders") is None
