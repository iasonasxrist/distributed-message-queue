"""Reads, writes and replication on a healthy cluster."""
import threading

import pytest


def messages(count, start=0):
    return [{"n": n} for n in range(start, start + count)]


# --- topics ------------------------------------------------------------------

def test_create_topic_spreads_leaders_over_brokers(three_brokers):
    partitions = three_brokers.create_topic("orders", 3)

    assert [partitions[p]["leader"] for p in ("0", "1", "2")] == ["b1", "b2", "b3"]
    for partition in partitions.values():
        assert partition["leader"] not in partition["followers"]
        assert sorted(partition["followers"]) == sorted(partition["isr"])
        assert len(partition["followers"]) == 2


def test_create_topic_twice_is_rejected(three_brokers):
    three_brokers.create_topic("orders")
    response = three_brokers.broker("b2").post("/create_topic/orders/1")
    assert response.status_code == 400
    assert response.json()["detail"] == "Topic already exists"


@pytest.mark.parametrize("partitions", [0, -1])
def test_create_topic_needs_a_positive_partition_count(three_brokers, partitions):
    assert three_brokers.broker("b1").post(f"/create_topic/orders/{partitions}").status_code == 400


@pytest.mark.parametrize("name", ["a.b", "a b", "has$sign", "x" * 129])
def test_unsafe_topic_names_are_rejected_everywhere(three_brokers, name):
    broker = three_brokers.broker("b1")
    assert broker.post(f"/create_topic/{name}/1").status_code == 400
    assert broker.post(f"/write/{name}/0", json={"n": 1}).status_code == 400
    assert broker.post(f"/replicate/{name}/0", json={"messages": []}).status_code == 400
    assert broker.get(f"/read/{name}/0").status_code == 400
    assert not (three_brokers.tmp_path / "b1" / "data").exists()


# --- writes ------------------------------------------------------------------

def test_write_is_stored_on_the_leader_and_every_follower(three_brokers):
    three_brokers.create_topic("orders")
    for message in messages(5):
        assert three_brokers.write("orders", 0, message).status_code == 200

    for broker in ("b1", "b2", "b3"):
        assert three_brokers.log(broker, "orders") == messages(5)


def test_write_reports_which_followers_have_the_message(three_brokers):
    three_brokers.create_topic("orders")
    body = three_brokers.write("orders", 0, {"n": 0}).json()
    assert body["broker"] == "b1"
    assert body["replicated_to"] == ["b2", "b3"]


def test_follower_refuses_writes_and_names_the_leader(three_brokers):
    three_brokers.create_topic("orders")
    response = three_brokers.broker("b2").post("/write/orders/0", json={"n": 0})

    assert response.status_code == 409
    assert response.json()["detail"]["leader"] == "b1"
    assert three_brokers.log("b2", "orders") is None


def test_write_to_unknown_topic_or_partition_leaves_no_file(three_brokers):
    three_brokers.create_topic("orders")
    broker = three_brokers.broker("b1")

    assert broker.post("/write/missing/0", json={"n": 0}).status_code == 404
    assert broker.post("/write/orders/9", json={"n": 0}).status_code == 404
    assert three_brokers.log("b1", "missing") is None
    assert three_brokers.log("b1", "orders", 9) is None


def test_steady_writes_do_not_touch_zookeeper(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.write("orders", 0, {"n": 0})
    writes_before = three_brokers.zookeeper.writes

    for message in messages(20, start=1):
        three_brokers.write("orders", 0, message)

    assert three_brokers.zookeeper.writes == writes_before


def test_concurrent_writes_keep_the_same_order_on_every_replica(three_brokers):
    three_brokers.create_topic("orders")

    def writer(start):
        for message in messages(25, start):
            assert three_brokers.write("orders", 0, message).status_code == 200

    threads = [threading.Thread(target=writer, args=(i * 25,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    leader_log = three_brokers.log("b1", "orders")
    assert sorted(m["n"] for m in leader_log) == list(range(200))
    assert three_brokers.log("b2", "orders") == leader_log
    assert three_brokers.log("b3", "orders") == leader_log


def test_partitions_of_one_topic_are_independent(three_brokers):
    three_brokers.create_topic("orders", 3)
    three_brokers.write("orders", 0, {"p": 0})
    three_brokers.write("orders", 1, {"p": 1})

    assert three_brokers.log("b1", "orders", 0) == [{"p": 0}]
    assert three_brokers.log("b1", "orders", 1) == [{"p": 1}]
    assert three_brokers.log("b1", "orders", 2) is None


# --- reads -------------------------------------------------------------------

def test_read_all_returns_every_message(three_brokers):
    three_brokers.create_topic("orders")
    for message in messages(3):
        three_brokers.write("orders", 0, message)

    body = three_brokers.broker("b2").get("/read/orders/0").json()
    assert body["messages"] == messages(3)
    assert (body["start_offset"], body["end_offset"], body["log_start_offset"]) == (0, 3, 0)
    assert len(body["timestamps"]) == 3


def test_read_before_any_write_is_404(three_brokers):
    three_brokers.create_topic("orders")
    assert three_brokers.broker("b1").get("/read/orders/0").status_code == 404
    assert three_brokers.broker("b1").get("/read/orders/0/offset/0").status_code == 404


@pytest.mark.parametrize("offset, expected, start", [
    (0, [0, 1, 2], 0),
    (1, [1, 2], 1),
    (3, [], 3),      # caught up
    (50, [], 3),     # past the end: clamped
])
def test_read_from_offset(three_brokers, offset, expected, start):
    three_brokers.create_topic("orders")
    for message in messages(3):
        three_brokers.write("orders", 0, message)

    response = three_brokers.broker("b1").get(f"/read/orders/0/offset/{offset}")
    assert response.status_code == 200
    body = response.json()
    assert [m["n"] for m in body["messages"]] == expected
    assert body["start_offset"] == start
    assert body["end_offset"] == 3


def test_negative_offset_is_rejected(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.write("orders", 0, {"n": 0})
    assert three_brokers.broker("b1").get("/read/orders/0/offset/-1").status_code == 400


def test_polling_with_end_offset_delivers_each_message_once(three_brokers):
    three_brokers.create_topic("orders")
    offset, seen = 0, []
    for batch in ([0, 1], [], [2], [3, 4, 5], []):
        for n in batch:
            three_brokers.write("orders", 0, {"n": n})
        if three_brokers.log("b1", "orders") is None:
            continue
        body = three_brokers.broker("b1").get(f"/read/orders/0/offset/{offset}").json()
        seen += [m["n"] for m in body["messages"]]
        offset = body["end_offset"]

    assert seen == [0, 1, 2, 3, 4, 5]


def test_offsets_endpoint_reports_the_log_end(three_brokers):
    three_brokers.create_topic("orders")
    assert three_brokers.broker("b3").get("/offsets/orders/0").json()["end_offset"] == 0
    for message in messages(4):
        three_brokers.write("orders", 0, message)
    body = three_brokers.broker("b3").get("/offsets/orders/0").json()
    assert (body["broker"], body["start_offset"], body["end_offset"], body["segments"]) == ("b3", 0, 4, 1)


# --- replication protocol ----------------------------------------------------

def test_follower_behind_the_leader_asks_for_the_missing_messages(three_brokers):
    three_brokers.create_topic("orders")
    response = three_brokers.broker("b2").post(
        "/replicate/orders/0", json={"messages": [{"n": 5}], "base_offset": 5}
    )
    assert response.status_code == 409
    assert response.json()["detail"]["end_offset"] == 0
    assert three_brokers.log("b2", "orders") is None


def test_follower_ahead_of_the_leader_drops_its_extra_messages(three_brokers):
    three_brokers.create_topic("orders")
    follower = three_brokers.broker("b2")
    follower.post("/replicate/orders/0", json={"messages": messages(3), "base_offset": 0})

    # the leader's log only has one message before this one
    follower.post("/replicate/orders/0", json={"messages": [{"n": "new"}], "base_offset": 1})

    assert three_brokers.log("b2", "orders") == [{"n": 0}, {"n": "new"}]
