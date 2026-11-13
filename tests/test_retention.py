"""Segmented partition logs, retention by age, and "purge now"."""
import json
import os
import time

import pytest

from partition_log import PartitionLog

DAY = 24 * 3600


def messages(count, start=0):
    return [{"n": n} for n in range(start, start + count)]


# --- PartitionLog on its own ----------------------------------------------------

def new_log(tmp_path, max_messages=5):
    return PartitionLog(str(tmp_path / "partition_0"), max_messages)


def bases(log):
    return [segment.base for segment in log.segments]


def test_a_new_segment_starts_when_the_current_one_is_full(tmp_path):
    log = new_log(tmp_path)
    log.append(messages(12))

    assert bases(log) == [0, 5, 10]
    assert [segment.count for segment in log.segments] == [5, 5, 2]
    assert (log.start_offset, log.end_offset) == (0, 12)
    assert sorted(os.listdir(log.directory)) == [
        "00000000000000000000.log", "00000000000000000005.log", "00000000000000000010.log",
    ]


def test_read_starts_at_the_requested_offset_across_segments(tmp_path):
    log = new_log(tmp_path)
    log.append(messages(12))

    start, found, timestamps = log.read(7)

    assert start == 7
    assert found == messages(5, start=7)
    assert len(timestamps) == 5


def test_read_past_the_end_returns_nothing(tmp_path):
    log = new_log(tmp_path)
    log.append(messages(3))
    assert log.read(50) == (3, [], [])


def test_state_is_rebuilt_from_disk(tmp_path):
    log = new_log(tmp_path)
    log.append(messages(12), [100.0 + n for n in range(12)])

    reopened = new_log(tmp_path)

    assert bases(reopened) == [0, 5, 10]
    assert (reopened.start_offset, reopened.end_offset) == (0, 12)
    assert reopened.read(0)[1] == messages(12)
    assert reopened.segments[0].last_ts == 104.0


def test_purge_deletes_whole_old_segments_and_keeps_offsets(tmp_path):
    log = new_log(tmp_path)
    log.append(messages(12), [100.0 + n for n in range(12)])   # stored at t=100..111

    removed = log.purge(older_than_ts=105.0)                    # only segment 0 is entirely older

    assert removed == 5
    assert bases(log) == [5, 10]
    assert (log.start_offset, log.end_offset) == (5, 12)
    assert log.read(0) == (5, messages(7, start=5), [105.0 + n for n in range(7)])
    assert log.read(8)[1] == messages(4, start=8)               # offset 8 is still message 8


def test_segment_with_one_newer_message_is_kept(tmp_path):
    log = new_log(tmp_path)
    log.append(messages(12), [100.0 + n for n in range(12)])

    assert log.purge(older_than_ts=104.0) == 0                  # segment 0 holds t=104, not older
    assert bases(log) == [0, 5, 10]


def test_purging_everything_keeps_the_end_offset(tmp_path):
    log = new_log(tmp_path)
    log.append(messages(12), [100.0] * 12)

    assert log.purge(older_than_ts=200.0) == 12

    assert (log.start_offset, log.end_offset) == (12, 12)
    assert log.read(0) == (12, [], [])
    log.append(messages(1, start=12))
    assert (log.start_offset, log.end_offset) == (12, 13)
    reopened = new_log(tmp_path)
    assert (reopened.start_offset, reopened.end_offset) == (12, 13)


def test_purge_on_an_empty_or_missing_log_does_nothing(tmp_path):
    log = new_log(tmp_path)
    assert log.purge(time.time() + DAY) == 0
    assert not log.exists


def test_truncate_drops_the_tail_across_segments(tmp_path):
    log = new_log(tmp_path)
    log.append(messages(12))

    log.truncate_to(7)

    assert bases(log) == [0, 5]
    assert log.end_offset == 7
    assert log.read(0)[1] == messages(7)
    log.append([{"n": "new"}])
    assert log.read(7)[1] == [{"n": "new"}]


def test_reset_replaces_the_log_from_a_given_offset(tmp_path):
    log = new_log(tmp_path)
    log.append(messages(12))

    log.reset(40, messages(3, start=40), [1.0, 2.0, 3.0])

    assert (log.start_offset, log.end_offset) == (40, 43)
    assert bases(log) == [40]
    assert log.read(0) == (40, messages(3, start=40), [1.0, 2.0, 3.0])


def test_old_single_file_logs_are_converted(tmp_path):
    legacy = tmp_path / "partition_0.log"
    legacy.write_text("".join(json.dumps(m) + "\n" for m in messages(7)))

    log = PartitionLog(str(tmp_path / "partition_0"), 5, legacy_file=str(legacy))

    assert not legacy.exists()
    assert bases(log) == [0, 5]
    assert log.read(0)[1] == messages(7)


# --- in the cluster ----------------------------------------------------------------

@pytest.fixture
def small_segments(cluster, monkeypatch):
    """Three brokers that start a new segment every 5 messages."""
    monkeypatch.setenv("SEGMENT_MAX_MESSAGES", "5")
    return cluster.start("b1", "b2", "b3")


def write_many(cluster, topic, count, start=0):
    for message in messages(count, start):
        assert cluster.write(topic, 0, message).status_code == 200


def offsets(cluster, broker, topic="orders"):
    body = cluster.broker(broker).get(f"/offsets/{topic}/0").json()
    return body["start_offset"], body["end_offset"]


def run_retention(cluster, seconds_from_now):
    """Run each broker's retention check as if that much time had passed."""
    return sum(module.apply_retention(now=time.time() + seconds_from_now) for module in cluster.brokers.values())


def test_every_replica_has_the_same_segments_and_timestamps(small_segments):
    small_segments.create_topic("orders")
    write_many(small_segments, "orders", 12)

    for broker in ("b1", "b2", "b3"):
        assert small_segments.segments(broker, "orders") == [0, 5, 10]
    assert small_segments.records("b2", "orders") == small_segments.records("b1", "orders")


def test_write_reports_the_offset_of_the_message(small_segments):
    small_segments.create_topic("orders")
    write_many(small_segments, "orders", 3)
    assert small_segments.write("orders", 0, {"n": 3}).json()["offset"] == 3


def test_messages_older_than_the_default_retention_are_deleted(small_segments):
    small_segments.create_topic("orders")
    write_many(small_segments, "orders", 12)

    assert run_retention(small_segments, 29 * DAY) == 0          # default is 30 days
    assert run_retention(small_segments, 31 * DAY) == 36         # 12 messages on each of 3 brokers

    for broker in ("b1", "b2", "b3"):
        assert offsets(small_segments, broker) == (12, 12)
        assert small_segments.log(broker, "orders") == []


def test_default_retention_comes_from_the_settings_file(cluster, monkeypatch):
    monkeypatch.setenv("RETENTION_HOURS", "1")
    cluster.start("b1")
    cluster.create_topic("orders")
    write_many(cluster, "orders", 3)

    assert run_retention(cluster, 2 * 3600) == 3


def test_a_topic_can_have_its_own_retention(small_segments):
    small_segments.create_topic("orders")
    small_segments.create_topic("audit")
    write_many(small_segments, "orders", 6)
    write_many(small_segments, "audit", 6)
    small_segments.set_retention({"default_hours": 720, "topics": {"orders": 1}})

    run_retention(small_segments, 2 * 3600)

    assert offsets(small_segments, "b1", "orders") == (6, 6)
    assert offsets(small_segments, "b1", "audit") == (0, 6)


def test_retention_of_zero_keeps_messages_forever(small_segments):
    small_segments.create_topic("orders")
    write_many(small_segments, "orders", 6)
    small_segments.set_retention({"default_hours": 0})

    assert run_retention(small_segments, 3650 * DAY) == 0
    assert offsets(small_segments, "b1") == (0, 6)


def test_changing_the_setting_is_applied_at_once(small_segments):
    small_segments.create_topic("orders")
    write_many(small_segments, "orders", 12)
    cutoff = time.time()
    time.sleep(0.01)
    write_many(small_segments, "orders", 3, start=12)   # offsets 12..14 are newer than the cutoff

    small_segments.set_retention({"purge_before": {"orders": cutoff}})   # no retention run needed

    # segments 0-4 and 5-9 are gone; segment 10-14 holds newer messages and stays
    for broker in ("b1", "b2", "b3"):
        assert offsets(small_segments, broker) == (10, 15)
        assert small_segments.segments(broker, "orders") == [10]


def test_purge_now_from_the_console_empties_every_replica(small_segments):
    small_segments.create_topic("orders")
    write_many(small_segments, "orders", 12)

    response = small_segments.console.post("/api/purge/orders", json={"older_than_hours": 0})

    assert response.status_code == 200
    for broker in ("b1", "b2", "b3"):
        assert offsets(small_segments, broker) == (12, 12)


def test_purge_now_only_touches_the_named_topic(small_segments):
    small_segments.create_topic("orders")
    small_segments.create_topic("audit")
    write_many(small_segments, "orders", 6)
    write_many(small_segments, "audit", 6)

    small_segments.console.post("/api/purge/orders", json={"older_than_hours": 0})

    assert offsets(small_segments, "b1", "orders") == (6, 6)
    assert offsets(small_segments, "b1", "audit") == (0, 6)


def test_purge_now_with_an_age_keeps_recent_messages(small_segments):
    small_segments.create_topic("orders")
    write_many(small_segments, "orders", 12)

    small_segments.console.post("/api/purge/orders", json={"older_than_hours": 1})

    assert offsets(small_segments, "b1") == (0, 12)


def test_writes_continue_with_the_next_offset_after_a_purge(small_segments):
    small_segments.create_topic("orders")
    write_many(small_segments, "orders", 12)
    small_segments.console.post("/api/purge/orders", json={"older_than_hours": 0})

    response = small_segments.write("orders", 0, {"n": 12})

    assert response.json()["offset"] == 12
    assert response.json()["replicated_to"] == ["b2", "b3"]
    for broker in ("b1", "b2", "b3"):
        assert small_segments.log(broker, "orders") == [{"n": 12}]
        assert offsets(small_segments, broker) == (12, 13)


def test_subscriber_behind_a_purge_continues_with_what_is_left(small_segments):
    small_segments.create_topic("orders")
    consumer = small_segments.consumer
    consumer.post("/subscribe/ada@example.com/orders")
    for n in range(12):
        small_segments.producer.post("/write/orders", json={"key": "k", "value": n})
    small_segments.console.post("/api/purge/orders", json={"older_than_hours": 0})
    small_segments.producer.post("/write/orders", json={"key": "k", "value": "after"})

    body = consumer.get("/read/ada@example.com/orders/k").json()

    assert [m["value"] for m in body["messages"]] == ["after"]
    assert (body["start_offset"], body["end_offset"]) == (12, 13)


def test_follower_that_missed_purged_messages_restarts_from_the_leaders_oldest(small_segments):
    small_segments.create_topic("orders")
    small_segments.disconnect("b2")
    write_many(small_segments, "orders", 12)                     # b2 has none of these
    small_segments.console.post("/api/purge/orders", json={"older_than_hours": 0})
    small_segments.reconnect("b2")

    write_many(small_segments, "orders", 2, start=12)

    assert offsets(small_segments, "b2") == (12, 14)
    assert small_segments.log("b2", "orders") == messages(2, start=12)
    assert small_segments.partition("orders")["isr"] == ["b2", "b3"]


def test_broker_that_was_down_during_a_purge_matches_the_leader_when_it_returns(small_segments):
    small_segments.create_topic("orders")
    write_many(small_segments, "orders", 12)
    small_segments.kill("b3")
    small_segments.console.post("/api/purge/orders", json={"older_than_hours": 0})
    write_many(small_segments, "orders", 2, start=12)

    small_segments.start_broker("b3")

    assert offsets(small_segments, "b3") == offsets(small_segments, "b1") == (12, 14)
    assert small_segments.log("b3", "orders") == messages(2, start=12)


def test_a_restarted_broker_applies_the_retention_setting(small_segments):
    small_segments.create_topic("orders")
    write_many(small_segments, "orders", 12)
    small_segments.kill("b1")
    small_segments.kill("b2")
    small_segments.kill("b3")                                    # nobody is running
    small_segments.set_retention({"purge_before": {"orders": time.time() + 1}})

    small_segments.start_broker("b3")

    assert offsets(small_segments, "b3") == (12, 12)


# --- console settings API ---------------------------------------------------------

def test_settings_show_the_default_until_someone_changes_it(three_brokers):
    assert three_brokers.console.get("/api/settings").json() == {
        "read_only": False, "retention": {"default_hours": 720.0, "topics": {}},
    }


def test_default_retention_can_be_changed_from_the_console(three_brokers):
    response = three_brokers.console.put("/api/settings/retention", json={"default_hours": 48})

    assert response.status_code == 200
    assert three_brokers.console.get("/api/settings").json()["retention"]["default_hours"] == 48
    assert three_brokers.brokers["b2"]._retention["default_hours"] == 48     # brokers picked it up


def test_topic_retention_can_be_set_and_cleared(three_brokers):
    three_brokers.create_topic("orders")
    console = three_brokers.console

    console.put("/api/settings/retention/orders", json={"hours": 6})
    topic = console.get("/api/cluster").json()["topics"][0]
    assert (topic["retention_hours"], topic["retention_is_default"]) == (6, False)

    console.put("/api/settings/retention/orders", json={"hours": None})
    topic = console.get("/api/cluster").json()["topics"][0]
    assert (topic["retention_hours"], topic["retention_is_default"]) == (720, True)


def test_changing_one_setting_keeps_the_others(three_brokers):
    three_brokers.create_topic("orders")
    console = three_brokers.console
    console.put("/api/settings/retention/orders", json={"hours": 6})
    console.post("/api/purge/orders", json={"older_than_hours": 0})
    console.put("/api/settings/retention", json={"default_hours": 48})

    stored = json.loads(three_brokers.zk.get("/config/retention")[0])

    assert stored["default_hours"] == 48
    assert stored["topics"] == {"orders": 6}
    assert stored["purge_before"]["orders"] > 0


@pytest.mark.parametrize("method, path, body, status", [
    ("put", "/api/settings/retention", {"default_hours": -1}, 400),
    ("put", "/api/settings/retention", {"default_hours": 10 ** 9}, 400),
    ("put", "/api/settings/retention/missing", {"hours": 5}, 404),
    ("put", "/api/settings/retention/bad.name", {"hours": 5}, 400),
    ("post", "/api/purge/missing", {"older_than_hours": 0}, 404),
    ("post", "/api/purge/orders", {"older_than_hours": -3}, 400),
])
def test_invalid_settings_are_rejected(three_brokers, method, path, body, status):
    three_brokers.create_topic("orders")
    assert getattr(three_brokers.console, method)(path, json=body).status_code == status
    assert not three_brokers.zk.exists("/config/retention")


def test_read_only_console_refuses_changes(three_brokers, monkeypatch):
    three_brokers.create_topic("orders")
    console = three_brokers.console
    monkeypatch.setattr(three_brokers.console_module, "READ_ONLY", True)   # CONSOLE_READ_ONLY=1

    assert console.post("/api/purge/orders", json={"older_than_hours": 0}).status_code == 403
    assert console.put("/api/settings/retention", json={"default_hours": 1}).status_code == 403
    assert console.get("/api/settings").json()["read_only"] is True


def test_snapshot_reports_what_is_stored_after_a_purge(small_segments):
    small_segments.create_topic("orders")
    write_many(small_segments, "orders", 12)
    small_segments.console.post("/api/purge/orders", json={"older_than_hours": 0})
    write_many(small_segments, "orders", 2, start=12)

    partition = small_segments.console.get("/api/cluster").json()["topics"][0]["partitions"][0]

    assert (partition["start_offset"], partition["end_offset"], partition["stored"]) == (12, 14, 2)
    assert all(r["lag"] == 0 for r in partition["replicas"])


def test_console_message_viewer_works_after_a_purge(small_segments):
    small_segments.create_topic("orders")
    write_many(small_segments, "orders", 12)
    small_segments.console.post("/api/purge/orders", json={"older_than_hours": 0})
    write_many(small_segments, "orders", 2, start=12)

    body = small_segments.console.get("/api/messages/orders/0?limit=50").json()

    assert [m["offset"] for m in body["messages"]] == [12, 13]


# --- the in-memory copy of recent messages ----------------------------------------

def test_recent_messages_are_served_from_memory(tmp_path, monkeypatch):
    log = new_log(tmp_path)
    log.append(messages(12), [100.0 + n for n in range(12)])
    monkeypatch.setattr(PartitionLog, "_read_segment", staticmethod(lambda segment: pytest.fail("read the file")))

    assert log.read(7) == (7, messages(5, start=7), [107.0 + n for n in range(5)])
    assert log.read(3, limit=4) == (3, messages(4, start=3), [103.0 + n for n in range(4)])
    assert log.read(12) == (12, [], [])


def test_memory_and_disk_agree_after_every_kind_of_change(tmp_path):
    log = PartitionLog(str(tmp_path / "partition_0"), 5, recent_messages=8)
    log.append(messages(12), [100.0 + n for n in range(12)])     # memory now holds offsets 4..11
    log.truncate_to(10)
    log.append(messages(3, start=50))
    log.purge(older_than_ts=105.0)

    reopened = PartitionLog(str(tmp_path / "partition_0"), 5)     # reads everything from disk
    for offset in range(0, 14):
        assert log.read(offset)[:2] == reopened.read(offset)[:2], offset
        assert log.read(offset, limit=2)[:2] == reopened.read(offset, limit=2)[:2], offset

    log.reset(40, messages(2, start=40))
    assert log.read(0)[:2] == (40, messages(2, start=40))
