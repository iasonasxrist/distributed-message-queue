"""The console's JSON API (what the page at http://localhost:8080 is drawn from)."""


def snapshot(cluster):
    response = cluster.console.get("/api/cluster")
    assert response.status_code == 200, response.text
    return response.json()


def partition(snap, topic="orders", partition_id="0"):
    topic_data = next(t for t in snap["topics"] if t["name"] == topic)
    return next(p for p in topic_data["partitions"] if p["id"] == partition_id)


def replica(part, broker):
    return next((r for r in part["replicas"] if r["broker"] == broker), None)


def test_empty_cluster(three_brokers):
    snap = snapshot(three_brokers)
    assert snap["topics"] == []
    assert [b["id"] for b in snap["brokers"]] == ["b1", "b2", "b3"]
    assert snap["summary"] == {
        "brokers_alive": 3, "brokers_known": 3, "topics": 0,
        "partitions": 0, "offline": 0, "under_replicated": 0,
    }


def test_healthy_topic_shows_leader_followers_and_offsets(three_brokers):
    three_brokers.create_topic("orders", 3)
    for n in range(4):
        three_brokers.write("orders", 0, {"n": n})

    snap = snapshot(three_brokers)
    part = partition(snap)

    assert part["leader"] == "b1"
    assert part["status"] == "healthy"
    assert part["end_offset"] == 4
    assert [(r["broker"], r["role"], r["in_sync"], r["end_offset"], r["lag"]) for r in part["replicas"]] == [
        ("b1", "leader", True, 4, 0),
        ("b2", "follower", True, 4, 0),
        ("b3", "follower", True, 4, 0),
    ]
    assert [(b["id"], b["leader_of"], b["follower_of"]) for b in snap["brokers"]] == [
        ("b1", 1, 2), ("b2", 1, 2), ("b3", 1, 2),
    ]
    assert snap["summary"]["partitions"] == 3
    assert snap["summary"]["under_replicated"] == 0


def test_lagging_follower_is_flagged_with_its_lag(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.write("orders", 0, {"n": 0})
    three_brokers.disconnect("b3")
    three_brokers.write("orders", 0, {"n": 1})
    three_brokers.write("orders", 0, {"n": 2})
    three_brokers.reconnect("b3")

    part = partition(snapshot(three_brokers))

    assert part["status"] == "under_replicated"
    assert replica(part, "b3")["in_sync"] is False
    assert replica(part, "b3")["end_offset"] == 1
    assert replica(part, "b3")["lag"] == 2
    assert replica(part, "b2")["lag"] == 0


def test_unreachable_replica_has_unknown_offset(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.disconnect("b2")

    part = partition(snapshot(three_brokers))

    assert replica(part, "b2")["end_offset"] is None
    assert replica(part, "b2")["lag"] is None


def test_leader_failover_is_visible(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.kill("b1")

    snap = snapshot(three_brokers)
    part = partition(snap)

    assert part["leader"] == "b2"
    assert part["status"] == "under_replicated"
    assert replica(part, "b1") is None          # no longer a replica ...
    assert "b1" in part["assigned"]             # ... but still expected back
    assert {b["id"]: b["alive"] for b in snap["brokers"]} == {"b1": False, "b2": True, "b3": True}
    assert snap["summary"]["brokers_alive"] == 2
    assert snap["summary"]["brokers_known"] == 3


def test_returning_broker_makes_the_partition_healthy_again(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.kill("b1")
    three_brokers.start_broker("b1")

    part = partition(snapshot(three_brokers))

    assert part["status"] == "healthy"
    assert replica(part, "b1")["role"] == "follower"


def test_offline_partition_is_reported(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.disconnect("b2")
    three_brokers.disconnect("b3")
    three_brokers.write("orders", 0, {"n": 0})
    three_brokers.kill("b1")

    snap = snapshot(three_brokers)

    assert partition(snap)["status"] == "offline"
    assert partition(snap)["leader"] is None
    assert snap["summary"]["offline"] == 1


def test_messages_shows_the_tail_of_a_replica(three_brokers):
    three_brokers.create_topic("orders")
    for n in range(30):
        three_brokers.write("orders", 0, {"n": n})

    body = three_brokers.console.get("/api/messages/orders/0?broker=b3&limit=5").json()

    assert body["broker"] == "b3"
    assert body["end_offset"] == 30
    assert [m["offset"] for m in body["messages"]] == [25, 26, 27, 28, 29]
    assert body["messages"][-1]["value"] == {"n": 29}


def test_messages_defaults_to_the_leader_and_handles_an_empty_partition(three_brokers):
    three_brokers.create_topic("orders")
    body = three_brokers.console.get("/api/messages/orders/0").json()
    assert body == {"topic": "orders", "partition": 0, "broker": "b1", "end_offset": 0, "messages": []}


def test_messages_only_calls_replicas_of_the_partition(three_brokers):
    three_brokers.create_topic("orders")
    console = three_brokers.console
    three_brokers.requests_log.clear()

    assert console.get("/api/messages/orders/0?broker=evil.example.com").status_code == 404
    assert console.get("/api/messages/missing/0").status_code == 404
    assert console.get("/api/messages/orders/7").status_code == 404
    assert console.get("/api/messages/bad.name/0").status_code == 400
    assert three_brokers.requests_log == []


def test_messages_from_a_stopped_or_silent_broker_is_503(three_brokers):
    three_brokers.create_topic("orders")
    three_brokers.disconnect("b2")
    assert three_brokers.console.get("/api/messages/orders/0?broker=b2").status_code == 503


def test_page_and_health_check_are_served(three_brokers):
    page = three_brokers.console.get("/")
    assert page.status_code == 200
    assert "Pub-Sub console" in page.text
    assert three_brokers.console.get("/healthz").json() == {"status": "ok"}
