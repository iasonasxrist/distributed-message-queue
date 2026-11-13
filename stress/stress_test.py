#!/usr/bin/env python3
"""
Stress test for a running pub-sub cluster.

It talks only to the public APIs (producer, consumer, console), so it works
against `docker compose up` or any other deployment. Three phases:

1. Throughput and latency: writes as fast as possible at increasing numbers
   of concurrent clients, to find where throughput stops growing.
2. Reads: how long a new subscriber needs to replay a partition, and how
   long a caught-up poll takes.
3. Fault tolerance: under steady load, take a broker down, measure how long the
   cluster needs to react and what the clients notice, bring the broker back,
   then check that no acknowledged message was lost or duplicated. Scenarios:
   the leader or a follower, crashing or shut down normally.

    pip install requests
    python stress/stress_test.py                      # against docker compose
    python stress/stress_test.py --skip-fault         # load only, nothing is killed
    python stress/stress_test.py --user admin --password <yours>
    python stress/stress_test.py --skip-load --scenarios all   # the four shutdown scenarios

Phase 3 uses `docker kill`, `docker stop` and `docker start` on the broker id
(broker ids are the container hostnames). Use --kill-cmd, --stop-cmd and
--start-cmd for other setups.
"""
import argparse
import hashlib
import json
import random
import shlex
import statistics
import subprocess
import sys
import threading
import time

import requests

CLIENT_TIMEOUT = 20  # seconds a single request may take before it counts as failed
HTTP = requests.Session()  # carries the sign-in for every call that is not part of the load


# --- helpers -------------------------------------------------------------------

def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(pct / 100 * (len(ordered) - 1))))
    return ordered[index]


def latency_stats(latencies):
    """Latency summary in milliseconds."""
    if not latencies:
        return {"p50": None, "p95": None, "p99": None, "max": None, "mean": None}
    ms = [x * 1000 for x in latencies]
    return {
        "p50": round(percentile(ms, 50), 1),
        "p95": round(percentile(ms, 95), 1),
        "p99": round(percentile(ms, 99), 1),
        "max": round(max(ms), 1),
        "mean": round(statistics.fmean(ms), 1),
    }


def partition_of(key, partitions):
    """Same partitioner as the producer and the consumer."""
    return int(hashlib.sha256(str(key).encode()).hexdigest(), 16) % partitions


def key_for_partition(partition, partitions):
    n = 0
    while partition_of(f"key-{n}", partitions) != partition:
        n += 1
    return f"key-{n}"


def plural(count, word):
    return f"{count} {word}" if count == 1 else f"{count} {word}s"


def fmt(value, unit=""):
    return "-" if value is None else f"{value}{unit}"


def table(headers, rows):
    widths = [max(len(str(c)) for c in col) for col in zip(headers, *rows)]
    line = "  ".join(str(h).ljust(w) for h, w in zip(headers, widths))
    out = [line, "  ".join("-" * w for w in widths)]
    out += ["  ".join(str(c).ljust(w) for c, w in zip(row, widths)) for row in rows]
    return "\n".join(out)


class Cluster:
    def __init__(self, args):
        self.producer = args.producer.rstrip("/")
        self.consumer = args.consumer.rstrip("/")
        self.console = args.console.rstrip("/")
        self.acks = args.acks

    def snapshot(self):
        response = HTTP.get(f"{self.console}/api/cluster", timeout=CLIENT_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def topic(self, snapshot, name):
        return next((t for t in snapshot["topics"] if t["name"] == name), None)

    def create_topic(self, name, partitions):
        response = HTTP.post(f"{self.producer}/create_topic/{name}/{partitions}", timeout=CLIENT_TIMEOUT,
                             params={"acks": self.acks} if self.acks else None)
        if response.status_code != 200:
            sys.exit(f"Could not create topic {name}: HTTP {response.status_code} {response.text}")

    def delete_topic(self, name):
        try:
            HTTP.delete(f"{self.producer}/delete_topic/{name}", timeout=CLIENT_TIMEOUT)
        except requests.RequestException:
            pass

    def read_partition(self, topic, partition, partitions, user):
        """Everything a new subscriber gets from one partition, with the time it took."""
        key = key_for_partition(partition, partitions)
        HTTP.post(f"{self.consumer}/subscribe/{user}/{topic}", timeout=CLIENT_TIMEOUT)
        started = time.perf_counter()
        response = HTTP.get(f"{self.consumer}/read/{user}/{topic}/{key}", timeout=120)
        elapsed = time.perf_counter() - started
        response.raise_for_status()
        return response.json()["messages"], elapsed


# --- load generation -----------------------------------------------------------

class Load:
    """Worker threads that publish messages and record every attempt."""

    def __init__(self, cluster, topic, partitions, workers, payload_bytes, rate=None, tag="m"):
        self.url = f"{cluster.producer}/write/{topic}"
        self.partitions = partitions
        self.workers = workers
        self.padding = "x" * payload_bytes
        self.interval = workers / rate if rate else 0   # seconds between sends per worker
        self.tag = tag
        self.records = []      # (start, end, ok, status, partition, message id)
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.threads = []
        self.keys = [f"key-{n}" for n in range(64)]

    def _worker(self, number):
        session = requests.Session()
        session.auth = HTTP.auth
        rng = random.Random(number)
        local, sequence = [], 0
        next_send = time.perf_counter()
        while not self.stop_event.is_set():
            if self.interval:
                delay = next_send - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
                next_send = max(next_send + self.interval, time.perf_counter())
            key = rng.choice(self.keys)
            message_id = f"{self.tag}-{number}-{sequence}"
            sequence += 1
            started = time.time()
            try:
                response = session.post(
                    self.url, json={"key": key, "id": message_id, "pad": self.padding}, timeout=CLIENT_TIMEOUT
                )
                status = response.status_code
            except requests.RequestException:
                status = 0
            local.append((started, time.time(), status == 200, status, partition_of(key, self.partitions), message_id))
        with self.lock:
            self.records += local

    def start(self):
        self.threads = [threading.Thread(target=self._worker, args=(n,), daemon=True) for n in range(self.workers)]
        for thread in self.threads:
            thread.start()
        return self

    def stop(self):
        self.stop_event.set()
        for thread in self.threads:
            thread.join(CLIENT_TIMEOUT + 5)
        return self.records


# --- phase 1: throughput and latency --------------------------------------------

def phase_throughput(cluster, args, report):
    print("\n== Phase 1: throughput and write latency ==")
    topic = f"{args.prefix}-load"
    cluster.create_topic(topic, args.partitions)
    rows, results = [], []
    for level in args.levels:
        load = Load(cluster, topic, args.partitions, level, args.payload_bytes, tag=f"l{level}").start()
        time.sleep(args.duration)
        records = load.stop()
        ok = [r for r in records if r[2]]
        span = (max(r[1] for r in records) - min(r[0] for r in records)) if records else args.duration
        stats = latency_stats([r[1] - r[0] for r in ok])
        result = {
            "clients": level,
            "throughput_per_s": round(len(ok) / span, 1) if span else 0,
            "acknowledged": len(ok),
            "failed": len(records) - len(ok),
            "latency_ms": stats,
        }
        results.append(result)
        rows.append([level, result["throughput_per_s"], result["acknowledged"], result["failed"],
                     fmt(stats["p50"]), fmt(stats["p95"]), fmt(stats["p99"]), fmt(stats["max"])])
        print(f"  {level:>3} clients: {result['throughput_per_s']:>8} msg/s, p50 {fmt(stats['p50'])} ms, p99 {fmt(stats['p99'])} ms")
    print()
    print(table(["clients", "msg/s", "acked", "failed", "p50 ms", "p95 ms", "p99 ms", "max ms"], rows))
    best = max(results, key=lambda r: r["throughput_per_s"])
    print(f"\nPeak: {best['throughput_per_s']} msg/s with {best['clients']} concurrent clients.")
    report["throughput"] = {"topic": topic, "partitions": args.partitions, "payload_bytes": args.payload_bytes,
                            "levels": results, "peak": best}
    return topic


# --- phase 2: reads -------------------------------------------------------------

def phase_reads(cluster, args, report, topic):
    print("\n== Phase 2: reads ==")
    rows, results = [], []
    for partition in range(args.partitions):
        user = f"stress-{args.prefix}-replay-{partition}"
        messages, elapsed = cluster.read_partition(topic, partition, args.partitions, user)
        key = key_for_partition(partition, args.partitions)
        polls = []
        for _ in range(args.polls):
            started = time.perf_counter()
            HTTP.get(f"{cluster.consumer}/read/{user}/{topic}/{key}", timeout=120)
            polls.append(time.perf_counter() - started)
        stats = latency_stats(polls)
        result = {
            "partition": partition,
            "messages": len(messages),
            "replay_seconds": round(elapsed, 3),
            "replay_msgs_per_s": round(len(messages) / elapsed, 1) if elapsed else None,
            "caught_up_poll_ms": stats,
        }
        results.append(result)
        rows.append([partition, len(messages), result["replay_seconds"], fmt(result["replay_msgs_per_s"]),
                     fmt(stats["p50"]), fmt(stats["p95"])])
    print(table(["partition", "messages", "replay s", "replay msg/s", "poll p50 ms", "poll p95 ms"], rows))
    report["reads"] = results


# --- phase 3: fault tolerance ---------------------------------------------------

def run_command(template, broker):
    command = template.format(broker=broker)
    result = subprocess.run(shlex.split(command), capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"Command failed: {command}\n{result.stderr.strip()}")


def wait_for(condition, timeout, interval=0.2):
    """Poll until condition() is truthy; returns (value, seconds waited) or (None, None)."""
    started = time.time()
    while time.time() - started < timeout:
        try:
            value = condition()
        except requests.RequestException:
            value = None
        if value:
            return value, time.time() - started
        time.sleep(interval)
    return None, None


SCENARIOS = {
    # name: (which broker, how it goes down, what it means)
    "kill-leader": ("leader", "kill", "the leader crashes"),
    "stop-leader": ("leader", "stop", "the leader is shut down normally"),
    "kill-follower": ("follower", "kill", "a follower crashes"),
    "stop-follower": ("follower", "stop", "a follower is shut down normally"),
}


def phase_fault(cluster, args, report, scenario):
    role, how, meaning = SCENARIOS[scenario]
    print(f"\n== Phase 3: fault tolerance, {scenario} ({meaning}) ==")
    topic = f"{args.prefix}-{scenario}"
    # A follower scenario uses one partition, so the broker taken down leads nothing
    partitions = args.partitions if role == "leader" else 1
    cluster.create_topic(topic, partitions)
    before = cluster.topic(cluster.snapshot(), topic)
    first = before["partitions"][0]
    if role == "leader":
        victim = first["leader"]
    else:
        victim = next(r["broker"] for r in first["replicas"] if r["role"] == "follower")
    led_by_victim = [int(p["id"]) for p in before["partitions"] if p["leader"] == victim]
    print(f"  Steady load: {args.fault_rate} msg/s from {args.fault_workers} clients on {plural(partitions, 'partition')}.")
    print(f"  Broker taken down: {victim} ({'leads partition ' + ', '.join(map(str, led_by_victim)) if led_by_victim else 'a follower'}).")

    load = Load(cluster, topic, partitions, args.fault_workers, args.payload_bytes, rate=args.fault_rate, tag="f").start()
    time.sleep(args.warmup)

    # --- take the broker down
    kill_time = time.time()
    run_command(args.kill_cmd if how == "kill" else args.stop_cmd, victim)
    print(f"  t+0.0s   {'killed' if how == 'kill' else 'stopped'} {victim}")

    def cluster_reacted():
        """Every partition has a live leader and no longer counts the broker as a replica."""
        topic_state = cluster.topic(cluster.snapshot(), topic)
        return all(
            p["leader"] not in (None, victim) and victim not in [r["broker"] for r in p["replicas"]]
            for p in topic_state["partitions"]
        )

    _, election_seconds = wait_for(cluster_reacted, args.failover_timeout)
    if election_seconds is None:
        print(f"  !! the cluster did not react within {args.failover_timeout}s")
    else:
        reaction = "every partition has a live leader again" if role == "leader" else "the follower was removed from the replica set"
        print(f"  t+{election_seconds:.1f}s  {reaction}")
    time.sleep(args.after_kill)

    # --- bring it back
    restart_time = time.time()
    run_command(args.start_cmd, victim)
    print(f"  t+{restart_time - kill_time:.1f}s  restarted {victim}")

    def fully_replicated():
        topic_state = cluster.topic(cluster.snapshot(), topic)
        return all(
            p["status"] == "healthy" and all(r["in_sync"] and r["lag"] == 0 for r in p["replicas"])
            for p in topic_state["partitions"]
        )

    _, recovery_seconds = wait_for(fully_replicated, args.recovery_timeout, interval=0.5)
    if recovery_seconds is None:
        print(f"  !! not fully replicated again within {args.recovery_timeout}s")
    else:
        print(f"  t+{time.time() - kill_time:.1f}s  {victim} is back in sync on every partition")
    time.sleep(2)
    records = load.stop()
    time.sleep(1)

    # --- what the clients saw
    acked = [r for r in records if r[2]]
    failed = [r for r in records if not r[2]]
    steady = [r[1] - r[0] for r in acked if r[1] < kill_time]
    during = [r[1] - r[0] for r in acked if kill_time <= r[0] < kill_time + (election_seconds or args.failover_timeout)]
    after = [r[1] - r[0] for r in acked if r[0] >= restart_time]
    outages = {}
    for partition in range(partitions):
        failures = [r for r in failed if r[4] == partition and r[0] >= kill_time - 1]
        if failures:
            first_failure = min(r[0] for r in failures)
            later_ok = [r[1] for r in acked if r[4] == partition and r[0] > max(f[0] for f in failures)]
            outages[partition] = {
                "failed_writes": len(failures),
                "seconds": round((min(later_ok) if later_ok else time.time()) - first_failure, 1),
                "recovered": bool(later_ok),
            }

    # --- durability: is every acknowledged message still there, exactly once?
    lost, duplicated, stored_total, unacked_stored = [], 0, 0, 0
    acked_ids = {r[5] for r in acked}
    for partition in range(partitions):
        messages, _ = cluster.read_partition(topic, partition, partitions, f"stress-{args.prefix}-{scenario}-verify-{partition}")
        ids = [m.get("id") for m in messages]
        stored_total += len(ids)
        duplicated += len(ids) - len(set(ids))
        unacked_stored += len(set(ids) - acked_ids)
        expected = {r[5] for r in acked if r[4] == partition}
        lost += sorted(expected - set(ids))
    final = cluster.topic(cluster.snapshot(), topic)
    replicas_equal = all(
        len({r["end_offset"] for r in p["replicas"]}) == 1 and len(p["replicas"]) == p["replica_target"]
        for p in final["partitions"]
    )

    result = {
        "scenario": scenario,
        "meaning": meaning,
        "topic": topic,
        "broker_taken_down": victim,
        "partitions_it_led": led_by_victim,
        "load": {"clients": args.fault_workers, "target_msgs_per_s": args.fault_rate},
        "attempted": len(records),
        "acknowledged": len(acked),
        "failed_writes": len(failed),
        "failed_by_status": {str(s): sum(1 for r in failed if r[3] == s) for s in sorted({r[3] for r in failed})},
        "reaction_seconds": round(election_seconds, 1) if election_seconds is not None else None,
        "write_outage_by_partition": outages,
        "rejoin_seconds_after_restart": round(recovery_seconds, 1) if recovery_seconds is not None else None,
        # with producer retries a failover shows up as slow writes rather than failed ones
        "longest_write_while_down_s": round(max(
            (r[1] - r[0] for r in records if kill_time - 1 <= r[0] < restart_time), default=0), 1),
        "latency_ms": {"before": latency_stats(steady), "while_reacting": latency_stats(during),
                       "after_restart": latency_stats(after)},
        "durability": {
            "acknowledged": len(acked_ids),
            "stored": stored_total,
            "lost_acknowledged": len(lost),
            "lost_examples": lost[:10],
            "duplicates": duplicated,
            "stored_but_never_acknowledged": unacked_stored,
            "all_replicas_same_length": replicas_equal,
        },
    }
    passed = not lost and not duplicated and replicas_equal and election_seconds is not None and recovery_seconds is not None
    result["passed"] = passed
    report.setdefault("fault_tolerance", {})[scenario] = result

    print()
    print(table(["metric", "value"], [
        ["writes attempted", len(records)],
        ["writes acknowledged", len(acked)],
        ["writes failed", f"{len(failed)} {result['failed_by_status'] or ''}"],
        ["time for the cluster to react", fmt(result["reaction_seconds"], " s")],
        ["longest write while the broker was down", fmt(result["longest_write_while_down_s"], " s")],
        ["time to rejoin after restart", fmt(result["rejoin_seconds_after_restart"], " s")],
        ["p99 before / while reacting", f"{fmt(result['latency_ms']['before']['p99'])} / {fmt(result['latency_ms']['while_reacting']['p99'])} ms"],
        ["acknowledged messages lost", len(lost)],
        ["duplicated messages", duplicated],
        ["stored but never acknowledged", unacked_stored],
        ["all replicas the same length", "yes" if replicas_equal else "NO"],
    ]))
    print(f"\n{scenario}:", "PASS" if passed else "FAIL")
    return topic, passed


def fault_summary(report):
    results = report.get("fault_tolerance", {})
    if len(results) < 2:
        return
    print("\n== Fault tolerance summary ==")
    print(table(
        ["scenario", "reaction", "failed writes", "longest write", "rejoin", "lost", "duplicated", "result"],
        [[name, fmt(r["reaction_seconds"], " s"), r["failed_writes"], fmt(r["longest_write_while_down_s"], " s"),
          fmt(r["rejoin_seconds_after_restart"], " s"), r["durability"]["lost_acknowledged"],
          r["durability"]["duplicates"], "PASS" if r["passed"] else "FAIL"] for name, r in results.items()],
    ))


# --- main ------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--producer", default="http://localhost:4000")
    parser.add_argument("--consumer", default="http://localhost:5000")
    parser.add_argument("--console", default="http://localhost:8080")
    parser.add_argument("--user", default="admin", help="user to sign in as (needs the admin role)")
    parser.add_argument("--password", default="admin")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--acks", choices=["all", "majority", "leader"],
                        help="when writes to the test topics are confirmed (default: the cluster's setting)")
    parser.add_argument("--levels", default="1,2,4,8,16,32", help="concurrent clients to try, in order")
    parser.add_argument("--duration", type=float, default=10, help="seconds per level")
    parser.add_argument("--payload-bytes", type=int, default=100)
    parser.add_argument("--polls", type=int, default=20, help="caught-up polls to time per partition")
    parser.add_argument("--fault-workers", type=int, default=8)
    parser.add_argument("--fault-rate", type=float, default=100, help="messages per second during the fault test")
    parser.add_argument("--warmup", type=float, default=5, help="seconds of load before the broker is killed")
    parser.add_argument("--after-kill", type=float, default=10, help="seconds of load after the election")
    parser.add_argument("--failover-timeout", type=float, default=60)
    parser.add_argument("--recovery-timeout", type=float, default=90)
    parser.add_argument("--scenarios", default="kill-leader",
                        help="comma-separated: " + ", ".join(SCENARIOS) + ", or 'all'")
    parser.add_argument("--kill-cmd", default="docker kill {broker}", help="how to crash a broker")
    parser.add_argument("--stop-cmd", default="docker stop {broker}", help="how to shut a broker down normally")
    parser.add_argument("--start-cmd", default="docker start {broker}")
    parser.add_argument("--skip-fault", action="store_true", help="do not kill anything")
    parser.add_argument("--skip-load", action="store_true", help="only run the fault tolerance phase")
    parser.add_argument("--keep-topics", action="store_true", help="leave the test topics in place")
    parser.add_argument("--out", default="stress-results.json")
    args = parser.parse_args()
    args.levels = [int(x) for x in args.levels.split(",") if x]
    args.scenarios = list(SCENARIOS) if args.scenarios == "all" else [x for x in args.scenarios.split(",") if x]
    unknown = [x for x in args.scenarios if x not in SCENARIOS]
    if unknown:
        parser.error(f"unknown scenario: {', '.join(unknown)}")
    args.prefix = f"stress-{int(time.time())}"

    HTTP.auth = (args.user, args.password)
    cluster = Cluster(args)
    try:
        snapshot = cluster.snapshot()
    except requests.HTTPError as error:
        if error.response.status_code == 401:
            sys.exit(f"Sign-in failed for user {args.user}. Pass --user and --password.")
        sys.exit(f"The console at {cluster.console} answered {error.response.status_code}")
    except requests.RequestException as error:
        sys.exit(f"Cannot reach the console at {cluster.console}: {error}")
    alive = [b["id"] for b in snapshot["brokers"] if b["alive"]]
    print(f"Cluster: {len(alive)} brokers running ({', '.join(alive)}).")
    if not alive:
        sys.exit("No brokers are running.")

    report = {"started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "brokers": alive, "settings": {
        k: v for k, v in vars(args).items() if k not in ("prefix", "password")}}
    topics, passed = [], True
    if not args.skip_load:
        topic = phase_throughput(cluster, args, report)
        topics.append(topic)
        phase_reads(cluster, args, report, topic)
    if not args.skip_fault:
        if len(alive) < 2:
            print("\nSkipping the fault tolerance phase: it needs at least two brokers.")
        else:
            for scenario in args.scenarios:
                topic, ok = phase_fault(cluster, args, report, scenario)
                topics.append(topic)
                passed = passed and ok
            fault_summary(report)
    if not args.keep_topics:
        for topic in topics:
            cluster.delete_topic(topic)

    with open(args.out, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nFull results written to {args.out}")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
