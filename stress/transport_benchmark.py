#!/usr/bin/env python3
"""
Transport benchmark: what does one call between two services cost in Python?

This is an experiment, not part of the system. It does not touch the cluster.
It starts a tiny server for each mechanism in its own process, sends it the
same small message many times from one client, and reports the cost per call.
Every server does the same work: decode the message(s), count them, acknowledge.

Mechanisms compared
    http-current   what the services use today: FastAPI endpoint + requests
    http-light     the same over HTTP, trimmed: async endpoint + urllib3
    grpc-unary     gRPC, one call per message
    grpc-stream    gRPC, one long-lived stream, one acknowledgement per message
    tcp            a persistent TCP connection with length-prefixed JSON
    zeromq         ZeroMQ request/reply

Each is measured twice: one message per call, and 100 messages per call
(batching), because batching changes the picture more than the transport does.

    pip install fastapi uvicorn requests grpcio pyzmq
    python stress/transport_benchmark.py
    python stress/transport_benchmark.py --only tcp,grpc-stream --calls 5000

Mechanisms whose library is not installed are skipped.
"""
import argparse
import asyncio
import json
import socket
import struct
import subprocess
import sys
import time

MESSAGE = {"key": "key-17", "id": "m-0-12345", "pad": "x" * 100}
BATCH = 100


def encode(messages):
    return json.dumps({"messages": messages}).encode("utf-8")


def handle(payload: bytes) -> bytes:
    """The work every server does: decode, count, acknowledge."""
    messages = json.loads(payload)["messages"]
    return json.dumps({"stored": len(messages)}).encode("utf-8")


# --- servers (each runs in its own process: `--serve <name> --port <port>`) --------------

def serve_http(port, light):
    import uvicorn
    from fastapi import Body, FastAPI, Request, Response

    app = FastAPI()
    if light:
        @app.post("/write")
        async def write(request: Request):              # no worker thread, no validation model
            return Response(handle(await request.body()), media_type="application/json")
    else:
        @app.post("/write")
        def write(messages: list = Body(..., embed=True)):   # as the broker's endpoints are written
            return {"stored": len(messages)}

    uvicorn.run(app, host="127.0.0.1", port=port, log_level="error", access_log=False)


def serve_grpc(port):
    from concurrent import futures

    import grpc

    def unary(request, context):
        return handle(request)

    def stream(requests, context):
        for request in requests:
            yield handle(request)

    handler = grpc.method_handlers_generic_handler("bench.Log", {
        "Write": grpc.unary_unary_rpc_method_handler(unary),
        "Stream": grpc.stream_stream_rpc_method_handler(stream),
    })
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    server.add_generic_rpc_handlers((handler,))
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    server.wait_for_termination()


def serve_tcp(port):
    async def connection(reader, writer):
        try:
            while True:
                size = struct.unpack("!I", await reader.readexactly(4))[0]
                reply = handle(await reader.readexactly(size))
                writer.write(struct.pack("!I", len(reply)) + reply)
                await writer.drain()
        except asyncio.IncompleteReadError:
            writer.close()

    async def main():
        server = await asyncio.start_server(connection, "127.0.0.1", port)
        async with server:
            await server.serve_forever()

    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass
    asyncio.run(main())


def serve_zeromq(port):
    import zmq

    sock = zmq.Context().socket(zmq.REP)
    sock.bind(f"tcp://127.0.0.1:{port}")
    while True:
        sock.send(handle(sock.recv()))


SERVERS = {
    "http-current": lambda port: serve_http(port, light=False),
    "http-light": lambda port: serve_http(port, light=True),
    "grpc": serve_grpc,
    "tcp": serve_tcp,
    "zeromq": serve_zeromq,
}


# --- clients: each returns a function that sends one payload and waits for the reply --------

def client_http_current(port):
    import requests

    session = requests.Session()
    url = f"http://127.0.0.1:{port}/write"

    def call(messages):
        response = session.post(url, json={"messages": messages}, timeout=10)
        assert response.status_code == 200
    return call


def client_http_light(port):
    import urllib3

    pool = urllib3.HTTPConnectionPool("127.0.0.1", port, maxsize=1)

    def call(messages):
        response = pool.request("POST", "/write", body=encode(messages),
                                headers={"Content-Type": "application/json"})
        assert response.status == 200
    return call


def client_grpc_unary(port):
    import grpc

    channel = grpc.insecure_channel(f"127.0.0.1:{port}", options=[("grpc.enable_http_proxy", 0)])
    write = channel.unary_unary("/bench.Log/Write")

    def call(messages):
        write(encode(messages))
    return call


def client_grpc_stream(port):
    import queue

    import grpc

    channel = grpc.insecure_channel(f"127.0.0.1:{port}", options=[("grpc.enable_http_proxy", 0)])
    outgoing = queue.Queue()
    replies = channel.stream_stream("/bench.Log/Stream")(iter(outgoing.get, None))

    def call(messages):
        outgoing.put(encode(messages))
        next(replies)                      # wait for this message's acknowledgement
    return call


def client_tcp(port):
    sock = socket.create_connection(("127.0.0.1", port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def read(size):
        data = b""
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                raise ConnectionError("server closed the connection")
            data += chunk
        return data

    def call(messages):
        payload = encode(messages)
        sock.sendall(struct.pack("!I", len(payload)) + payload)
        read(struct.unpack("!I", read(4))[0])
    return call


def client_zeromq(port):
    import zmq

    sock = zmq.Context().socket(zmq.REQ)
    sock.connect(f"tcp://127.0.0.1:{port}")

    def call(messages):
        sock.send(encode(messages))
        sock.recv()
    return call


# name: (server to start, client factory, library that must be importable, description)
MECHANISMS = {
    "http-current": ("http-current", client_http_current, ["fastapi", "uvicorn", "requests"], "FastAPI + requests (today)"),
    "http-light": ("http-light", client_http_light, ["fastapi", "uvicorn", "urllib3"], "async endpoint + urllib3"),
    "grpc-unary": ("grpc", client_grpc_unary, ["grpc"], "gRPC, one call per message"),
    "grpc-stream": ("grpc", client_grpc_stream, ["grpc"], "gRPC, one open stream"),
    "tcp": ("tcp", client_tcp, [], "persistent TCP, own framing"),
    "zeromq": ("zeromq", client_zeromq, ["zmq"], "ZeroMQ request/reply"),
}


# --- measuring ---------------------------------------------------------------------------

def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(port, process, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError("the server process exited")
        try:
            socket.create_connection(("127.0.0.1", port), 0.5).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("the server did not start")


def percentile(values, pct):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(pct / 100 * (len(ordered) - 1)))]


def measure(call, messages, calls, warmup):
    for _ in range(warmup):
        call(messages)
    timings = []
    started = time.perf_counter()
    for _ in range(calls):
        t0 = time.perf_counter()
        call(messages)
        timings.append(time.perf_counter() - t0)
    elapsed = time.perf_counter() - started
    return {
        "p50_ms": round(percentile(timings, 50) * 1000, 3),
        "p99_ms": round(percentile(timings, 99) * 1000, 3),
        "calls_per_s": round(calls / elapsed),
        "messages_per_s": round(calls * len(messages) / elapsed),
    }


def run(name, calls, warmup):
    server_name, make_client, libraries, _ = MECHANISMS[name]
    for library in libraries:
        try:
            __import__(library)
        except ImportError:
            return {"skipped": f"{library} is not installed"}
    port = free_port()
    process = subprocess.Popen([sys.executable, __file__, "--serve", server_name, "--port", str(port)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_for_port(port, process)
        call = make_client(port)
        return {
            "single": measure(call, [MESSAGE], calls, warmup),
            "batched": measure(call, [MESSAGE] * BATCH, max(calls // 20, 50), max(warmup // 20, 5)),
        }
    finally:
        process.terminate()
        process.wait(5)


def table(headers, rows):
    widths = [max(len(str(c)) for c in col) for col in zip(headers, *rows)]
    lines = ["  ".join(str(h).ljust(w) for h, w in zip(headers, widths)), "  ".join("-" * w for w in widths)]
    lines += ["  ".join(str(c).ljust(w) for c, w in zip(row, widths)) for row in rows]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", default="", help="comma-separated mechanisms (default: all)")
    parser.add_argument("--calls", type=int, default=3000, help="calls measured per mechanism")
    parser.add_argument("--warmup", type=int, default=300)
    parser.add_argument("--out", default="transport-benchmark.json")
    parser.add_argument("--serve", help=argparse.SUPPRESS)
    parser.add_argument("--port", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.serve:
        SERVERS[args.serve](args.port)
        return

    names = [n for n in args.only.split(",") if n] or list(MECHANISMS)
    unknown = [n for n in names if n not in MECHANISMS]
    if unknown:
        parser.error(f"unknown mechanism: {', '.join(unknown)}")

    print(f"One client, one server, same machine. {args.calls} calls each; message of {len(encode([MESSAGE]))} bytes.\n")
    results = {}
    for name in names:
        print(f"  measuring {name} ...", flush=True)
        results[name] = run(name, args.calls, args.warmup)

    baseline = results.get("http-current", {}).get("single", {}).get("p50_ms")
    rows = []
    for name, result in results.items():
        description = MECHANISMS[name][3]
        if "skipped" in result:
            rows.append([name, description, "skipped: " + result["skipped"], "", "", "", ""])
            continue
        single, batched = result["single"], result["batched"]
        speedup = f"{baseline / single['p50_ms']:.1f}x" if baseline else ""
        rows.append([name, description, single["p50_ms"], single["p99_ms"], single["calls_per_s"],
                     speedup, batched["messages_per_s"]])
    print()
    print(table(["mechanism", "what it is", "p50 ms", "p99 ms", "calls/s", "vs today",
                 f"msg/s, {BATCH} per call"], rows))
    print("\n'calls/s' is one client sending one message per call and waiting for each reply.")
    print(f"The last column sends {BATCH} messages in each call: batching, on the same transport.")

    with open(args.out, "w") as f:
        json.dump({"message_bytes": len(encode([MESSAGE])), "calls": args.calls, "batch": BATCH,
                   "results": results}, f, indent=2)
    print(f"\nResults written to {args.out}")


if __name__ == "__main__":
    main()
