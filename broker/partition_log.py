"""
On-disk log of one partition, stored as segment files.

    data/<topic>/partition_<n>/00000000000000000000.log    offsets 0 .. 999
    data/<topic>/partition_<n>/00000000000000001000.log    offsets 1000 .. 1999
    ...

A segment file is named after the offset of its first message and holds one
JSON record per line: {"ts": <time stored>, "m": <the message>}.

Why segments: old messages are removed by deleting whole files, which never
changes the offset of the messages that remain, and a read only opens the
files that contain the offsets it needs.

This class is not thread-safe. The broker holds the partition's lock around
every call.
"""
import collections
import itertools
import json
import os
import shutil
import time
from typing import List, Optional, Tuple


class Segment:
    def __init__(self, path: str, base: int, count: int = 0,
                 first_ts: Optional[float] = None, last_ts: Optional[float] = None):
        self.path = path
        self.base = base          # offset of the first message in this file
        self.count = count        # number of messages in this file
        self.first_ts = first_ts  # time the oldest message was stored
        self.last_ts = last_ts    # time the newest message was stored

    @property
    def end(self) -> int:
        return self.base + self.count


class PartitionLog:
    def __init__(self, directory: str, segment_max_messages: int = 1000, legacy_file: Optional[str] = None,
                 recent_messages: int = 2000):
        self.directory = directory
        self.segment_max_messages = max(int(segment_max_messages), 1)
        self.segments: List[Segment] = []
        # The newest messages are also kept in memory as (offset, time, message),
        # so sending them to followers or readers does not re-read the file.
        self._recent = collections.deque(maxlen=recent_messages)
        if legacy_file and os.path.isfile(legacy_file) and not os.path.isdir(directory):
            self._import_legacy(legacy_file)
        self._load()

    # --- state -----------------------------------------------------------------
    @property
    def exists(self) -> bool:
        """False until the first message is stored (or the log is reset)."""
        return bool(self.segments)

    @property
    def start_offset(self) -> int:
        """Offset of the oldest message still stored."""
        return self.segments[0].base if self.segments else 0

    @property
    def end_offset(self) -> int:
        """Offset the next message will get."""
        return self.segments[-1].end if self.segments else 0

    def stats(self) -> dict:
        oldest = next((s.first_ts for s in self.segments if s.count), None)
        return {
            "start_offset": self.start_offset,
            "end_offset": self.end_offset,
            "segments": len(self.segments),
            "oldest_ts": oldest,
        }

    # --- reading ---------------------------------------------------------------
    def read(self, offset: int = 0, limit: Optional[int] = None) -> Tuple[int, List[dict], List[float]]:
        """
        Messages from `offset` to the end, or at most `limit` of them.

        Returns (start, messages, timestamps). `start` is the offset of the first
        message returned: it is moved forward if `offset` was already purged, and
        back to the end of the log if `offset` is past it.
        """
        start = min(max(offset, self.start_offset), self.end_offset)
        if self._recent and self._recent[0][0] <= start:
            # everything asked for is still in memory
            skip = start - self._recent[0][0]
            items = list(itertools.islice(self._recent, skip, None if limit is None else skip + limit))
            return start, [m for _, _, m in items], [ts for _, ts, _ in items]
        messages, timestamps = [], []
        for segment in self.segments:
            if segment.end <= start:
                continue  # entirely before the requested offset: do not open the file
            for record in self._read_segment(segment)[max(start - segment.base, 0):]:
                messages.append(record["m"])
                timestamps.append(record["ts"])
            if limit is not None and len(messages) >= limit:
                return start, messages[:limit], timestamps[:limit]   # do not open later segments
        return start, messages, timestamps

    # --- writing ---------------------------------------------------------------
    def append(self, messages: List[dict], timestamps: Optional[List[float]] = None) -> None:
        """Store messages at the end of the log, starting a new segment when the current one is full."""
        if not messages:
            if not self.segments:
                self._new_segment(0)
            return
        now = time.time()
        if not timestamps or len(timestamps) != len(messages):
            timestamps = [now] * len(messages)
        if not self.segments:
            self._new_segment(0)
        position = 0
        while position < len(messages):
            segment = self.segments[-1]
            if segment.count >= self.segment_max_messages:
                segment = self._new_segment(segment.end)
            room = self.segment_max_messages - segment.count
            chunk = list(zip(timestamps, messages))[position:position + room]
            with open(segment.path, "a") as f:
                for ts, message in chunk:
                    f.write(json.dumps({"ts": ts, "m": message}) + "\n")
            self._recent.extend((segment.end + i, ts, message) for i, (ts, message) in enumerate(chunk))
            if segment.count == 0:
                segment.first_ts = chunk[0][0]
            segment.count += len(chunk)
            segment.last_ts = chunk[-1][0]
            position += len(chunk)

    def truncate_to(self, offset: int) -> None:
        """Drop every message at `offset` and after (used when this copy is ahead of the leader)."""
        if offset <= self.start_offset:
            self.reset(offset)
            return
        while self._recent and self._recent[-1][0] >= offset:
            self._recent.pop()
        while self.segments and self.segments[-1].base >= offset:
            os.remove(self.segments.pop().path)
        segment = self.segments[-1]
        keep = self._read_segment(segment)[:offset - segment.base]
        with open(segment.path, "w") as f:
            for record in keep:
                f.write(json.dumps(record) + "\n")
        segment.count = len(keep)
        segment.last_ts = keep[-1]["ts"] if keep else None
        segment.first_ts = keep[0]["ts"] if keep else None

    def reset(self, base_offset: int = 0, messages: Optional[List[dict]] = None,
              timestamps: Optional[List[float]] = None) -> None:
        """Replace the whole log with `messages`, the first one at `base_offset`."""
        if os.path.isdir(self.directory):
            shutil.rmtree(self.directory)
        self.segments = []
        self._recent.clear()
        self._new_segment(base_offset)
        if messages:
            self.append(messages, timestamps)

    def purge(self, older_than_ts: float) -> int:
        """
        Delete the oldest segments whose newest message was stored before
        `older_than_ts`. Returns the number of messages removed.

        Only whole segments are removed, oldest first, so a segment holding even
        one newer message stays. Offsets of the remaining messages do not change.
        """
        removed = 0
        end = self.end_offset
        while self.segments:
            segment = self.segments[0]
            if segment.count == 0 or segment.last_ts is None or segment.last_ts >= older_than_ts:
                break
            os.remove(segment.path)
            removed += segment.count
            self.segments.pop(0)
        if removed and not self.segments:
            self._new_segment(end)  # keeps the end offset when everything was purged
        return removed

    # --- internals -------------------------------------------------------------
    def _segment_path(self, base: int) -> str:
        return os.path.join(self.directory, f"{base:020d}.log")

    def _new_segment(self, base: int) -> Segment:
        os.makedirs(self.directory, exist_ok=True)
        segment = Segment(self._segment_path(base), base)
        open(segment.path, "a").close()
        self.segments.append(segment)
        return segment

    @staticmethod
    def _read_segment(segment: Segment) -> List[dict]:
        with open(segment.path, "r") as f:
            return [json.loads(line) for line in f.read().splitlines() if line.strip()]

    def _load(self) -> None:
        self.segments = []
        if not os.path.isdir(self.directory):
            return
        names = sorted(n for n in os.listdir(self.directory) if n.endswith(".log") and n[:-4].isdigit())
        for name in names:
            segment = Segment(os.path.join(self.directory, name), int(name[:-4]))
            records = self._read_segment(segment)
            segment.count = len(records)
            if records:
                segment.first_ts, segment.last_ts = records[0]["ts"], records[-1]["ts"]
            self.segments.append(segment)

    def _import_legacy(self, legacy_file: str) -> None:
        """Convert the old single-file format (one bare message per line) into segments."""
        stored_at = os.path.getmtime(legacy_file)
        with open(legacy_file, "r") as f:
            messages = [json.loads(line) for line in f.read().splitlines() if line.strip()]
        self.append(messages, [stored_at] * len(messages))
        os.remove(legacy_file)
        self.segments = []
        self._recent.clear()
