"""Shared coordination helpers for archive and replication flows."""

from __future__ import annotations

import copy
import dataclasses
import fcntl
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def ensure_ident(identifier: str) -> str:
    if not identifier or not identifier.replace("_", "").isalnum():
        raise ValueError(f"Unsafe identifier: {identifier}")
    return identifier


@dataclass(frozen=True)
class SegmentState:
    epoch: int
    physical_table: str
    closed: bool
    max_id: Optional[int]
    copied_until_id: int
    replication_complete: bool
    archived_at: Optional[str]

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SegmentState":
        segment = cls(
            epoch=int(payload.get("epoch", 0)),
            physical_table=ensure_ident(str(payload.get("physical_table", ""))),
            closed=bool(payload.get("closed", False)),
            max_id=None if payload.get("max_id") is None else int(payload["max_id"]),
            copied_until_id=int(payload.get("copied_until_id", 0)),
            replication_complete=bool(payload.get("replication_complete", False)),
            archived_at=payload.get("archived_at"),
        )
        return segment.normalized()

    def normalized(self) -> "SegmentState":
        if not self.closed:
            return dataclasses.replace(
                self,
                max_id=None,
                replication_complete=False,
                copied_until_id=max(0, self.copied_until_id),
            )
        if self.max_id is None or self.max_id <= 0:
            return dataclasses.replace(
                self,
                max_id=max(0, self.max_id or 0),
                copied_until_id=max(0, self.copied_until_id),
                replication_complete=True,
            )
        copied_until_id = min(max(0, self.copied_until_id), self.max_id)
        replication_complete = copied_until_id >= self.max_id
        return dataclasses.replace(
            self,
            max_id=self.max_id,
            copied_until_id=copied_until_id,
            replication_complete=replication_complete,
        )


@dataclass(frozen=True)
class StreamState:
    logical_table: str
    next_epoch: int
    segments: Tuple[SegmentState, ...]

    @classmethod
    def default(cls, logical_table: str) -> "StreamState":
        logical_table = ensure_ident(logical_table)
        return cls(
            logical_table=logical_table,
            next_epoch=2,
            segments=(
                SegmentState(
                    epoch=1,
                    physical_table=logical_table,
                    closed=False,
                    max_id=None,
                    copied_until_id=0,
                    replication_complete=False,
                    archived_at=None,
                ),
            ),
        )

    @classmethod
    def from_dict(cls, logical_table: str, payload: Dict[str, Any]) -> "StreamState":
        logical_table = ensure_ident(logical_table)
        segments = tuple(
            sorted(
                (SegmentState.from_dict(item) for item in payload.get("segments", [])),
                key=lambda item: item.epoch,
            )
        )
        if not segments:
            return cls.default(logical_table)
        open_segments = [segment for segment in segments if not segment.closed]
        if len(open_segments) != 1:
            raise ValueError(f"State for {logical_table} must contain exactly one open segment")
        max_epoch = max(segment.epoch for segment in segments)
        next_epoch = max(int(payload.get("next_epoch", max_epoch + 1)), max_epoch + 1)
        return cls(logical_table=logical_table, next_epoch=next_epoch, segments=segments)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "logical_table": self.logical_table,
            "next_epoch": self.next_epoch,
            "segments": [dataclasses.asdict(segment.normalized()) for segment in self.segments],
        }

    def open_segment(self) -> SegmentState:
        for segment in self.segments:
            if not segment.closed:
                return segment
        raise ValueError(f"No open segment found for {self.logical_table}")

    def pending_closed_segments(self) -> Tuple[SegmentState, ...]:
        return tuple(segment for segment in self.segments if segment.closed and not segment.replication_complete)

    def close_open_segment(self, archive_name: str, max_id: int, archived_at: Optional[str]) -> "StreamState":
        archive_name = ensure_ident(archive_name)
        open_segment = self.open_segment()
        updated_segments: List[SegmentState] = []
        for segment in self.segments:
            if segment == open_segment:
                updated_segments.append(
                    SegmentState(
                        epoch=segment.epoch,
                        physical_table=archive_name,
                        closed=True,
                        max_id=max(0, int(max_id)),
                        copied_until_id=segment.copied_until_id,
                        replication_complete=False,
                        archived_at=archived_at,
                    ).normalized()
                )
            else:
                updated_segments.append(segment.normalized())
        updated_segments.append(
            SegmentState(
                epoch=self.next_epoch,
                physical_table=self.logical_table,
                closed=False,
                max_id=None,
                copied_until_id=0,
                replication_complete=False,
                archived_at=None,
            )
        )
        return StreamState(
            logical_table=self.logical_table,
            next_epoch=self.next_epoch + 1,
            segments=tuple(sorted(updated_segments, key=lambda item: item.epoch)),
        )

    def prune_dropped(self, physical_tables: Iterable[str]) -> "StreamState":
        drop_set = {ensure_ident(table) for table in physical_tables}
        kept = tuple(segment for segment in self.segments if segment.physical_table not in drop_set)
        return StreamState.from_dict(self.logical_table, {"logical_table": self.logical_table, "next_epoch": self.next_epoch, "segments": [dataclasses.asdict(segment) for segment in kept]})


class CoordinationStore:
    def __init__(self, state_dir: str):
        self.state_dir = Path(state_dir)

    def ensure_dirs(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def path_for(self, logical_table: str) -> Path:
        return self.state_dir / f"{ensure_ident(logical_table)}.json"

    def load(self, logical_table: str, auto_create: bool = False) -> Optional[StreamState]:
        self.ensure_dirs()
        path = self.path_for(logical_table)
        if not path.exists():
            if not auto_create:
                return None
            state = StreamState.default(logical_table)
            self.save(state)
            return state
        payload = json.loads(path.read_text(encoding="utf-8"))
        return StreamState.from_dict(logical_table, payload)

    def save(self, stream_state: StreamState) -> None:
        self.ensure_dirs()
        path = self.path_for(stream_state.logical_table)
        tmp_path = path.with_name(f"{path.name}.tmp.{os.getpid()}.{time.time_ns()}")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(stream_state.to_dict(), handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)

    def drop_decision(
        self,
        stream_state: StreamState,
        candidate_tables: Sequence[str],
        require_replication_complete: bool,
    ) -> Tuple[List[str], List[str]]:
        allow: List[str] = []
        skip: List[str] = []
        segment_lookup = {segment.physical_table: segment for segment in stream_state.segments}
        for candidate in candidate_tables:
            candidate = ensure_ident(candidate)
            segment = segment_lookup.get(candidate)
            if segment is None:
                skip.append(candidate)
                continue
            if require_replication_complete and not segment.replication_complete:
                skip.append(candidate)
                continue
            allow.append(candidate)
        return allow, skip


class FileLock:
    def __init__(
        self,
        path: str,
        timeout_seconds: int = 0,
        poll_seconds: int = 1,
    ) -> None:
        self.path = Path(path)
        self.timeout_seconds = timeout_seconds
        self.poll_seconds = max(1, poll_seconds)
        self.handle: Optional[Any] = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+", encoding="utf-8")
        start = time.monotonic()
        while True:
            try:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.handle.seek(0)
                self.handle.truncate()
                self.handle.write(f"{os.getpid()}\n")
                self.handle.flush()
                return self
            except OSError as exc:
                if self.timeout_seconds <= 0:
                    raise RuntimeError(f"Unable to acquire lock: {self.path}") from exc
                if time.monotonic() - start >= self.timeout_seconds:
                    raise RuntimeError(f"Timed out waiting for lock: {self.path}") from exc
                time.sleep(self.poll_seconds)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.handle:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None
