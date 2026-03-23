from __future__ import annotations

import json
import os
import shutil
import socket
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_token(value: str) -> str:
    text = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value).strip().lower())
    return text.strip("_") or "lock"


class LockAcquisitionError(RuntimeError):
    pass


@dataclass
class LockHandle:
    audit_root: Path
    resource_name: str
    lock_dir: Path
    metadata: dict[str, Any]


def _emit_event(audit_root: Path, payload: dict[str, Any]) -> Path:
    events_dir = audit_root / "lock_events"
    events_dir.mkdir(parents=True, exist_ok=True)
    resource_token = _safe_token(payload.get("resource_name", "lock"))
    event_token = _safe_token(payload.get("event_type", "event"))
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    path = events_dir / f"{stamp}_{resource_token}_{event_token}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def acquire_lock(
    audit_root: Path,
    resource_name: str,
    execution_id: str,
    metadata: dict[str, Any] | None = None,
    wait_seconds: float = 0.0,
    poll_interval: float = 1.0,
) -> LockHandle:
    audit_root.mkdir(parents=True, exist_ok=True)
    locks_dir = audit_root / "locks"
    locks_dir.mkdir(parents=True, exist_ok=True)

    resource_token = _safe_token(resource_name)
    lock_dir = locks_dir / resource_token
    base_metadata = {
        "resource_name": resource_name,
        "execution_id": execution_id,
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "acquired_at": _utc_now_iso(),
    }
    if metadata:
        base_metadata.update(metadata)

    deadline = time.time() + max(wait_seconds, 0.0)
    while True:
        try:
            lock_dir.mkdir()
            (lock_dir / "metadata.json").write_text(
                json.dumps(base_metadata, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            _emit_event(
                audit_root,
                {
                    "event_type": "lock_acquired",
                    **base_metadata,
                },
            )
            return LockHandle(
                audit_root=audit_root,
                resource_name=resource_name,
                lock_dir=lock_dir,
                metadata=base_metadata,
            )
        except FileExistsError:
            current_holder: dict[str, Any] = {}
            metadata_path = lock_dir / "metadata.json"
            if metadata_path.exists():
                try:
                    current_holder = json.loads(metadata_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    current_holder = {"raw_metadata_path": str(metadata_path)}

            _emit_event(
                audit_root,
                {
                    "event_type": "lock_blocked",
                    "resource_name": resource_name,
                    "execution_id": execution_id,
                    "blocked_at": _utc_now_iso(),
                    "current_holder": current_holder,
                },
            )
            if time.time() >= deadline:
                holder_exec = current_holder.get("execution_id", "desconhecido")
                raise LockAcquisitionError(
                    f"Lock ocupado para {resource_name}. Execucao atual: {holder_exec}"
                )
            time.sleep(max(poll_interval, 0.1))


def release_lock(handle: LockHandle, event_type: str = "lock_released") -> None:
    _emit_event(
        handle.audit_root,
        {
            "event_type": event_type,
            "resource_name": handle.resource_name,
            "execution_id": handle.metadata.get("execution_id", ""),
            "released_at": _utc_now_iso(),
            "metadata": handle.metadata,
        },
    )
    shutil.rmtree(handle.lock_dir, ignore_errors=True)


@contextmanager
def managed_lock(
    audit_root: Path,
    resource_name: str,
    execution_id: str,
    metadata: dict[str, Any] | None = None,
    wait_seconds: float = 0.0,
    poll_interval: float = 1.0,
) -> Iterator[LockHandle]:
    handle = acquire_lock(
        audit_root=audit_root,
        resource_name=resource_name,
        execution_id=execution_id,
        metadata=metadata,
        wait_seconds=wait_seconds,
        poll_interval=poll_interval,
    )
    try:
        yield handle
    finally:
        release_lock(handle)
