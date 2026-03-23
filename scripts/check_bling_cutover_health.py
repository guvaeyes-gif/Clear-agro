from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.shared.bling_paths import get_bling_root_paths, get_bling_sync_files  # noqa: E402


STATUS_DIR = ROOT / "logs" / "integration" / "status"
CUTOVER_FILES = [
    ROOT / "config" / "paths" / "bling_cache_roots.json",
    ROOT / "integrations" / "bling" / "runners" / "run_bling_supabase_daily.ps1",
    ROOT / "integrations" / "bling" / "config" / "bling_ingest_hub_v1.json",
    ROOT / "integrations" / "bling" / "config" / "bling_ingest_hub_v1_cz.json",
    ROOT / "integrations" / "bling" / "config" / "bling_ingest_hub_v1_cr.json",
]


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iso_from_ts(timestamp: float | None) -> str:
    if timestamp is None:
        return ""
    return (
        datetime.fromtimestamp(timestamp, tz=UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Check post-cutover health of Bling cache roots and recent statuses.")
    ap.add_argument("--run-id", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    return ap.parse_args()


def _latest_status(pattern: str) -> dict[str, Any]:
    matches = sorted(STATUS_DIR.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    if not matches:
        return {"found": False, "pattern": pattern}

    path = matches[0]
    payload: dict[str, Any] = {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}

    return {
        "found": True,
        "pattern": pattern,
        "path": str(path),
        "last_modified": _iso_from_ts(path.stat().st_mtime),
        "status": payload.get("status", ""),
        "run_id": payload.get("run_id", ""),
    }


def _file_health(canonical_root: Path, compatibility_root: Path, name: str) -> dict[str, Any]:
    canonical = canonical_root / name
    compatibility = compatibility_root / name
    canonical_exists = canonical.exists()
    compatibility_exists = compatibility.exists()

    canonical_size = canonical.stat().st_size if canonical_exists else None
    compatibility_size = compatibility.stat().st_size if compatibility_exists else None
    canonical_mtime = canonical.stat().st_mtime if canonical_exists else None
    compatibility_mtime = compatibility.stat().st_mtime if compatibility_exists else None

    same_size = canonical_exists and compatibility_exists and canonical_size == compatibility_size
    mtime_delta_seconds = (
        abs(canonical_mtime - compatibility_mtime)
        if canonical_mtime is not None and compatibility_mtime is not None
        else None
    )
    synchronized = bool(same_size and mtime_delta_seconds is not None and mtime_delta_seconds <= 2.0)

    return {
        "file": name,
        "canonical_exists": canonical_exists,
        "compatibility_exists": compatibility_exists,
        "canonical_size": canonical_size,
        "compatibility_size": compatibility_size,
        "canonical_last_modified": _iso_from_ts(canonical_mtime),
        "compatibility_last_modified": _iso_from_ts(compatibility_mtime),
        "same_size": same_size,
        "mtime_delta_seconds": mtime_delta_seconds,
        "synchronized": synchronized,
    }


def _cutover_timestamp() -> float:
    timestamps = [path.stat().st_mtime for path in CUTOVER_FILES if path.exists()]
    return max(timestamps) if timestamps else datetime.now(tz=UTC).timestamp()


def main() -> int:
    args = parse_args()
    STATUS_DIR.mkdir(parents=True, exist_ok=True)

    roots = get_bling_root_paths()
    canonical_root = roots["canonical_root"]
    compatibility_root = roots["compatibility_root"]

    file_checks = [_file_health(canonical_root, compatibility_root, name) for name in get_bling_sync_files()]
    total = len(file_checks)
    synchronized = sum(1 for item in file_checks if item["synchronized"])
    missing_canonical = sum(1 for item in file_checks if not item["canonical_exists"])
    missing_compatibility = sum(1 for item in file_checks if not item["compatibility_exists"])
    mismatched = sum(
        1
        for item in file_checks
        if item["canonical_exists"] and item["compatibility_exists"] and not item["synchronized"]
    )

    latest_statuses = {
        "finance_ingest_hub": _latest_status("finance_ingest_hub*_status.json"),
        "bling_import_generator": _latest_status("bling_import_generator*_status.json"),
        "bling_reconciliation": _latest_status("bling_supabase_reconciliation*_status.json"),
        "sync_to_canonical": _latest_status("sync_bling_cache_roots_step3_sync*_status.json"),
        "mirror_to_legacy_dry_run": _latest_status("sync_bling_cache_roots_step4_mirror_dryrun*_status.json"),
    }

    cutover_ts = _cutover_timestamp()
    latest_pipeline_ts = max(
        [
            datetime.fromisoformat(item["last_modified"].replace("Z", "+00:00")).timestamp()
            for key, item in latest_statuses.items()
            if key in {"finance_ingest_hub", "bling_import_generator", "bling_reconciliation"}
            and item.get("found")
            and item.get("last_modified")
        ],
        default=0.0,
    )
    post_cutover_pipeline_seen = latest_pipeline_ts >= cutover_ts

    overall_ok = (
        missing_canonical == 0
        and latest_statuses["sync_to_canonical"].get("found", False)
        and latest_statuses["mirror_to_legacy_dry_run"].get("found", False)
    )

    payload = {
        "status": "success" if overall_ok else "warning",
        "job_name": "check_bling_cutover_health",
        "run_id": args.run_id,
        "generated_at": _utc_now_iso(),
        "roots": {
            "canonical_root": str(canonical_root),
            "compatibility_root": str(compatibility_root),
        },
        "cutover": {
            "reference_timestamp": _iso_from_ts(cutover_ts),
            "post_cutover_pipeline_seen": post_cutover_pipeline_seen,
        },
        "summary": {
            "files_total": total,
            "files_synchronized": synchronized,
            "files_missing_in_canonical": missing_canonical,
            "files_missing_in_compatibility": missing_compatibility,
            "files_mismatched": mismatched,
        },
        "latest_statuses": latest_statuses,
        "files": file_checks,
    }

    status_path = STATUS_DIR / f"check_bling_cutover_health_{args.run_id}_status.json"
    status_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(str(status_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
