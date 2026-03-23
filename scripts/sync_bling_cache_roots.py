from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.shared.bling_paths import (  # noqa: E402
    get_bling_root_paths,
    get_bling_sync_files,
)

STATUS_DIR = ROOT / "logs" / "integration" / "status"


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Synchronize Bling cache files from the compatibility root to the canonical root."
    )
    ap.add_argument("--run-id", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    ap.add_argument("--source-root", default="", help="Optional source root override.")
    ap.add_argument("--target-root", default="", help="Optional target root override.")
    ap.add_argument("--dry-run", action="store_true", help="Plan copy operations without writing files.")
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite target files even when source timestamp is not newer.",
    )
    return ap.parse_args()


def _plan_copy(source: Path, target: Path, overwrite: bool) -> tuple[bool, str]:
    if not source.exists():
        return False, "source_missing"
    if not target.exists():
        return True, "target_missing"
    if overwrite:
        return True, "overwrite_forced"
    source_stat = source.stat()
    target_stat = target.stat()
    if source_stat.st_mtime > target_stat.st_mtime or source_stat.st_size != target_stat.st_size:
        return True, "source_newer_or_size_changed"
    return False, "already_current"


def main() -> int:
    args = parse_args()
    roots = get_bling_root_paths()
    source_root = Path(args.source_root).resolve() if args.source_root else roots["sync_source_root"]
    target_root = Path(args.target_root).resolve() if args.target_root else roots["sync_target_root"]
    files = get_bling_sync_files()

    target_root.mkdir(parents=True, exist_ok=True)
    STATUS_DIR.mkdir(parents=True, exist_ok=True)

    copied = 0
    skipped = 0
    missing = 0
    operations: list[dict[str, str | bool]] = []

    for name in files:
        source = source_root / name
        target = target_root / name
        should_copy, reason = _plan_copy(source, target, overwrite=args.overwrite)
        record = {
            "file": name,
            "source": str(source),
            "target": str(target),
            "should_copy": should_copy,
            "reason": reason,
        }
        operations.append(record)

        if reason == "source_missing":
            missing += 1
            continue
        if not should_copy:
            skipped += 1
            continue
        if not args.dry_run:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        copied += 1

    payload = {
        "status": "success",
        "job_name": "sync_bling_cache_roots",
        "run_id": args.run_id,
        "dry_run": args.dry_run,
        "source_root": str(source_root),
        "target_root": str(target_root),
        "summary": {
            "planned_or_copied": copied,
            "skipped": skipped,
            "source_missing": missing,
            "files_total": len(files),
        },
        "operations": operations,
        "generated_at": _utc_now_iso(),
    }
    status_path = STATUS_DIR / f"sync_bling_cache_roots_{args.run_id}_status.json"
    status_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(str(status_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
