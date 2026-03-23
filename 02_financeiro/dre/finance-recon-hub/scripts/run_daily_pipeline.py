from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
RUN_LOG = ROOT / "out" / "run_logs" / "daily_pipeline.jsonl"

if str(SRC) not in sys.path:
    sys.path.append(str(SRC))

from recon.config import resolve_bling_api_dir


def append_log(payload: dict) -> None:
    RUN_LOG.parent.mkdir(parents=True, exist_ok=True)
    row = {"ts_utc": datetime.now(timezone.utc).isoformat(), **payload}
    with RUN_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def run_cmd(cmd: list[str], cwd: Path) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
        out = (p.stdout or "") + "\n" + (p.stderr or "")
        return p.returncode, out.strip()
    except Exception as exc:
        return 98, str(exc)


def resolve_bling_dir() -> Path:
    return resolve_bling_api_dir(ROOT)


def main() -> int:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    append_log({"run_id": run_id, "stage": "start"})

    presync_cmd = [
        sys.executable,
        "sync_erp.py",
        "--year",
        str(datetime.now().year),
        "--modules",
        "contas_receber,contas_pagar",
        "--max-pages",
        "3",
    ]
    bling_dir = None
    try:
        bling_dir = resolve_bling_dir()
        pre_rc, pre_out = run_cmd(presync_cmd, cwd=bling_dir)
    except Exception as exc:
        pre_rc, pre_out = 97, str(exc)
    data_stale = pre_rc != 0
    append_log(
        {
            "run_id": run_id,
            "stage": "presync",
            "bling_dir": str(bling_dir) if bling_dir else None,
            "status": "ok" if pre_rc == 0 else "failed",
            "returncode": pre_rc,
            "output_tail": pre_out[-800:],
        }
    )

    recon_cmd = [
        sys.executable,
        "scripts\\run_reconciliation.py",
        "--run-id",
        run_id,
        "--retry-once",
    ]
    if data_stale:
        recon_cmd.append("--data-stale")

    recon_rc, recon_out = run_cmd(recon_cmd, cwd=ROOT)
    append_log(
        {
            "run_id": run_id,
            "stage": "reconciliation",
            "status": "ok" if recon_rc == 0 else "failed",
            "returncode": recon_rc,
            "data_stale": data_stale,
            "output_tail": recon_out[-1200:],
        }
    )
    append_log({"run_id": run_id, "stage": "finish", "status": "ok" if recon_rc == 0 else "failed"})

    print(f"run_id={run_id}")
    print(f"pre_sync_status={'ok' if pre_rc == 0 else 'failed'}")
    print(f"reconciliation_status={'ok' if recon_rc == 0 else 'failed'}")
    return recon_rc


if __name__ == "__main__":
    raise SystemExit(main())
