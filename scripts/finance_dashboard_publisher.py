from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.telegram import send_telegram_message, telegram_enabled


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_config(config_path: Path) -> dict:
    raw = config_path.read_text(encoding="utf-8").strip()
    if not raw:
        raise ValueError(f"Empty config file: {config_path}")
    return json.loads(raw)


def _latest_match(project_root: Path, pattern: str) -> Path | None:
    matches = [p for p in project_root.glob(pattern) if p.is_file()]
    if not matches:
        return None
    return sorted(matches, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def _normalize_required_status_specs(raw_specs: Any) -> list[list[str]]:
    if raw_specs is None:
        return []
    normalized: list[list[str]] = []
    for item in raw_specs:
        if isinstance(item, str):
            normalized.append([item])
            continue
        if isinstance(item, (list, tuple)):
            group = [str(value) for value in item if str(value).strip()]
            if group:
                normalized.append(group)
    return normalized


def _normalize_pattern_candidates(raw_patterns: Any) -> list[str]:
    if raw_patterns is None:
        return []
    if isinstance(raw_patterns, str):
        return [raw_patterns]
    if isinstance(raw_patterns, (list, tuple)):
        return [str(value) for value in raw_patterns if str(value).strip()]
    return []


def _first_available_match(project_root: Path, patterns: list[str]) -> tuple[Path | None, str]:
    for pattern in patterns:
        latest = _latest_match(project_root, pattern)
        if latest is not None:
            return latest, pattern
    return None, patterns[0] if patterns else ""


def _write_launch_cmd(path: Path, project_root: Path, runbook_path: Path) -> None:
    content = [
        "@echo off",
        f"cd /d {project_root}",
        "echo Dashboard Financeiro v1 - checklist de publicacao",
        "echo 1) Abra o Metabase",
        "echo 2) Acesse a colecao Clear OS / Financeiro / v1",
        "echo 3) Atualize os cards usando os SQL em dashboards\\metabase",
        "echo 4) Valide o card de qualidade antes de publicar",
        f"start \"\" \"{runbook_path}\"",
    ]
    path.write_text("\n".join(content) + "\n", encoding="ascii")


def _build_fail_alert(run_id: str, quality_gate: dict, healthcheck_path: Path) -> str:
    lines = [
        "Clear OS - Dashboard Financeiro v1",
        f"ALERTA: quality gate FAIL no run {run_id}",
        f"Detalhe: {quality_gate.get('detail', 'n/a')}",
        f"Arquivo: {healthcheck_path}",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate dashboard readiness and publish status artifacts for Finance Dashboard v1."
    )
    parser.add_argument("--config", required=True, help="Path to config file (JSON content).")
    parser.add_argument("--run-id", required=True, help="Run identifier.")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    config = _load_config(config_path)

    project_root = (config_path.parent / config.get("project_root", "..")).resolve()
    output_dir = (project_root / config.get("output_dir", "out/dashboard_financeiro_v1")).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    required_files = [project_root / rel for rel in config.get("required_files", [])]
    required_status_specs = _normalize_required_status_specs(config.get("required_status_globs", []))
    runbook_rel = config.get("runbook_path", "docs/runbooks/dashboard_financeiro_v1_runbook.md")
    runbook_path = (project_root / runbook_rel).resolve()

    checks: list[dict] = []

    for path in required_files:
        checks.append(
            {
                "check": f"required_file:{path.relative_to(project_root).as_posix()}",
                "ok": path.exists(),
            }
        )

    latest_status_files: dict[str, str] = {}
    for patterns in required_status_specs:
        latest, matched_pattern = _first_available_match(project_root, patterns)
        ok = latest is not None
        check_label = " | ".join(patterns)
        checks.append({"check": f"required_status:{check_label}", "ok": ok})
        if latest:
            latest_status_files[matched_pattern] = str(latest)

    quality_gate = {"ok": False, "source_file": "", "detail": "missing reconciliation status file"}
    recon_patterns = _normalize_pattern_candidates(
        config.get(
        "quality_gate_status_glob",
        [
            "logs/integration/status/bling_supabase_reconciliation*_status.json",
            "11_agentes_automacoes/12_integracoes_agent/pipeline/out/status/bling_supabase_reconciliation*_status.json",
        ],
    )
    )
    recon_latest, _ = _first_available_match(project_root, recon_patterns)
    if recon_latest:
        try:
            recon_data = json.loads(recon_latest.read_text(encoding="utf-8"))
            checks_summary = recon_data.get("checks_summary", {})
            fail_count = int(checks_summary.get("fail", 1))
            status_ok = recon_data.get("status") == "success"
            quality_gate["ok"] = status_ok and fail_count == 0
            quality_gate["source_file"] = str(recon_latest)
            quality_gate["detail"] = f"status={recon_data.get('status')}, fail={fail_count}"
        except Exception as exc:
            quality_gate["source_file"] = str(recon_latest)
            quality_gate["detail"] = f"invalid json: {exc}"

    checks.append({"check": "quality_gate:reconciliation_status", "ok": quality_gate["ok"]})

    ready = all(item["ok"] for item in checks)
    launch_cmd_path = output_dir / f"launch_dashboard_financeiro_v1_{args.run_id}.cmd"
    _write_launch_cmd(launch_cmd_path, project_root, runbook_path)

    healthcheck = {
        "run_id": args.run_id,
        "ready": ready,
        "metabase_collection": config.get("metabase_collection", "Clear OS / Financeiro / v1"),
        "sql_cards_dir": str((project_root / config.get("sql_cards_dir", "dashboards/metabase")).resolve()),
        "checks": checks,
        "quality_gate": quality_gate,
        "latest_status_files": latest_status_files,
        "launch_cmd": str(launch_cmd_path),
        "generated_at": _utc_now_iso(),
    }
    healthcheck_path = output_dir / "dashboard_healthcheck.json"
    healthcheck_path.write_text(json.dumps(healthcheck, indent=2), encoding="utf-8")

    notifications: list[dict] = []
    if not ready and config.get("alert_on_fail", True):
        alert_message = _build_fail_alert(args.run_id, quality_gate, healthcheck_path)
        alert_file = output_dir / f"dashboard_alert_{args.run_id}.txt"
        alert_file.write_text(alert_message + "\n", encoding="utf-8")
        notifications.append({"channel": "file", "ok": True, "detail": str(alert_file)})

        if telegram_enabled():
            ok, detail = send_telegram_message(alert_message)
            notifications.append({"channel": "telegram", "ok": ok, "detail": detail})
        else:
            notifications.append(
                {
                    "channel": "telegram",
                    "ok": False,
                    "detail": "TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID nao configurados",
                }
            )

    status_payload = {
        "status": "success" if ready else "partial",
        "run_id": args.run_id,
        "inputs": [str(config_path)],
        "outputs": [str(healthcheck_path), str(launch_cmd_path)],
        "warnings": [] if ready else ["Dashboard healthcheck has failing checks"],
        "notifications": notifications,
        "error": "",
    }
    status_path = output_dir / f"finance_dashboard_publisher_{args.run_id}_status.json"
    status_path.write_text(json.dumps(status_payload, indent=2), encoding="utf-8")

    print(str(status_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
