from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def _validated_source_name(file_name: str) -> tuple[str, str] | None:
    mapping = {
        "bling_faturamento_mensal_sem_devolucoes_2025.csv": ("faturamento_validado", "csv"),
        "dre_2025_ajustado_genetica_agrun_mensal.csv": ("revenue_validado", "csv"),
        "dre_2025_governanca_mckinsey_mensal.csv": ("costs_validado", "csv"),
        "DRE_GRUPO_CZ_CLEAR_2025_latest.xlsx": ("dre_base_raw", "xlsx"),
        "contas_pagar_cache.jsonl": ("ap_jsonl", "jsonl"),
        "contatos_cache.jsonl": ("contacts_jsonl", "jsonl"),
        "vendas_2025_cache.jsonl": ("sales_2025_jsonl", "jsonl"),
        "vendas_2026_cache.jsonl": ("sales_2026_jsonl", "jsonl"),
    }
    return mapping.get(file_name)


def _load_validated_ingest_sources(repo_root: Path) -> list[dict[str, str]]:
    candidates = sorted(
        (repo_root / "out" / "aios" / "monthly-fin-close").glob("validated_*"),
        reverse=True,
    )
    for run_path in candidates:
        snapshot_path = run_path / "validated_inputs_snapshot.json"
        if not snapshot_path.exists():
            continue
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        sources: list[dict[str, str]] = []
        for item in payload.get("files", []):
            source = Path(str(item.get("source", "")))
            mapped = _validated_source_name(source.name)
            if mapped is None or not source.exists():
                continue
            name, typ = mapped
            sources.append({"name": name, "path": str(source), "type": typ})
        if sources:
            return sources
    return []


def _validated_sources_by_name(repo_root: Path) -> dict[str, dict[str, str]]:
    return {item["name"]: item for item in _load_validated_ingest_sources(repo_root)}


def _orchestrator_paths() -> tuple[Path, Path]:
    home = Path.home()
    script = home / ".codex" / "skills" / "finance-control-tower-orchestrator" / "scripts" / "finance_control_tower_orchestrator.py"
    config = home / ".codex" / "skills" / "finance-control-tower-orchestrator" / "templates" / "default_config.json"
    return script, config


def _copy_if_exists(src: Path, dst: Path) -> bool:
    if not src.exists():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def _rewrite_finance_control_tower_paths(value: object, new_root: str) -> object:
    old_root = "C:\\Users\\cesar.zarovski\\Finance_Control_Tower"
    if isinstance(value, str):
        return value.replace(old_root, new_root)
    if isinstance(value, list):
        return [_rewrite_finance_control_tower_paths(v, new_root) for v in value]
    if isinstance(value, dict):
        return {k: _rewrite_finance_control_tower_paths(v, new_root) for k, v in value.items()}
    return value


def _local_wrapper_script(repo_root: Path, step_name: str) -> Path | None:
    mapping = {
        "ap-cost-classifier": repo_root / "scripts" / "ap_cost_classifier_local.py",
        "dre-engine-mckinsey": repo_root / "scripts" / "dre_engine_mckinsey_local.py",
        "finance-qa-audit": repo_root / "scripts" / "finance_qa_audit_local.py",
    }
    return mapping.get(step_name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run monthly financial close via local AIOS fallback.")
    parser.add_argument("--run-id", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    parser.add_argument("--input-dir", default="")
    parser.add_argument("--fail-fast", action="store_true")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    clear_os_root = repo_root.parents[2]
    run_dir = repo_root / "out" / "aios" / "monthly-fin-close" / args.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    script_path, config_path = _orchestrator_paths()
    if not script_path.exists() or not config_path.exists():
        msg = {
            "run_id": args.run_id,
            "status": "failed",
            "error": "orchestrator_skill_not_found",
            "script": str(script_path),
            "config": str(config_path),
            "generated_at": datetime.now().isoformat(),
        }
        (run_dir / "execution.json").write_text(json.dumps(msg, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(msg, ensure_ascii=False))
        raise SystemExit(2)

    env = dict(**os.environ)
    if args.input_dir:
        env["AIOS_INPUT_DIR"] = args.input_dir

    with config_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    remap_root = str(run_dir / "control_tower")
    cfg["output_dir"] = str(run_dir)
    steps = cfg.get("steps", [])
    cfg_dir = run_dir / "configs"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    validated_ingest_sources = _load_validated_ingest_sources(repo_root)
    validated_sources_by_name = _validated_sources_by_name(repo_root)

    for idx, step in enumerate(steps):
        step_cfg_path = Path(str(step["config"]))
        with step_cfg_path.open("r", encoding="utf-8") as f:
            step_cfg = json.load(f)
        step_cfg = _rewrite_finance_control_tower_paths(step_cfg, remap_root)
        step_name = str(step.get("name", ""))
        scope_metadata_path = str((run_dir / "control_tower" / "out" / "dre_scope_metadata.json").resolve())
        local_script = _local_wrapper_script(repo_root, step_name)
        if step_name == "finance-ingest-hub" and validated_ingest_sources:
            step_cfg["input_sources"] = validated_ingest_sources
        elif step_name == "dre-engine-mckinsey":
            step_cfg["delegate_script"] = str(step["script"])
            step_cfg["revenue_scope_basis"] = "empresa_receita_operacional"
            step_cfg["scope_metadata_path"] = scope_metadata_path
            if local_script is not None:
                step["script"] = str(local_script)
        elif step_name == "finance-qa-audit":
            step_cfg["input_dre_mensal"] = str(
                (run_dir / "control_tower" / "data" / "exports" / "dre_mckinsey_mensal.csv").resolve()
            )
            step_cfg["expected_scope_basis"] = "empresa_receita_operacional"
            step_cfg["scope_metadata_path"] = scope_metadata_path
            step_cfg["warn_if_negative_ebitda_months_gt"] = 4
            if local_script is not None:
                step["script"] = str(local_script)
        elif step_name == "ap-cost-classifier" and validated_sources_by_name:
            ap_src = validated_sources_by_name.get("ap_jsonl")
            contacts_src = validated_sources_by_name.get("contacts_jsonl")
            sales_2026 = validated_sources_by_name.get("sales_2026_jsonl")
            sales_2025 = validated_sources_by_name.get("sales_2025_jsonl")
            if ap_src is not None:
                step_cfg["ap_jsonl"] = ap_src["path"]
            if contacts_src is not None:
                step_cfg["contacts_jsonl"] = contacts_src["path"]
            fallback_sales: list[str] = []
            if sales_2026 is not None:
                fallback_sales.append(sales_2026["path"])
            if sales_2025 is not None:
                fallback_sales.append(sales_2025["path"])
            if fallback_sales:
                step_cfg["fallback_sales_jsonl"] = fallback_sales
            if local_script is not None:
                step["script"] = str(local_script)
        elif step_name == "finance-dashboard-publisher":
            run_dir_rel = run_dir.relative_to(clear_os_root).as_posix()
            step_cfg = {
                "project_root": str(clear_os_root),
                "output_dir": f"{run_dir_rel}/control_tower/out",
                "required_files": [
                    f"{run_dir_rel}/control_tower/data/exports/dre_mckinsey_mensal.csv",
                    f"{run_dir_rel}/control_tower/data/exports/dre_mckinsey_resumo.csv",
                    f"{run_dir_rel}/control_tower/data/exports/qa_finance_summary.md",
                ],
                "required_status_globs": [
                    [f"{run_dir_rel}/control_tower/out/finance_ingest_hub_{args.run_id}_status.json"],
                    [f"{run_dir_rel}/control_tower/out/dre_engine_mckinsey_{args.run_id}_status.json"],
                    [f"{run_dir_rel}/control_tower/out/finance_qa_audit_{args.run_id}_status.json"],
                    [f"{run_dir_rel}/control_tower/out/ap_cost_classifier_{args.run_id}_status.json"],
                ],
                "runbook_path": "docs/runbooks/dashboard_financeiro_v1_runbook.md",
                "sql_cards_dir": "dashboards/metabase",
                "metabase_collection": "Clear OS / Financeiro / v1",
            }
            step["script"] = str(clear_os_root / "scripts" / "finance_dashboard_publisher.py")
        temp_step_cfg = cfg_dir / f"step_{idx + 1}_{step.get('name', 'step')}.json"
        temp_step_cfg.write_text(json.dumps(step_cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        step["config"] = str(temp_step_cfg)

    local_cfg = run_dir / "orchestrator_config.json"
    local_cfg.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    cmd = [sys.executable, str(script_path), "--config", str(local_cfg), "--run-id", args.run_id]
    if args.fail_fast:
        cmd.append("--fail-fast")

    started_at = datetime.now().isoformat()
    proc = subprocess.run(cmd, cwd=str(repo_root), capture_output=True, text=True, env=env)
    finished_at = datetime.now().isoformat()

    status_src = run_dir / f"finance_control_tower_orchestrator_{args.run_id}_status.json"
    summary_src = run_dir / f"finance_control_tower_orchestrator_{args.run_id}_summary.md"

    copied_status = _copy_if_exists(status_src, run_dir / "orchestrator_status.json")
    copied_summary = _copy_if_exists(summary_src, run_dir / "orchestrator_summary.md")

    pm_decision = {
        "run_id": args.run_id,
        "agent": "@pm",
        "worker": "pm-adapter",
        "checklist_worker": "pm-checklist",
        "decision": "pending_review",
        "required_action": "manual_review" if proc.returncode != 0 else "approve_or_publish",
        "status_source": str(run_dir / "orchestrator_status.json"),
        "summary_source": str(run_dir / "orchestrator_summary.md"),
        "generated_at": datetime.now().isoformat(),
    }
    (run_dir / "pm_governance_decision.json").write_text(
        json.dumps(pm_decision, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    execution = {
        "run_id": args.run_id,
        "status": "success" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "started_at": started_at,
        "finished_at": finished_at,
        "command": cmd,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "copied_status": copied_status,
        "copied_summary": copied_summary,
    }
    (run_dir / "execution.json").write_text(json.dumps(execution, ensure_ascii=False, indent=2), encoding="utf-8")

    latest = {
        "latest_run_id": args.run_id,
        "path": str(run_dir),
        "updated_at": datetime.now().isoformat(),
    }
    latest_path = repo_root / "out" / "aios" / "monthly-fin-close" / "latest.json"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(str(run_dir / "execution.json"))
    raise SystemExit(proc.returncode)


if __name__ == "__main__":
    main()
