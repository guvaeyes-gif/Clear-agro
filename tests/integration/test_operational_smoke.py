from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from integrations.shared.lock_utils import LockAcquisitionError, acquire_lock, release_lock


ROOT = Path(__file__).resolve().parents[2]
PUBLISHER = ROOT / "scripts" / "finance_dashboard_publisher.py"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _create_required_dashboard_files(project_root: Path) -> None:
    required_paths = [
        "supabase/migrations/20260311152000_finance_dashboard_v1_views.sql",
        "dashboards/specs/dashboard_financeiro_v1.md",
        "dashboards/metabase/01_kpis_diarios.sql",
        "dashboards/metabase/02_ap_aging.sql",
        "dashboards/metabase/03_ar_aging.sql",
        "dashboards/metabase/04_cash_projection_30d.sql",
        "dashboards/metabase/05_data_quality_banner.sql",
        "docs/runbooks/dashboard_financeiro_v1_runbook.md",
    ]
    for rel_path in required_paths:
        path = project_root / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("ok\n", encoding="utf-8")


def _publisher_config() -> dict:
    return {
        "project_root": "..",
        "output_dir": "out/dashboard_financeiro_v1",
        "metabase_collection": "Clear OS / Financeiro / v1",
        "sql_cards_dir": "dashboards/metabase",
        "runbook_path": "docs/runbooks/dashboard_financeiro_v1_runbook.md",
        "required_files": [
            "supabase/migrations/20260311152000_finance_dashboard_v1_views.sql",
            "dashboards/specs/dashboard_financeiro_v1.md",
            "dashboards/metabase/01_kpis_diarios.sql",
            "dashboards/metabase/02_ap_aging.sql",
            "dashboards/metabase/03_ar_aging.sql",
            "dashboards/metabase/04_cash_projection_30d.sql",
            "dashboards/metabase/05_data_quality_banner.sql",
            "docs/runbooks/dashboard_financeiro_v1_runbook.md",
        ],
        "required_status_globs": [
            [
                "logs/integration/status/finance_ingest_hub*_status.json",
                "11_agentes_automacoes/12_integracoes_agent/pipeline/out/status/finance_ingest_hub*_status.json",
            ],
            [
                "logs/integration/status/bling_import_generator*_status.json",
                "11_agentes_automacoes/12_integracoes_agent/pipeline/out/status/bling_import_generator*_status.json",
            ],
            [
                "logs/integration/status/bling_supabase_reconciliation*_status.json",
                "11_agentes_automacoes/12_integracoes_agent/pipeline/out/status/bling_supabase_reconciliation*_status.json",
            ],
        ],
        "quality_gate_status_glob": [
            "logs/integration/status/bling_supabase_reconciliation*_status.json",
            "11_agentes_automacoes/12_integracoes_agent/pipeline/out/status/bling_supabase_reconciliation*_status.json",
        ],
        "alert_on_fail": False,
    }


def _run_publisher(project_root: Path, run_id: str) -> tuple[dict, dict]:
    config_path = project_root / "templates" / "default_config.yaml"
    _write_json(config_path, _publisher_config())

    result = subprocess.run(
        [sys.executable, str(PUBLISHER), "--config", str(config_path), "--run-id", run_id],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout

    output_dir = project_root / "out" / "dashboard_financeiro_v1"
    healthcheck = json.loads((output_dir / "dashboard_healthcheck.json").read_text(encoding="utf-8"))
    status_payload = json.loads(
        (output_dir / f"finance_dashboard_publisher_{run_id}_status.json").read_text(encoding="utf-8")
    )
    return healthcheck, status_payload


def test_dashboard_publisher_prefers_new_status_paths(tmp_path: Path) -> None:
    project_root = tmp_path
    _create_required_dashboard_files(project_root)

    _write_json(
        project_root / "logs" / "integration" / "status" / "finance_ingest_hub_new_status.json",
        {"status": "success"},
    )
    _write_json(
        project_root / "logs" / "integration" / "status" / "bling_import_generator_new_status.json",
        {"status": "success"},
    )
    _write_json(
        project_root / "logs" / "integration" / "status" / "bling_supabase_reconciliation_new_status.json",
        {"status": "success", "checks_summary": {"fail": 0}},
    )
    _write_json(
        project_root
        / "11_agentes_automacoes"
        / "12_integracoes_agent"
        / "pipeline"
        / "out"
        / "status"
        / "bling_supabase_reconciliation_legacy_status.json",
        {"status": "failed", "checks_summary": {"fail": 3}},
    )

    healthcheck, status_payload = _run_publisher(project_root, "new_paths")

    assert healthcheck["ready"] is True
    assert "logs/integration/status" in healthcheck["quality_gate"]["source_file"].replace("\\", "/")
    assert status_payload["status"] == "success"


def test_dashboard_publisher_falls_back_to_legacy_status_paths(tmp_path: Path) -> None:
    project_root = tmp_path
    _create_required_dashboard_files(project_root)

    legacy_root = project_root / "11_agentes_automacoes" / "12_integracoes_agent" / "pipeline" / "out" / "status"
    _write_json(legacy_root / "finance_ingest_hub_legacy_status.json", {"status": "success"})
    _write_json(legacy_root / "bling_import_generator_legacy_status.json", {"status": "success"})
    _write_json(
        legacy_root / "bling_supabase_reconciliation_legacy_status.json",
        {"status": "success", "checks_summary": {"fail": 0}},
    )

    healthcheck, status_payload = _run_publisher(project_root, "legacy_paths")

    assert healthcheck["ready"] is True
    assert "11_agentes_automacoes/12_integracoes_agent/pipeline/out/status" in healthcheck["quality_gate"][
        "source_file"
    ].replace("\\", "/")
    assert status_payload["status"] == "success"


def test_bling_wrappers_point_to_canonical_runner() -> None:
    cz_wrapper = (ROOT / "automation" / "jobs" / "run_bling_supabase_daily_cz.cmd").read_text(encoding="utf-8")
    cr_wrapper = (ROOT / "automation" / "jobs" / "run_bling_supabase_daily_cr.cmd").read_text(encoding="utf-8")
    automation_runner = (ROOT / "automation" / "scripts" / "run_bling_supabase_daily.ps1").read_text(encoding="utf-8")
    automation_register = (ROOT / "automation" / "scheduler" / "register_bling_supabase_daily_task.ps1").read_text(
        encoding="utf-8"
    )

    expected = "integrations\\bling\\runners\\run_bling_supabase_daily.ps1"
    assert expected in cz_wrapper
    assert expected in cr_wrapper
    assert expected in automation_runner
    assert "integrations\\bling\\runners\\register_bling_supabase_daily_task.ps1" in automation_register


def test_bling_configs_and_runner_publish_to_new_log_paths() -> None:
    cz_config = json.loads((ROOT / "integrations" / "bling" / "config" / "bling_ingest_hub_v1_cz.json").read_text())
    cr_config = json.loads((ROOT / "integrations" / "bling" / "config" / "bling_ingest_hub_v1_cr.json").read_text())
    runner = (ROOT / "integrations" / "bling" / "runners" / "run_bling_supabase_daily.ps1").read_text(
        encoding="utf-8"
    )

    assert cz_config["status_dir"].replace("/", "\\").endswith("logs\\integration\\status")
    assert cr_config["status_dir"].replace("/", "\\").endswith("logs\\integration\\status")
    for source in cz_config["input_sources"]:
        assert "\\bling_api\\" in source["path"].replace("/", "\\")
    for source in cr_config["input_sources"]:
        assert "\\bling_api\\" in source["path"].replace("/", "\\")
    assert 'logs\\integration\\status' in runner
    assert 'logs\\integration\\runs' in runner
    assert 'bling_pipeline_' in runner
    assert 'supabase_db_push' in runner
    assert 'sync_bling_cache_roots.py' in runner


def test_lock_utils_blocks_second_acquisition(tmp_path: Path) -> None:
    audit_root = tmp_path / "logs" / "audit"
    handle = acquire_lock(
        audit_root=audit_root,
        resource_name="bling_reconciliation_cz",
        execution_id="run_a",
        metadata={"company_scope": "CZ"},
    )
    try:
        try:
            acquire_lock(
                audit_root=audit_root,
                resource_name="bling_reconciliation_cz",
                execution_id="run_b",
            )
            assert False, "second lock acquisition should have failed"
        except LockAcquisitionError as exc:
            assert "Lock ocupado" in str(exc)
    finally:
        release_lock(handle)

    event_files = list((audit_root / "lock_events").glob("*.json"))
    assert event_files, "expected audit lock events to be emitted"
