from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_SCOPE_BASIS = "empresa_receita_operacional"
DEFAULT_DELEGATE = (
    Path.home() / ".codex" / "skills" / "dre-engine-mckinsey" / "scripts" / "dre_engine_mckinsey.py"
)
BASIS_OPTIONS = (
    "cz_receita_liquida_ajustada",
    "empresa_receita_operacional",
    "empresa_receita_total_com_intercompany",
)


def load_config(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(text)
    try:
        import yaml  # type: ignore

        return yaml.safe_load(text)
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("YAML config requires PyYAML (`pip install pyyaml`) or use .json config") from exc


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def evaluate_scope(
    revenue_df: pd.DataFrame,
    cost_df: pd.DataFrame,
    basis: str,
    cmv_pct: float,
) -> dict[str, Any]:
    merged = revenue_df[["mes_num", basis]].merge(
        cost_df[["mes_num", "custos_variaveis_total", "custo_fixo_base"]],
        on="mes_num",
        how="left",
    )
    merged[basis] = pd.to_numeric(merged[basis], errors="coerce").fillna(0.0)
    merged["custos_variaveis_total"] = pd.to_numeric(merged["custos_variaveis_total"], errors="coerce").fillna(0.0)
    merged["custo_fixo_base"] = pd.to_numeric(merged["custo_fixo_base"], errors="coerce").fillna(0.0)
    merged["receita_liquida"] = merged[basis]
    merged["cmv_proxy"] = (merged["receita_liquida"] * cmv_pct).round(2)
    merged["lucro_bruto"] = (merged["receita_liquida"] - merged["cmv_proxy"]).round(2)
    merged["margem_contribuicao"] = (merged["lucro_bruto"] - merged["custos_variaveis_total"]).round(2)
    merged["ebitda"] = (merged["margem_contribuicao"] - merged["custo_fixo_base"]).round(2)
    return {
        "basis": basis,
        "receita_liquida_total": round(float(merged["receita_liquida"].sum()), 2),
        "custos_variaveis_total": round(float(merged["custos_variaveis_total"].sum()), 2),
        "custo_fixo_total": round(float(merged["custo_fixo_base"].sum()), 2),
        "ebitda_total": round(float(merged["ebitda"].sum()), 2),
        "meses_ebitda_negativo": int((merged["ebitda"] < 0).sum()),
    }


def build_adjusted_revenue(revenue_df: pd.DataFrame, basis: str) -> pd.DataFrame:
    adjusted = revenue_df.copy()
    adjusted["receita_bruta_dre_consistente"] = pd.to_numeric(adjusted[basis], errors="coerce").fillna(0.0)
    adjusted["devolucoes_dre_consistente"] = 0.0
    adjusted["receita_liquida_dre_consistente"] = adjusted["receita_bruta_dre_consistente"]
    adjusted["base_receita_dre"] = basis
    return adjusted


def normalize_output(dre_path: Path, selected_basis: str) -> None:
    df = pd.read_csv(dre_path)
    if "cmv_proxy" in df.columns and "cmv_proxy_35" not in df.columns:
        df["cmv_proxy_35"] = pd.to_numeric(df["cmv_proxy"], errors="coerce").fillna(0.0)
    df["revenue_scope_basis"] = selected_basis
    df.to_csv(dre_path, index=False, encoding="utf-8-sig")


def patch_status(status_dir: Path, run_id: str, metadata_path: Path, selected_basis: str) -> None:
    status_path = status_dir / f"dre_engine_mckinsey_{run_id}_status.json"
    if not status_path.exists():
        return
    status = json.loads(status_path.read_text(encoding="utf-8"))
    status["scope_metadata_path"] = str(metadata_path)
    status["selected_revenue_scope_basis"] = selected_basis
    write_json(status_path, status)


def resolve_input_path(path: Path, fallback_name: str) -> Path:
    if path.exists():
        return path
    sibling_staging = path.parent.parent / "staging" / fallback_name
    if sibling_staging.exists():
        return sibling_staging
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    cfg_path = Path(args.config)
    cfg = load_config(cfg_path)
    out_dir = Path(cfg["output_dir"])
    status_dir = Path(cfg.get("status_dir", out_dir))
    status_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    revenue_cfg = dict(cfg["revenue"])
    revenue_path = resolve_input_path(Path(revenue_cfg["path"]), "stg_revenue_validado.csv")
    cost_path = resolve_input_path(Path(cfg["costs"]["path"]), "stg_costs_validado.csv")
    selected_basis = str(cfg.get("revenue_scope_basis", DEFAULT_SCOPE_BASIS))
    delegate_script = Path(cfg.get("delegate_script", str(DEFAULT_DELEGATE)))
    scope_metadata_path = Path(cfg.get("scope_metadata_path", status_dir / "dre_scope_metadata.json"))
    cmv_pct = float(cfg.get("cmv_pct", 0.35))

    revenue_df = pd.read_csv(revenue_path).copy()
    cost_df = pd.read_csv(cost_path).copy()

    available_bases = [basis for basis in BASIS_OPTIONS if basis in revenue_df.columns]
    comparisons = [evaluate_scope(revenue_df, cost_df, basis, cmv_pct) for basis in available_bases]
    if selected_basis not in revenue_df.columns:
        selected_basis = revenue_cfg.get("receita_liquida_col", selected_basis)

    adjusted_revenue = build_adjusted_revenue(revenue_df, selected_basis)
    adjusted_revenue_path = out_dir / f"{revenue_path.stem}_consistente.csv"
    adjusted_revenue.to_csv(adjusted_revenue_path, index=False, encoding="utf-8-sig")

    metadata = {
        "run_id": args.run_id,
        "source_revenue_path": str(revenue_path),
        "source_cost_path": str(cost_path),
        "adjusted_revenue_path": str(adjusted_revenue_path),
        "selected_basis": selected_basis,
        "candidate_basis": comparisons,
    }
    write_json(scope_metadata_path, metadata)

    delegate_cfg = dict(cfg)
    delegate_cfg["revenue"] = {
        "path": str(adjusted_revenue_path),
        "mes_col": revenue_cfg["mes_col"],
        "receita_bruta_col": "receita_bruta_dre_consistente",
        "devolucoes_col": "devolucoes_dre_consistente",
        "receita_liquida_col": "receita_liquida_dre_consistente",
    }
    delegate_cfg["costs"] = dict(cfg["costs"])
    delegate_cfg["costs"]["path"] = str(cost_path)
    delegate_cfg.pop("delegate_script", None)
    delegate_cfg.pop("revenue_scope_basis", None)
    delegate_cfg.pop("scope_metadata_path", None)

    runtime_cfg_path = status_dir / f"dre_engine_runtime_{args.run_id}.json"
    write_json(runtime_cfg_path, delegate_cfg)

    cmd = [sys.executable, str(delegate_script), "--config", str(runtime_cfg_path), "--run-id", args.run_id]
    subprocess.run(cmd, check=True)

    monthly_path = out_dir / "dre_mckinsey_mensal.csv"
    if monthly_path.exists():
        normalize_output(monthly_path, selected_basis)
    patch_status(status_dir, args.run_id, scope_metadata_path, selected_basis)


if __name__ == "__main__":
    main()
