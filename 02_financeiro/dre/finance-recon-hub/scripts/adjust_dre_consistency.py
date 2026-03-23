from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd


REVENUE_FILE = "dre_2025_ajustado_genetica_agrun_mensal.csv"
COST_FILE = "dre_2025_governanca_mckinsey_mensal.csv"
DEFAULT_BASIS = "empresa_receita_operacional"
BASIS_OPTIONS = (
    "cz_receita_liquida_ajustada",
    "empresa_receita_operacional",
    "empresa_receita_total_com_intercompany",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True, help="Run directory created by monthly-fin-close.")
    parser.add_argument("--basis", choices=BASIS_OPTIONS, default=DEFAULT_BASIS)
    parser.add_argument("--cmv-pct", type=float, default=0.35)
    parser.add_argument("--run-id", default="consistent_dre")
    parser.add_argument(
        "--dre-script",
        default=str(Path.home() / ".codex" / "skills" / "dre-engine-mckinsey" / "scripts" / "dre_engine_mckinsey.py"),
    )
    parser.add_argument(
        "--qa-script",
        default=str(Path.home() / ".codex" / "skills" / "finance-qa-audit" / "scripts" / "finance_qa_audit.py"),
    )
    return parser.parse_args()


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {path}")
    return pd.read_csv(path).copy()


def evaluate_basis(
    revenue_df: pd.DataFrame,
    cost_df: pd.DataFrame,
    basis: str,
    cmv_pct: float,
) -> dict[str, float | int | str]:
    df = revenue_df[["mes_num", basis]].merge(
        cost_df[["mes_num", "custos_variaveis_total", "custo_fixo_base"]],
        on="mes_num",
        how="left",
    )
    df[basis] = pd.to_numeric(df[basis], errors="coerce").fillna(0.0)
    df["custos_variaveis_total"] = pd.to_numeric(df["custos_variaveis_total"], errors="coerce").fillna(0.0)
    df["custo_fixo_base"] = pd.to_numeric(df["custo_fixo_base"], errors="coerce").fillna(0.0)
    df["receita_liquida"] = df[basis]
    df["cmv_proxy"] = (df["receita_liquida"] * cmv_pct).round(2)
    df["lucro_bruto"] = (df["receita_liquida"] - df["cmv_proxy"]).round(2)
    df["margem_contribuicao"] = (df["lucro_bruto"] - df["custos_variaveis_total"]).round(2)
    df["ebitda"] = (df["margem_contribuicao"] - df["custo_fixo_base"]).round(2)
    return {
        "basis": basis,
        "receita_liquida_total": round(float(df["receita_liquida"].sum()), 2),
        "custos_variaveis_total": round(float(df["custos_variaveis_total"].sum()), 2),
        "custo_fixo_total": round(float(df["custo_fixo_base"].sum()), 2),
        "ebitda_total": round(float(df["ebitda"].sum()), 2),
        "meses_ebitda_negativo": int((df["ebitda"] < 0).sum()),
    }


def build_adjusted_revenue(revenue_df: pd.DataFrame, basis: str) -> pd.DataFrame:
    adjusted = revenue_df.copy()
    adjusted["receita_bruta_dre_consistente"] = pd.to_numeric(adjusted[basis], errors="coerce").fillna(0.0)
    adjusted["devolucoes_dre_consistente"] = 0.0
    adjusted["receita_liquida_dre_consistente"] = adjusted["receita_bruta_dre_consistente"]
    adjusted["base_receita_dre"] = basis
    return adjusted


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_dre_for_qa(dre_path: Path) -> None:
    df = pd.read_csv(dre_path)
    if "cmv_proxy" in df.columns and "cmv_proxy_35" not in df.columns:
        df["cmv_proxy_35"] = pd.to_numeric(df["cmv_proxy"], errors="coerce").fillna(0.0)
        df.to_csv(dre_path, index=False, encoding="utf-8-sig")


def build_qa_frame(dre_path: Path) -> pd.DataFrame:
    df = pd.read_csv(dre_path)
    for col in [
        "receita_bruta_bling",
        "devolucoes",
        "receita_liquida",
        "cmv_proxy_35",
        "lucro_bruto",
        "custos_variaveis_total",
        "margem_contribuicao",
        "custo_fixo_base",
        "ebitda",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    checks = [
        ("meses_12", len(df) == 12),
        (
            "receita_liquida_formula",
            (df["receita_liquida"].round(2) == (df["receita_bruta_bling"] - df["devolucoes"]).round(2)).all()
            if {"receita_liquida", "receita_bruta_bling", "devolucoes"}.issubset(df.columns)
            else False,
        ),
        (
            "lucro_bruto_formula",
            (df["lucro_bruto"].round(2) == (df["receita_liquida"] - df["cmv_proxy_35"]).round(2)).all()
            if {"lucro_bruto", "receita_liquida", "cmv_proxy_35"}.issubset(df.columns)
            else False,
        ),
        (
            "ebitda_formula",
            (df["ebitda"].round(2) == (df["margem_contribuicao"] - df["custo_fixo_base"]).round(2)).all()
            if {"ebitda", "margem_contribuicao", "custo_fixo_base"}.issubset(df.columns)
            else False,
        ),
        ("sem_nan_numerico", int(df.select_dtypes(include=["number"]).isna().sum().sum()) == 0),
    ]
    return pd.DataFrame(
        [{"check": check, "ok": bool(ok), "status": "PASS" if ok else "FAIL"} for check, ok in checks]
    )


def normalize_qa_outputs(dre_path: Path, exports_dir: Path, status_dir: Path, run_id: str) -> None:
    qa = build_qa_frame(dre_path)
    qa_path = exports_dir / "qa_finance_report.csv"
    qa.to_csv(qa_path, index=False, encoding="utf-8-sig")

    summary_lines = [
        "# Finance QA Summary",
        "",
        f"- Run ID: {run_id}",
        f"- Dataset: {dre_path}",
        f"- Checks: {len(qa)}",
        f"- FAIL: {int((qa['status'] == 'FAIL').sum())}",
        "",
    ]
    for _, row in qa.iterrows():
        summary_lines.append(f"- {row['check']}: {row['status']}")
    (exports_dir / "qa_finance_summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    status = {
        "status": "success" if bool(qa["ok"].all()) else "partial",
        "run_id": run_id,
        "inputs": [str(dre_path)],
        "outputs": [str(qa_path), str(exports_dir / "qa_finance_summary.md")],
        "warnings": [] if bool(qa["ok"].all()) else ["Some QA checks failed"],
        "error": "",
    }
    write_json(status_dir / f"finance_qa_audit_{run_id}_status.json", status)


def run_python(script: Path, config_path: Path, run_id: str) -> None:
    cmd = [sys.executable, str(script), "--config", str(config_path), "--run-id", run_id]
    subprocess.run(cmd, check=True)


def render_report(comparison: pd.DataFrame, selected_basis: str, output_path: Path) -> None:
    selected = comparison.loc[comparison["basis"] == selected_basis].iloc[0]
    current = comparison.loc[comparison["basis"] == "cz_receita_liquida_ajustada"].iloc[0]
    lines = [
        "# DRE Consistency Review",
        "",
        f"- Base atual do snapshot: `cz_receita_liquida_ajustada`",
        f"- Base ajustada para regeneracao: `{selected_basis}`",
        f"- EBITDA atual: {current['ebitda_total']:.2f}",
        f"- EBITDA ajustado: {selected['ebitda_total']:.2f}",
        f"- Delta EBITDA: {(selected['ebitda_total'] - current['ebitda_total']):.2f}",
        f"- Meses com EBITDA negativo (atual): {int(current['meses_ebitda_negativo'])}",
        f"- Meses com EBITDA negativo (ajustado): {int(selected['meses_ebitda_negativo'])}",
        "",
        "Ajuste aplicado: custos consolidados foram mantidos e a receita do DRE foi alinhada para a mesma abrangencia.",
        "Para evitar dupla interpretacao de receita bruta e devolucoes na auditoria, a base ajustada usa a receita operacional escolhida como receita liquida do DRE e zera devolucoes nesse recorte.",
        "",
    ]
    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    run_dir = Path(args.run_dir).resolve()
    exports_dir = run_dir / "control_tower" / "data" / "exports"
    source_revenue_path = exports_dir / REVENUE_FILE
    source_cost_path = exports_dir / COST_FILE
    revenue_df = load_csv(source_revenue_path)
    cost_df = load_csv(source_cost_path)

    comparison = pd.DataFrame(
        [evaluate_basis(revenue_df, cost_df, basis, args.cmv_pct) for basis in BASIS_OPTIONS]
    )

    adjusted_root = run_dir / "consistent_dre" / args.basis
    adjusted_exports = adjusted_root / "control_tower" / "data" / "exports"
    adjusted_out = adjusted_root / "control_tower" / "out"
    adjusted_exports.mkdir(parents=True, exist_ok=True)
    adjusted_out.mkdir(parents=True, exist_ok=True)

    comparison.to_csv(adjusted_root / "dre_revenue_scope_comparison.csv", index=False, encoding="utf-8-sig")
    render_report(comparison, args.basis, adjusted_root / "dre_consistency_review.md")

    adjusted_revenue = build_adjusted_revenue(revenue_df, args.basis)
    adjusted_revenue_path = adjusted_exports / "dre_2025_ajustado_genetica_agrun_mensal_consistente.csv"
    adjusted_revenue.to_csv(adjusted_revenue_path, index=False, encoding="utf-8-sig")

    dre_config = {
        "output_dir": str(adjusted_exports),
        "status_dir": str(adjusted_out),
        "cmv_pct": args.cmv_pct,
        "revenue": {
            "path": str(adjusted_revenue_path),
            "mes_col": "mes_num",
            "receita_bruta_col": "receita_bruta_dre_consistente",
            "devolucoes_col": "devolucoes_dre_consistente",
            "receita_liquida_col": "receita_liquida_dre_consistente",
        },
        "costs": {
            "path": str(source_cost_path),
            "mes_col": "mes_num",
            "variavel_total_col": "custos_variaveis_total",
            "fixo_total_col": "custo_fixo_base",
        },
        "fixed_split": {
            "pessoal": 0.45,
            "estrutura": 0.20,
            "tecnologia": 0.15,
            "gna": 0.20,
        },
    }
    dre_config_path = adjusted_root / "step_2_dre-engine-mckinsey.json"
    write_json(dre_config_path, dre_config)
    run_python(Path(args.dre_script), dre_config_path, args.run_id)
    normalize_dre_for_qa(adjusted_exports / "dre_mckinsey_mensal.csv")

    qa_config = {
        "input_dre_mensal": str(adjusted_exports / "dre_mckinsey_mensal.csv"),
        "output_dir": str(adjusted_exports),
        "status_dir": str(adjusted_out),
    }
    qa_config_path = adjusted_root / "step_3_finance-qa-audit.json"
    write_json(qa_config_path, qa_config)
    run_python(Path(args.qa_script), qa_config_path, args.run_id)
    normalize_qa_outputs(adjusted_exports / "dre_mckinsey_mensal.csv", adjusted_exports, adjusted_out, args.run_id)


if __name__ == "__main__":
    main()
