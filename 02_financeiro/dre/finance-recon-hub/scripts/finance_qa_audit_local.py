from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_EXPECTED_SCOPE_BASIS = "empresa_receita_operacional"


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


def as_bool(series: pd.Series) -> bool:
    return bool(series.all())


def make_check(check: str, ok: bool, status: str, details: str = "") -> dict[str, Any]:
    return {"check": check, "ok": bool(ok), "status": status, "details": details}


def read_scope_metadata(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_dre_path(dre_path: Path, out_dir: Path) -> Path:
    preferred = out_dir / "dre_mckinsey_mensal.csv"
    if preferred.exists():
        return preferred
    return dre_path


def build_checks(
    raw_df: pd.DataFrame,
    numeric_df: pd.DataFrame,
    scope_metadata: dict[str, Any],
    expected_basis: str,
    negative_ebitda_warn_limit: int,
) -> pd.DataFrame:
    checks: list[dict[str, Any]] = []
    required_cols = [
        "receita_bruta_bling",
        "devolucoes",
        "receita_liquida",
        "cmv_proxy_35",
        "lucro_bruto",
        "custos_variaveis_total",
        "margem_contribuicao",
        "custo_fixo_base",
        "ebitda",
    ]
    missing = [col for col in required_cols if col not in raw_df.columns]
    checks.append(
        make_check(
            "colunas_obrigatorias",
            not missing,
            "PASS" if not missing else "FAIL",
            "" if not missing else f"faltando: {', '.join(missing)}",
        )
    )
    checks.append(make_check("meses_12", len(raw_df) == 12, "PASS" if len(raw_df) == 12 else "FAIL"))

    if {"receita_liquida", "receita_bruta_bling", "devolucoes"}.issubset(numeric_df.columns):
        ok = as_bool(
            numeric_df["receita_liquida"].round(2)
            == (numeric_df["receita_bruta_bling"] - numeric_df["devolucoes"]).round(2)
        )
        checks.append(make_check("receita_liquida_formula", ok, "PASS" if ok else "FAIL"))
    else:
        checks.append(make_check("receita_liquida_formula", False, "FAIL", "colunas insuficientes"))

    if {"lucro_bruto", "receita_liquida", "cmv_proxy_35"}.issubset(numeric_df.columns):
        ok = as_bool(
            numeric_df["lucro_bruto"].round(2)
            == (numeric_df["receita_liquida"] - numeric_df["cmv_proxy_35"]).round(2)
        )
        checks.append(make_check("lucro_bruto_formula", ok, "PASS" if ok else "FAIL"))
    else:
        checks.append(make_check("lucro_bruto_formula", False, "FAIL", "colunas insuficientes"))

    if {"ebitda", "margem_contribuicao", "custo_fixo_base"}.issubset(numeric_df.columns):
        ok = as_bool(
            numeric_df["ebitda"].round(2)
            == (numeric_df["margem_contribuicao"] - numeric_df["custo_fixo_base"]).round(2)
        )
        checks.append(make_check("ebitda_formula", ok, "PASS" if ok else "FAIL"))
    else:
        checks.append(make_check("ebitda_formula", False, "FAIL", "colunas insuficientes"))

    coerced_nans = 0
    for col in required_cols:
        if col in raw_df.columns:
            coerced_nans += int(pd.to_numeric(raw_df[col], errors="coerce").isna().sum())
    checks.append(
        make_check(
            "sem_nan_numerico",
            coerced_nans == 0,
            "PASS" if coerced_nans == 0 else "FAIL",
            "" if coerced_nans == 0 else f"valores invalidos: {coerced_nans}",
        )
    )

    scope_basis_in_output = (
        str(raw_df["revenue_scope_basis"].dropna().iloc[0]) if "revenue_scope_basis" in raw_df.columns and not raw_df["revenue_scope_basis"].dropna().empty else ""
    )
    selected_basis = str(scope_metadata.get("selected_basis", scope_basis_in_output))
    has_metadata = bool(scope_metadata)
    checks.append(
        make_check(
            "scope_metadata_presente",
            has_metadata,
            "PASS" if has_metadata else "FAIL",
            "" if has_metadata else "metadata de escopo nao encontrada",
        )
    )
    matches_expected = selected_basis == expected_basis
    checks.append(
        make_check(
            "scope_receita_esperado",
            matches_expected,
            "PASS" if matches_expected else "FAIL",
            f"esperado={expected_basis}; atual={selected_basis or 'indefinido'}",
        )
    )

    if "revenue_scope_basis" in raw_df.columns:
        output_match = as_bool(raw_df["revenue_scope_basis"].fillna("").eq(expected_basis))
        checks.append(
            make_check(
                "scope_basis_no_output",
                output_match,
                "PASS" if output_match else "FAIL",
                "" if output_match else "coluna revenue_scope_basis nao confere com o esperado",
            )
        )
    else:
        checks.append(make_check("scope_basis_no_output", False, "FAIL", "coluna revenue_scope_basis ausente"))

    negative_ebitda_months = int((numeric_df.get("ebitda", pd.Series(dtype=float)) < 0).sum())
    warn_status = "WARN" if negative_ebitda_months > negative_ebitda_warn_limit else "PASS"
    checks.append(
        make_check(
            "meses_ebitda_negativo_monitorado",
            True,
            warn_status,
            f"meses_negativos={negative_ebitda_months}; limite_warn={negative_ebitda_warn_limit}",
        )
    )

    if {"margem_contribuicao"}.issubset(numeric_df.columns):
        negative_mc = int((numeric_df["margem_contribuicao"] < 0).sum())
        checks.append(
            make_check(
                "margem_contribuicao_negativa",
                negative_mc == 0,
                "PASS" if negative_mc == 0 else "FAIL",
                "" if negative_mc == 0 else f"meses_negativos={negative_mc}",
            )
        )

    return pd.DataFrame(checks)


def render_summary(run_id: str, dre_path: Path, qa: pd.DataFrame) -> str:
    lines = [
        "# Finance QA Summary",
        "",
        f"- Run ID: {run_id}",
        f"- Dataset: {dre_path}",
        f"- Checks: {len(qa)}",
        f"- FAIL: {int((qa['status'] == 'FAIL').sum())}",
        f"- WARN: {int((qa['status'] == 'WARN').sum())}",
        "",
    ]
    for _, row in qa.iterrows():
        details = f" ({row['details']})" if row["details"] else ""
        lines.append(f"- {row['check']}: {row['status']}{details}")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    cfg = load_config(Path(args.config))
    out_dir = Path(cfg["output_dir"])
    status_dir = Path(cfg.get("status_dir", out_dir))
    dre_path = resolve_dre_path(Path(cfg["input_dre_mensal"]), out_dir)
    scope_metadata_path = Path(cfg.get("scope_metadata_path", status_dir / "dre_scope_metadata.json"))
    expected_basis = str(cfg.get("expected_scope_basis", DEFAULT_EXPECTED_SCOPE_BASIS))
    negative_ebitda_warn_limit = int(cfg.get("warn_if_negative_ebitda_months_gt", 4))
    out_dir.mkdir(parents=True, exist_ok=True)
    status_dir.mkdir(parents=True, exist_ok=True)

    raw_df = pd.read_csv(dre_path)
    numeric_df = raw_df.copy()
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
        if col in numeric_df.columns:
            numeric_df[col] = pd.to_numeric(numeric_df[col], errors="coerce")

    scope_metadata = read_scope_metadata(scope_metadata_path)
    qa = build_checks(raw_df, numeric_df, scope_metadata, expected_basis, negative_ebitda_warn_limit)
    qa_path = out_dir / "qa_finance_report.csv"
    qa.to_csv(qa_path, index=False, encoding="utf-8-sig")

    summary_path = out_dir / "qa_finance_summary.md"
    summary_path.write_text(render_summary(args.run_id, dre_path, qa), encoding="utf-8")

    failed = int((qa["status"] == "FAIL").sum())
    warned = int((qa["status"] == "WARN").sum())
    status = {
        "status": "success" if failed == 0 else "partial",
        "run_id": args.run_id,
        "inputs": [str(dre_path)],
        "outputs": [str(qa_path), str(summary_path)],
        "warnings": [] if warned == 0 else [f"{warned} QA warnings"],
        "error": "",
        "scope_metadata_path": str(scope_metadata_path),
    }
    write_json(status_dir / f"finance_qa_audit_{args.run_id}_status.json", status)

    print(str(status_dir / f"finance_qa_audit_{args.run_id}_status.json"))
    print(str(qa_path))


if __name__ == "__main__":
    main()
