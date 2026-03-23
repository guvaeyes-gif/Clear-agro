from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--dre-path", default="")
    parser.add_argument("--ap-path", default="")
    parser.add_argument("--output-dir", default="")
    return parser.parse_args()


def to_brl(value: float) -> str:
    s = f"{value:,.2f}"
    return f"R$ {s}".replace(",", "X").replace(".", ",").replace("X", ".")


def infer_paths(run_dir: Path, dre_path: str, ap_path: str, output_dir: str) -> tuple[Path, Path, Path]:
    exports_dir = run_dir / "control_tower" / "data" / "exports"
    resolved_dre = Path(dre_path) if dre_path else exports_dir / "dre_mckinsey_mensal.csv"
    resolved_ap = Path(ap_path) if ap_path else exports_dir / "ap_bling_classificado.csv"
    resolved_output = Path(output_dir) if output_dir else exports_dir
    return resolved_dre, resolved_ap, resolved_output


def load_dre(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path).copy()
    numeric_cols = [
        "receita_liquida",
        "custos_variaveis_total",
        "custo_fixo_base",
        "margem_contribuicao",
        "margem_contribuicao_pct",
        "ebitda",
        "cmv_proxy_35",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df.sort_values("mes_num").reset_index(drop=True)


def summarize_ap(ap_path: Path, target_months: list[str]) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    if not ap_path.exists():
        empty = pd.DataFrame(columns=["mes", "categoria_mckinsey", "valor_total"])
        return empty, {}, {}
    ap = pd.read_csv(ap_path).copy()
    if "mes" not in ap.columns or "valor" not in ap.columns:
        empty = pd.DataFrame(columns=["mes", "categoria_mckinsey", "valor_total"])
        return empty, {}, {}
    ap["mes"] = ap["mes"].astype(str).str[:7]
    ap["valor"] = pd.to_numeric(ap["valor"], errors="coerce").fillna(0.0)
    ap = ap[ap["mes"].isin(target_months)].copy()
    cat_map: dict[str, pd.DataFrame] = {}
    supplier_map: dict[str, pd.DataFrame] = {}
    for month in target_months:
        month_df = ap[ap["mes"] == month].copy()
        cat_map[month] = (
            month_df.groupby("categoria_mckinsey", as_index=False)
            .agg(valor_total=("valor", "sum"))
            .sort_values("valor_total", ascending=False)
            .head(5)
        )
        supplier_map[month] = (
            month_df.groupby(["fornecedor", "categoria_mckinsey"], as_index=False)
            .agg(valor_total=("valor", "sum"))
            .sort_values("valor_total", ascending=False)
            .head(5)
        )
    ap_summary = (
        ap.groupby(["mes", "categoria_mckinsey"], as_index=False)
        .agg(valor_total=("valor", "sum"))
        .sort_values(["mes", "valor_total"], ascending=[True, False])
    )
    return ap_summary, cat_map, supplier_map


def build_analysis(dre: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
    positive = dre[dre["ebitda"] >= 0].copy()
    if positive.empty:
        positive = dre.copy()
    benchmark = {
        "median_positive_revenue": float(positive["receita_liquida"].median()),
        "median_positive_variable_ratio": float((positive["custos_variaveis_total"] / positive["receita_liquida"]).replace([pd.NA, float("inf")], 0.0).fillna(0.0).median()),
        "median_positive_fixed_cost": float(positive["custo_fixo_base"].median()),
        "median_positive_mc_pct": float(positive["margem_contribuicao_pct"].median()),
    }

    negative = dre[dre["ebitda"] < 0].copy()
    if negative.empty:
        return negative, benchmark

    negative["variable_cost_ratio"] = (
        negative["custos_variaveis_total"] / negative["receita_liquida"].replace(0, pd.NA)
    ).fillna(0.0)
    negative["fixed_cost_ratio"] = (
        negative["custo_fixo_base"] / negative["receita_liquida"].replace(0, pd.NA)
    ).fillna(0.0)
    negative["break_even_revenue_current_margin"] = (
        negative["custo_fixo_base"] / negative["margem_contribuicao_pct"].replace(0, pd.NA)
    ).fillna(0.0)
    negative["revenue_gap_to_break_even"] = (
        negative["break_even_revenue_current_margin"] - negative["receita_liquida"]
    ).clip(lower=0.0)

    negative["scale_headwind_value"] = (
        (benchmark["median_positive_revenue"] - negative["receita_liquida"]).clip(lower=0.0)
        * benchmark["median_positive_mc_pct"]
    ).fillna(0.0)
    negative["variable_headwind_value"] = (
        (negative["variable_cost_ratio"] - benchmark["median_positive_variable_ratio"]).clip(lower=0.0)
        * negative["receita_liquida"]
    ).fillna(0.0)
    negative["fixed_headwind_value"] = (
        negative["custo_fixo_base"] - benchmark["median_positive_fixed_cost"]
    ).clip(lower=0.0)

    def classify_driver(row: pd.Series) -> str:
        impacts = {
            "receita_insuficiente": float(row["scale_headwind_value"]),
            "custo_variavel_alto": float(row["variable_headwind_value"]),
            "custo_fixo_alto": float(row["fixed_headwind_value"]),
        }
        return max(impacts, key=impacts.get)

    negative["driver_principal"] = negative.apply(classify_driver, axis=1)
    return negative, benchmark


def render_report(
    run_id: str,
    analysis: pd.DataFrame,
    benchmark: dict[str, float],
    cat_map: dict[str, pd.DataFrame],
    supplier_map: dict[str, pd.DataFrame],
) -> str:
    lines = [
        "# Analise de EBITDA Negativo",
        "",
        f"- Run ID: {run_id}",
        f"- Meses com EBITDA negativo: {len(analysis)}",
        f"- Receita mediana dos meses positivos: {to_brl(benchmark['median_positive_revenue'])}",
        f"- Margem de contribuicao mediana dos meses positivos: {benchmark['median_positive_mc_pct']:.2%}",
        f"- Custo variavel mediano dos meses positivos: {benchmark['median_positive_variable_ratio']:.2%} da receita",
        f"- Custo fixo mediano dos meses positivos: {to_brl(benchmark['median_positive_fixed_cost'])}",
        "",
    ]

    for _, row in analysis.iterrows():
        month = str(row["mes"])
        lines.extend(
            [
                f"## {month}",
                "",
                f"- EBITDA: {to_brl(float(row['ebitda']))}",
                f"- Driver principal: `{row['driver_principal']}`",
                f"- Receita liquida: {to_brl(float(row['receita_liquida']))}",
                f"- Margem de contribuicao: {to_brl(float(row['margem_contribuicao']))} ({float(row['margem_contribuicao_pct']):.2%})",
                f"- Custo fixo: {to_brl(float(row['custo_fixo_base']))}",
                f"- Gap de receita para break-even com margem atual: {to_brl(float(row['revenue_gap_to_break_even']))}",
                f"- Headwind de escala: {to_brl(float(row['scale_headwind_value']))}",
                f"- Headwind de custo variavel: {to_brl(float(row['variable_headwind_value']))}",
                f"- Headwind de custo fixo: {to_brl(float(row['fixed_headwind_value']))}",
                "",
                "Top categorias AP no mes:",
            ]
        )
        cat_df = cat_map.get(month, pd.DataFrame())
        if cat_df.empty:
            lines.append("- sem dados AP para o mes")
        else:
            for _, cat_row in cat_df.iterrows():
                lines.append(f"- {cat_row['categoria_mckinsey']}: {to_brl(float(cat_row['valor_total']))}")
        lines.append("")
        lines.append("Top fornecedores AP no mes:")
        sup_df = supplier_map.get(month, pd.DataFrame())
        if sup_df.empty:
            lines.append("- sem dados AP para o mes")
        else:
            for _, sup_row in sup_df.iterrows():
                lines.append(
                    f"- {sup_row['fornecedor']} ({sup_row['categoria_mckinsey']}): {to_brl(float(sup_row['valor_total']))}"
                )
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    run_dir = Path(args.run_dir).resolve()
    dre_path, ap_path, output_dir = infer_paths(run_dir, args.dre_path, args.ap_path, args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    dre = load_dre(dre_path)
    analysis, benchmark = build_analysis(dre)
    target_months = analysis["mes"].astype(str).tolist() if not analysis.empty else []
    _, cat_map, supplier_map = summarize_ap(ap_path, target_months)

    details_out = output_dir / "negative_ebitda_analysis.csv"
    if analysis.empty:
        details = pd.DataFrame(columns=["mes", "ebitda", "driver_principal"])
    else:
        details = analysis[
            [
                "mes_num",
                "mes",
                "receita_liquida",
                "custos_variaveis_total",
                "custo_fixo_base",
                "margem_contribuicao",
                "margem_contribuicao_pct",
                "ebitda",
                "break_even_revenue_current_margin",
                "revenue_gap_to_break_even",
                "scale_headwind_value",
                "variable_headwind_value",
                "fixed_headwind_value",
                "driver_principal",
            ]
        ].copy()
    details.to_csv(details_out, index=False, encoding="utf-8-sig")

    report_text = render_report(run_dir.name, analysis, benchmark, cat_map, supplier_map)
    report_out = output_dir / "negative_ebitda_report.md"
    report_out.write_text(report_text + "\n", encoding="utf-8")

    print(str(report_out))
    print(str(details_out))


if __name__ == "__main__":
    main()
