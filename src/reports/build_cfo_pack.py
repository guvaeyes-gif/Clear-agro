from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
from pandas.errors import EmptyDataError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils import ensure_dir, save_quality_log
STG_DRE = ROOT / "data" / "staging" / "stg_dre.csv"
STG_BLING = ROOT / "data" / "staging" / "stg_bling.csv"
STG_BANKS = ROOT / "data" / "staging" / "stg_banks.csv"

MART_DRE = ROOT / "data" / "marts" / "fact_dre.csv"
MART_CASH = ROOT / "data" / "marts" / "fact_cashflow.csv"
MART_RECON = ROOT / "data" / "marts" / "fact_reconciliation.csv"

EXPORT_SUMMARY = ROOT / "data" / "exports" / "cfo_pack_executive_summary.csv"
EXPORT_BRIDGE = ROOT / "data" / "exports" / "cfo_pack_bridge.csv"
EXPORT_SEGMENTS = ROOT / "data" / "exports" / "cfo_pack_segments.csv"
EXPORT_MD = ROOT / "data" / "exports" / "cfo_pack.md"
QUALITY_PATH = ROOT / "data" / "quality" / "cfo_pack_report.json"


def _read(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _monthify(df: pd.DataFrame, date_col: str = "data") -> pd.DataFrame:
    if date_col in df.columns:
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df["mes"] = df[date_col].dt.to_period("M").astype(str)
    return df


def main() -> int:
    ensure_dir(EXPORT_SUMMARY.parent)
    ensure_dir(MART_DRE.parent)

    dre = _monthify(_read(STG_DRE))
    bling = _monthify(_read(STG_BLING))
    banks = _monthify(_read(STG_BANKS))

    if dre.empty and bling.empty and banks.empty:
        pd.DataFrame([{"metric": "status", "value": "no_data"}]).to_csv(EXPORT_SUMMARY, index=False)
        pd.DataFrame([{"driver": "No data", "valor": 0.0}]).to_csv(EXPORT_BRIDGE, index=False)
        pd.DataFrame([{"segmento": "BR", "receita": 0.0, "dre": 0.0, "cash": 0.0}]).to_csv(
            EXPORT_SEGMENTS, index=False
        )
        EXPORT_MD.write_text(
            "# CFO Pack (Draft)\n\nNenhum dado encontrado em data/staging para gerar analise.",
            encoding="utf-8",
        )
        save_quality_log(
            QUALITY_PATH,
            {
                "status": "warning",
                "message": "No staging data found for CFO pack",
                "exports": [
                    str(EXPORT_SUMMARY.relative_to(ROOT)),
                    str(EXPORT_BRIDGE.relative_to(ROOT)),
                    str(EXPORT_SEGMENTS.relative_to(ROOT)),
                    str(EXPORT_MD.relative_to(ROOT)),
                ],
            },
        )
        print(str(EXPORT_MD))
        return 0

    # Minimal marts
    if not dre.empty:
        fact_dre = dre.copy()
        if "valor" not in fact_dre.columns:
            fact_dre["valor"] = 0.0
        fact_dre.to_csv(MART_DRE, index=False)
    else:
        fact_dre = pd.DataFrame(columns=["mes", "valor", "country", "conta", "centro"])
        fact_dre.to_csv(MART_DRE, index=False)

    if not banks.empty:
        fact_cash = banks.copy()
        if "valor" not in fact_cash.columns:
            fact_cash["valor"] = 0.0
        fact_cash.to_csv(MART_CASH, index=False)
    else:
        fact_cash = pd.DataFrame(columns=["mes", "valor", "country", "descricao", "entidade"])
        fact_cash.to_csv(MART_CASH, index=False)

    # Simple reconciliation by month
    dre_m = fact_dre.groupby("mes", dropna=False)["valor"].sum().rename("dre_competencia") if "mes" in fact_dre.columns else pd.Series(dtype=float)
    cash_m = fact_cash.groupby("mes", dropna=False)["valor"].sum().rename("cash_caixa") if "mes" in fact_cash.columns else pd.Series(dtype=float)
    recon = pd.concat([dre_m, cash_m], axis=1).fillna(0).reset_index()
    if "dre_competencia" not in recon.columns:
        recon["dre_competencia"] = 0.0
    if "cash_caixa" not in recon.columns:
        recon["cash_caixa"] = 0.0
    recon["diferenca"] = recon["dre_competencia"] - recon["cash_caixa"]
    recon["exception_flag"] = recon["diferenca"].abs() > 1.0
    recon.to_csv(MART_RECON, index=False)

    # Executive summary
    receita = float(pd.to_numeric(bling.get("valor", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not bling.empty else 0.0
    dre_total = float(pd.to_numeric(fact_dre.get("valor", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not fact_dre.empty else 0.0
    cash_total = float(pd.to_numeric(fact_cash.get("valor", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not fact_cash.empty else 0.0

    summary = pd.DataFrame(
        [
            {"metric": "receita_bling", "value": receita},
            {"metric": "dre_competencia_total", "value": dre_total},
            {"metric": "cashflow_total", "value": cash_total},
            {"metric": "reconciliation_gap", "value": dre_total - cash_total},
        ]
    )
    summary.to_csv(EXPORT_SUMMARY, index=False)

    # Bridge placeholder
    bridge = pd.DataFrame(
        [
            {"driver": "Preco", "valor": 0.0},
            {"driver": "Volume", "valor": 0.0},
            {"driver": "Mix", "valor": 0.0},
            {"driver": "FX", "valor": 0.0},
            {"driver": "Impostos", "valor": 0.0},
            {"driver": "Devolucoes", "valor": 0.0},
            {"driver": "Frete", "valor": 0.0},
            {"driver": "Comissao", "valor": 0.0},
            {"driver": "Opex", "valor": 0.0},
        ]
    )
    bridge.to_csv(EXPORT_BRIDGE, index=False)

    # Segments placeholder BR vs PY
    segments = pd.DataFrame(
        [
            {"segmento": "BR", "receita": receita, "dre": dre_total, "cash": cash_total},
            {"segmento": "PY", "receita": 0.0, "dre": 0.0, "cash": 0.0},
        ]
    )
    segments.to_csv(EXPORT_SEGMENTS, index=False)

    md = [
        "# CFO Pack (Draft)",
        "",
        "## Executive Summary",
        f"- Receita (Bling): {receita:.2f}",
        f"- DRE Competencia: {dre_total:.2f}",
        f"- Cash Flow: {cash_total:.2f}",
        f"- Gap Competencia vs Caixa: {(dre_total - cash_total):.2f}",
        "",
        "## Reconciliation Exceptions",
        f"- Meses com divergencia material: {int(recon['exception_flag'].sum())}",
        "",
        "## Drivers",
        "- Bridge inicial criada em data/exports/cfo_pack_bridge.csv",
    ]
    EXPORT_MD.write_text("\n".join(md), encoding="utf-8")

    save_quality_log(
        QUALITY_PATH,
        {
            "status": "ok",
            "summary_rows": int(len(summary)),
            "reconciliation_months": int(len(recon)),
            "reconciliation_exceptions": int(recon["exception_flag"].sum()),
            "exports": [
                str(EXPORT_SUMMARY.relative_to(ROOT)),
                str(EXPORT_BRIDGE.relative_to(ROOT)),
                str(EXPORT_SEGMENTS.relative_to(ROOT)),
                str(EXPORT_MD.relative_to(ROOT)),
            ],
        },
    )

    print(str(EXPORT_MD))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
