from __future__ import annotations

import json
from pathlib import Path
import re
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.shared.bling_paths import resolve_bling_file
from src.utils import ensure_dir, save_quality_log

DATA_STAGING = ROOT / "data" / "staging"
DATA_MARTS = ROOT / "data" / "marts"
DATA_EXPORTS = ROOT / "data" / "exports"
DATA_QUALITY = ROOT / "data" / "quality"

AP_CACHE = resolve_bling_file("contas_pagar_cache.jsonl", mode="pipeline")
AR_CACHE = resolve_bling_file("contas_receber_cache.jsonl", mode="pipeline")

DRE_FILES = {
    "CR": DATA_STAGING / "stg_dre_cr.csv",
    "CZ": DATA_STAGING / "stg_dre_cz.csv",
    "EMPRESA": DATA_STAGING / "stg_dre_empresa.csv",
}
BANKS_FILE = DATA_STAGING / "stg_banks.csv"

OUT_FACT_DRE = DATA_MARTS / "fact_dre_finance.csv"
OUT_FACT_AP_AR = DATA_MARTS / "fact_ap_ar.csv"
OUT_FACT_CASH = DATA_MARTS / "fact_cashflow_detailed.csv"
OUT_FACT_RECON = DATA_MARTS / "fact_reconciliation_finance.csv"

OUT_EXEC = DATA_EXPORTS / "finance_executive_summary.csv"
OUT_KPIS = DATA_EXPORTS / "finance_kpis_monthly.csv"
OUT_EXC = DATA_EXPORTS / "finance_reconciliation_exceptions.csv"
OUT_MD = DATA_EXPORTS / "finance_pack.md"
OUT_QUALITY = DATA_QUALITY / "finance_pack_report.json"

MONTH_MAP = {
    "jan": 1,
    "fev": 2,
    "mar": 3,
    "abr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12,
}


def _read_jsonl(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    if not rows:
        return pd.DataFrame()
    return pd.json_normalize(rows)


def _to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _normalize_dre_wide(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["linha", "mes", "valor"])

    # Find the row where headers start (contains "Linha")
    header_idx = None
    for idx in range(min(len(df), 20)):
        row = [str(v).strip().lower() for v in df.iloc[idx].tolist()]
        if any(v == "linha" for v in row):
            header_idx = idx
            break

    if header_idx is None:
        return pd.DataFrame(columns=["linha", "mes", "valor"])

    headers = [str(v).strip() for v in df.iloc[header_idx].tolist()]
    body = df.iloc[header_idx + 1 :].copy()
    body.columns = headers
    body = body.dropna(how="all")

    if "Linha" not in body.columns:
        return pd.DataFrame(columns=["linha", "mes", "valor"])

    keep = ["Linha"] + [m.capitalize() for m in MONTH_MAP.keys()]
    keep = [c for c in keep if c in body.columns]
    out = body[keep].copy()
    out = out.rename(columns={"Linha": "linha"})

    value_cols = [c for c in out.columns if c != "linha"]
    out = out.melt(id_vars=["linha"], value_vars=value_cols, var_name="mes", value_name="valor")
    out["mes"] = out["mes"].str.strip().str.lower()
    out = out[out["mes"].isin(MONTH_MAP.keys())]
    out["mes_num"] = out["mes"].map(MONTH_MAP)
    out["valor"] = _to_float(out["valor"])
    out["linha"] = out["linha"].astype(str).str.strip()
    out = out[out["linha"] != ""]
    return out[["linha", "mes", "mes_num", "valor"]]


def load_dre_fact() -> pd.DataFrame:
    frames = []
    for entity, path in DRE_FILES.items():
        if not path.exists():
            continue
        raw = pd.read_csv(path)
        dre = _normalize_dre_wide(raw)
        if dre.empty:
            continue
        dre["empresa"] = entity
        dre["ano"] = 2025
        dre["data"] = pd.to_datetime(
            {
                "year": dre["ano"],
                "month": dre["mes_num"],
                "day": 1,
            }
        )
        dre["source"] = str(path.relative_to(ROOT))
        frames.append(dre)

    if not frames:
        return pd.DataFrame(columns=["data", "ano", "mes_num", "empresa", "linha", "valor", "source"])
    out = pd.concat(frames, ignore_index=True)
    return out[["data", "ano", "mes_num", "empresa", "linha", "valor", "source"]]


def load_ap_ar_fact() -> pd.DataFrame:
    ap = _read_jsonl(AP_CACHE)
    ar = _read_jsonl(AR_CACHE)

    parts = []
    if not ap.empty:
        x = ap.copy()
        x["tipo"] = "AP"
        x["data_ref"] = pd.to_datetime(x.get("vencimento"), errors="coerce")
        x["valor"] = _to_float(x.get("valor", pd.Series(dtype=float)))
        x["situacao"] = x.get("situacao", pd.Series(dtype=int)).fillna(-1).astype(int)
        x["entidade"] = x.get("contato.nome", x.get("contato.id", "")).astype(str)
        parts.append(x)

    if not ar.empty:
        y = ar.copy()
        y["tipo"] = "AR"
        y["data_ref"] = pd.to_datetime(y.get("vencimento"), errors="coerce")
        y["valor"] = _to_float(y.get("valor", pd.Series(dtype=float)))
        y["situacao"] = y.get("situacao", pd.Series(dtype=int)).fillna(-1).astype(int)
        y["entidade"] = y.get("contato.nome", y.get("contato.id", "")).astype(str)
        parts.append(y)

    if not parts:
        return pd.DataFrame(columns=["tipo", "data_ref", "valor", "situacao", "entidade", "empresa"])

    df = pd.concat(parts, ignore_index=True)
    df["empresa"] = "CLEAR"

    # Assumption for Bling status mapping (can be overridden later)
    status_map = {
        1: "aberto",
        2: "liquidado",
        3: "parcial",
    }
    df["status_label"] = df["situacao"].map(status_map).fillna("outro")
    df["realizado_flag"] = df["status_label"].isin(["liquidado", "parcial"])

    return df[["tipo", "data_ref", "valor", "situacao", "status_label", "realizado_flag", "entidade", "empresa"]]


def classify_cashflow(desc: str, valor: float) -> str:
    d = (desc or "").lower()
    if valor > 0:
        return "cash_in"
    if "tar" in d or "iof" in d or "juros" in d:
        return "finance_cost"
    if "salario" in d or "folha" in d:
        return "payroll"
    if "pix enviado" in d or "pagamento de boleto" in d or "ted" in d or "doc" in d:
        return "payables_outflow"
    return "other_outflow"


def load_cashflow_fact() -> pd.DataFrame:
    if not BANKS_FILE.exists():
        return pd.DataFrame(columns=["data", "valor", "descricao", "cash_class", "empresa"])

    df = pd.read_csv(BANKS_FILE)
    if df.empty:
        return pd.DataFrame(columns=["data", "valor", "descricao", "cash_class", "empresa"])

    df["data"] = pd.to_datetime(df.get("data"), errors="coerce")
    df["valor"] = _to_float(df.get("valor", pd.Series(dtype=float)))
    df["descricao"] = df.get("descricao", "").astype(str)
    df["cash_class"] = [classify_cashflow(d, v) for d, v in zip(df["descricao"], df["valor"])]
    df["empresa"] = "CLEAR"
    df["ano"] = df["data"].dt.year
    df["mes_num"] = df["data"].dt.month
    return df[["data", "ano", "mes_num", "valor", "descricao", "cash_class", "empresa"]]


def compute_kpis(dre: pd.DataFrame, ap_ar: pd.DataFrame, cash: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dre_m = pd.DataFrame(columns=["ano", "mes_num", "dre_receita", "dre_despesa"])
    if not dre.empty:
        tmp = dre.copy()
        tmp["linha_l"] = tmp["linha"].str.lower()
        tmp["is_receita"] = tmp["linha_l"].str.contains("receita", na=False)
        tmp["is_despesa"] = tmp["linha_l"].str.contains(
            "custo|despesa|opex|imposto|frete|comiss|cpv", regex=True, na=False
        )

        rev = tmp[tmp["is_receita"]].groupby(["ano", "mes_num"], as_index=False)["valor"].sum().rename(columns={"valor": "dre_receita"})
        exp = (
            tmp[tmp["is_despesa"]]
            .groupby(["ano", "mes_num"], as_index=False)["valor"]
            .sum()
            .rename(columns={"valor": "dre_despesa"})
        )
        dre_m = rev.merge(exp, on=["ano", "mes_num"], how="outer").fillna(0)

    cash_m = pd.DataFrame(columns=["ano", "mes_num", "cash_in", "cash_out", "ap_realizado_caixa"])
    if not cash.empty:
        c = cash.copy()
        c["cash_in_v"] = c["valor"].where(c["valor"] > 0, 0)
        c["cash_out_v"] = (-c["valor"].where(c["valor"] < 0, 0))
        c["ap_realizado_caixa_v"] = (-c["valor"]).where(c["cash_class"] == "payables_outflow", 0).clip(lower=0)
        cash_m = c.groupby(["ano", "mes_num"], as_index=False).agg(
            cash_in=("cash_in_v", "sum"),
            cash_out=("cash_out_v", "sum"),
            ap_realizado_caixa=("ap_realizado_caixa_v", "sum"),
        )

    ap_ar_m = pd.DataFrame(columns=["ano", "mes_num", "ap_total", "ap_realizado", "ar_total", "ar_realizado"])
    if not ap_ar.empty:
        x = ap_ar.copy()
        x["ano"] = x["data_ref"].dt.year
        x["mes_num"] = x["data_ref"].dt.month
        x = x.dropna(subset=["ano", "mes_num"])

        ap = x[x["tipo"] == "AP"].groupby(["ano", "mes_num"], as_index=False).agg(
            ap_total=("valor", "sum"),
            ap_realizado=("valor", lambda s: s[x.loc[s.index, "realizado_flag"]].sum()),
        )
        ar = x[x["tipo"] == "AR"].groupby(["ano", "mes_num"], as_index=False).agg(
            ar_total=("valor", "sum"),
            ar_realizado=("valor", lambda s: s[x.loc[s.index, "realizado_flag"]].sum()),
        )
        ap_ar_m = ap.merge(ar, on=["ano", "mes_num"], how="outer").fillna(0)

    kpis = dre_m.merge(cash_m, on=["ano", "mes_num"], how="outer").merge(ap_ar_m, on=["ano", "mes_num"], how="outer").fillna(0)
    if kpis.empty:
        kpis = pd.DataFrame(
            [{"ano": 2025, "mes_num": 1, "dre_receita": 0, "dre_despesa": 0, "cash_in": 0, "cash_out": 0, "ap_realizado_caixa": 0, "ap_total": 0, "ap_realizado": 0, "ar_total": 0, "ar_realizado": 0}]
        )

    kpis["ebitda_proxy"] = kpis["dre_receita"] - kpis["dre_despesa"]
    kpis["fcf_proxy"] = kpis["cash_in"] - kpis["cash_out"]
    kpis["capital_giro_proxy"] = (kpis["ar_total"] - kpis["ap_total"])
    kpis["ccc_proxy"] = ((kpis["ar_total"] - kpis["ap_total"]) / kpis["dre_receita"].replace(0, pd.NA)) * 30
    kpis["ccc_proxy"] = kpis["ccc_proxy"].fillna(0)

    recon = kpis[["ano", "mes_num", "dre_despesa", "cash_out", "ap_total", "ap_realizado", "ap_realizado_caixa"]].copy()
    recon["gap_competencia_vs_caixa"] = recon["dre_despesa"] - recon["cash_out"]
    recon["gap_ap_previsto_vs_realizado"] = recon["ap_total"] - recon["ap_realizado"]
    recon["exception_material"] = (recon["gap_competencia_vs_caixa"].abs() > 10000) | (
        recon["gap_ap_previsto_vs_realizado"].abs() > 10000
    )

    exceptions = recon[recon["exception_material"]].copy()

    return kpis, recon, exceptions


def write_pack(kpis: pd.DataFrame, recon: pd.DataFrame, exceptions: pd.DataFrame) -> None:
    kpis_sorted = kpis.sort_values(["ano", "mes_num"]).copy()
    latest = kpis_sorted.tail(1).iloc[0]
    latest_cash = kpis_sorted[(kpis_sorted["cash_in"] + kpis_sorted["cash_out"]) > 0]
    latest_cash = latest_cash.tail(1).iloc[0] if not latest_cash.empty else latest
    latest_ap = kpis_sorted[kpis_sorted["ap_total"] > 0]
    latest_ap = latest_ap.tail(1).iloc[0] if not latest_ap.empty else latest

    exec_rows = [
        {"metric": "Periodo ultimo caixa", "value": f"{int(latest_cash['ano'])}-{int(latest_cash['mes_num']):02d}"},
        {"metric": "Periodo ultimo AP", "value": f"{int(latest_ap['ano'])}-{int(latest_ap['mes_num']):02d}"},
        {"metric": "Receita (DRE) - ultimo caixa", "value": float(latest_cash["dre_receita"])},
        {"metric": "Despesa (DRE) - ultimo caixa", "value": float(latest_cash["dre_despesa"])},
        {"metric": "FCF proxy - ultimo caixa", "value": float(latest_cash["fcf_proxy"])},
        {"metric": "AP total - ultimo AP", "value": float(latest_ap["ap_total"])},
        {"metric": "AP realizado (Bling) - ultimo AP", "value": float(latest_ap["ap_realizado"])},
        {"metric": "AP realizado (Caixa) - ultimo caixa", "value": float(latest_cash["ap_realizado_caixa"])},
        {"metric": "AR total - ultimo AP", "value": float(latest_ap["ar_total"])},
        {"metric": "Capital de giro proxy - ultimo AP", "value": float(latest_ap["capital_giro_proxy"])},
        {"metric": "CCC proxy (dias) - ultimo AP", "value": float(latest_ap["ccc_proxy"])},
    ]
    pd.DataFrame(exec_rows).to_csv(OUT_EXEC, index=False)

    kpis.sort_values(["ano", "mes_num"]).to_csv(OUT_KPIS, index=False)
    exceptions.sort_values(["ano", "mes_num"]).to_csv(OUT_EXC, index=False)

    md = [
        "# Finance Pack - Clear Agro (Draft)",
        "",
        "## Executive Summary",
        f"- Ultimo periodo com caixa: {int(latest_cash['ano'])}-{int(latest_cash['mes_num']):02d}",
        f"- Receita (DRE): {latest_cash['dre_receita']:.2f}",
        f"- Despesa (DRE): {latest_cash['dre_despesa']:.2f}",
        f"- EBITDA proxy: {latest_cash['ebitda_proxy']:.2f}",
        f"- FCF proxy: {latest_cash['fcf_proxy']:.2f}",
        f"- AP realizado (Caixa): {latest_cash['ap_realizado_caixa']:.2f}",
        "",
        f"- Ultimo periodo com AP: {int(latest_ap['ano'])}-{int(latest_ap['mes_num']):02d}",
        f"- AP total / AP realizado (Bling): {latest_ap['ap_total']:.2f} / {latest_ap['ap_realizado']:.2f}",
        f"- AR total: {latest_ap['ar_total']:.2f}",
        f"- Capital de giro proxy: {latest_ap['capital_giro_proxy']:.2f}",
        f"- CCC proxy (dias): {latest_ap['ccc_proxy']:.2f}",
        "",
        "## Reconciliation",
        f"- Excecoes materiais: {len(exceptions)}",
        "- Arquivo de excecoes: data/exports/finance_reconciliation_exceptions.csv",
        "",
        "## Escopo de entidades",
        "- DRE paralelo: CR, CZ, EMPRESA",
        "- AP/AR/Caixa: CLEAR (Bling + bancos)",
        "",
        "## Observacoes",
        "- Mapeamento de status Bling assumido: 1=aberto, 2=liquidado, 3=parcial.",
        "- Ajustar em etapa de governanca se o dicionario do ERP diferir.",
    ]
    OUT_MD.write_text("\n".join(md), encoding="utf-8")


def main() -> int:
    ensure_dir(DATA_MARTS)
    ensure_dir(DATA_EXPORTS)
    ensure_dir(DATA_QUALITY)

    dre = load_dre_fact()
    ap_ar = load_ap_ar_fact()
    cash = load_cashflow_fact()

    dre.to_csv(OUT_FACT_DRE, index=False)
    ap_ar.to_csv(OUT_FACT_AP_AR, index=False)
    cash.to_csv(OUT_FACT_CASH, index=False)

    kpis, recon, exceptions = compute_kpis(dre, ap_ar, cash)
    recon.to_csv(OUT_FACT_RECON, index=False)

    write_pack(kpis, recon, exceptions)

    save_quality_log(
        OUT_QUALITY,
        {
            "status": "ok",
            "rows": {
                "fact_dre_finance": int(len(dre)),
                "fact_ap_ar": int(len(ap_ar)),
                "fact_cashflow_detailed": int(len(cash)),
                "fact_reconciliation_finance": int(len(recon)),
                "reconciliation_exceptions": int(len(exceptions)),
            },
            "outputs": [
                str(OUT_FACT_DRE.relative_to(ROOT)),
                str(OUT_FACT_AP_AR.relative_to(ROOT)),
                str(OUT_FACT_CASH.relative_to(ROOT)),
                str(OUT_FACT_RECON.relative_to(ROOT)),
                str(OUT_EXEC.relative_to(ROOT)),
                str(OUT_KPIS.relative_to(ROOT)),
                str(OUT_EXC.relative_to(ROOT)),
                str(OUT_MD.relative_to(ROOT)),
            ],
        },
    )

    print(str(OUT_MD))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
