import os
import json
import unicodedata
import streamlit as st
import pandas as pd
import altair as alt
from io import BytesIO
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

BASE = ROOT / "out" / "base_unificada.xlsx"

from src.data import (
    load_sheets,
    load_bling_realizado,
    load_bling_nfe,
    load_bling_contas,
    load_bling_estoque,
    load_sales_targets_view,
    load_sales_pipeline_view,
    load_crm_priority_queue,
    get_crm_view_error,
)
from src.metrics import compute_kpis, vendedor_performance_period, meta_realizado_mensal, sparkline_last_months, period_label
from src.viz import fmt_brl_abbrev, fmt_pct, bar_meta_realizado, bar_meta_realizado_single, sparkline
from src.metas_db import init_db, list_metas, create_meta, update_meta, pause_metas, summary_targets, transfer_assets, transfer_metas_futuras, seed_demo
from src.telegram import build_alerts_message, send_telegram_message, telegram_enabled

PROFILE = os.getenv("CRM_PROFILE", "director").strip().lower()
PUBLIC_REVIEW = os.getenv("CRM_PUBLIC_REVIEW", "").strip().lower() in {"1", "true", "yes", "on"}
APP_BUILD = os.getenv("APP_BUILD", "2026-03-01.1")
APP_TITLE = "Clear Agro CRM"
ACL_PATH = ROOT / "data" / "access_control.json"
DEFAULT_YEAR = 2026

st.set_page_config(page_title=APP_TITLE, layout="wide")

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1280px; margin: 0 auto;}
    h1, h2, h3 {color:#1f2a44;}
    div[data-testid="stMetricValue"] {font-size: 20px; white-space: nowrap;}
    div[data-testid="stMetricLabel"] {font-size: 12px;}
    </style>
    """,
    unsafe_allow_html=True,
)


def style_table(df: pd.DataFrame, numeric_cols=None):
    if numeric_cols is None:
        numeric_cols = []
    styler = df.style
    styler = styler.apply(lambda x: ["background-color: #f7f8fa" if i % 2 else "" for i in range(len(x))], axis=0)
    styler = styler.set_properties(**{"text-align": "left"})
    if numeric_cols:
        cols = [c for c in numeric_cols if c in df.columns]
        if cols:
            styler = styler.set_properties(subset=cols, **{"text-align": "right"})
    return styler


def status_chip(s: str) -> str:
    s = (s or "").upper()
    if s == "ATIVO":
        return "ATIVO"
    if s == "PAUSADO":
        return "PAUSADO"
    if s == "DESLIGADO":
        return "DESLIGADO"
    if s == "TRANSFERIDO":
        return "TRANSFERIDO"
    return s or "-"


def load_acl() -> dict:
    if not ACL_PATH.exists():
        return {}
    try:
        with ACL_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _clean_list(values):
    return [v for v in values if v not in (None, "", "TODOS")]


def _vendor_key(value: object) -> str:
    txt = str(value or "").strip().upper()
    if not txt:
        return ""
    txt = "".join(ch for ch in unicodedata.normalize("NFKD", txt) if not unicodedata.combining(ch))
    return " ".join(txt.split())


def apply_acl(df: pd.DataFrame, vendor_col: str = "vendedor") -> pd.DataFrame:
    if PROFILE != "gestor" or df.empty:
        return df
    acl = load_acl().get("gestor", {})
    allow = _clean_list(acl.get("allow_vendedores", []))
    block = _clean_list(acl.get("block_vendedores", []))
    block_canais = [str(c).strip().upper() for c in _clean_list(acl.get("block_canais", []))]
    block_clientes = [str(c).strip().upper() for c in _clean_list(acl.get("block_clientes", []))]

    # filter by vendor if possible
    if vendor_col in df.columns:
        if allow:
            df = df[df[vendor_col].isin(allow)]
        elif block:
            df = df[~df[vendor_col].isin(block)]

    # filter by canal if present
    if "canal" in df.columns and block_canais:
        df = df.copy()
        df["canal"] = df["canal"].astype(str)
        df = df[~df["canal"].str.upper().isin(block_canais)]

    if "cliente" in df.columns and block_clientes:
        df = df.copy()
        df["cliente"] = df["cliente"].astype(str)
        df = df[~df["cliente"].str.upper().isin(block_clientes)]

    return df


def apply_acl_codes(df: pd.DataFrame, vendor_col: str = "sales_rep_code") -> pd.DataFrame:
    if PROFILE != "gestor" or df.empty:
        return df
    acl = load_acl().get("gestor", {})
    allow = {_vendor_key(v) for v in _clean_list(acl.get("allow_vendedores", []))}
    block = {_vendor_key(v) for v in _clean_list(acl.get("block_vendedores", []))}
    block_canais = {str(c).strip().upper() for c in _clean_list(acl.get("block_canais", []))}
    block_clientes = {str(c).strip().upper() for c in _clean_list(acl.get("block_clientes", []))}

    out = df.copy()
    if vendor_col in out.columns:
        vendor_keys = out[vendor_col].map(_vendor_key)
        if allow:
            out = out[vendor_keys.isin(allow)]
        elif block:
            out = out[~vendor_keys.isin(block)]

    for channel_col in ["canal", "channel"]:
        if channel_col in out.columns and block_canais:
            out = out[~out[channel_col].astype(str).str.upper().isin(block_canais)]

    for customer_col in ["cliente", "customer_name"]:
        if customer_col in out.columns and block_clientes:
            out = out[~out[customer_col].astype(str).str.upper().isin(block_clientes)]

    return out


def filter_vendor_scope(df: pd.DataFrame, selected_vendor: str, columns: list[str]) -> pd.DataFrame:
    if df.empty or selected_vendor == "TODOS":
        return df
    selected_key = _vendor_key(selected_vendor)
    mask = pd.Series(False, index=df.index)
    for column in columns:
        if column in df.columns:
            mask = mask | df[column].map(_vendor_key).eq(selected_key)
    return df[mask]


def filter_targets_view(
    view_df: pd.DataFrame,
    target_year: int,
    period_type: str,
    month_num: int | None = None,
    quarter_num: int | None = None,
    state: str | None = None,
    sales_rep_code: str | None = None,
    statuses: list[str] | None = None,
) -> pd.DataFrame:
    if view_df.empty:
        return view_df
    df = view_df.copy()
    if "target_year" in df.columns:
        df = df[pd.to_numeric(df["target_year"], errors="coerce").eq(int(target_year))]
    if "period_type" in df.columns:
        df = df[df["period_type"].astype(str).str.upper() == str(period_type).upper()]
    if month_num is not None and "month_num" in df.columns:
        df = df[pd.to_numeric(df["month_num"], errors="coerce").eq(int(month_num))]
    if quarter_num is not None and "quarter_num" in df.columns:
        df = df[pd.to_numeric(df["quarter_num"], errors="coerce").eq(int(quarter_num))]
    if state and "state" in df.columns:
        df = df[df["state"].astype(str).str.upper() == str(state).upper()]
    if sales_rep_code and "sales_rep_code" in df.columns:
        df = df[df["sales_rep_code"].map(_vendor_key) == _vendor_key(sales_rep_code)]
    if statuses and "status" in df.columns:
        df = df[df["status"].astype(str).str.upper().isin([str(item).upper() for item in statuses])]
    return df


def numeric_column(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(0.0, index=df.index, dtype=float)
    return pd.to_numeric(df[column], errors="coerce").fillna(0)


def sort_by_available(df: pd.DataFrame, specs: list[tuple[str, bool]]) -> pd.DataFrame:
    columns = [column for column, _ in specs if column in df.columns]
    if not columns:
        return df
    ascending = [direction for column, direction in specs if column in df.columns]
    return df.sort_values(columns, ascending=ascending)


def filter_pipeline_period(
    df: pd.DataFrame,
    target_year: int,
    month_num: int | None,
    ytd: bool,
    quarter_num: int | None = None,
) -> pd.DataFrame:
    if df.empty or "expected_close_month" not in df.columns:
        return df
    out = df.copy()
    out["expected_close_month"] = pd.to_datetime(out["expected_close_month"], errors="coerce")
    out = out[out["expected_close_month"].dt.year.fillna(target_year).astype(int) == int(target_year)]
    if quarter_num is not None:
        q_start = (quarter_num - 1) * 3 + 1
        q_end = q_start + 2
        return out[out["expected_close_month"].dt.month.between(q_start, q_end)]
    if month_num is not None and not ytd:
        return out[out["expected_close_month"].dt.month == month_num]
    if ytd:
        return out[out["expected_close_month"].dt.month <= pd.Timestamp.today().month]
    return out


def filter_queue_period(
    df: pd.DataFrame,
    target_year: int,
    month_num: int | None,
    ytd: bool,
    quarter_num: int | None = None,
) -> pd.DataFrame:
    if df.empty or "due_at" not in df.columns:
        return df
    out = df.copy()
    out["due_at"] = pd.to_datetime(out["due_at"], errors="coerce")
    out = out[(out["due_at"].isna()) | (out["due_at"].dt.year.fillna(target_year).astype(int) == int(target_year))]
    if quarter_num is not None:
        q_start = (quarter_num - 1) * 3 + 1
        q_end = q_start + 2
        return out[out["due_at"].isna() | out["due_at"].dt.month.between(q_start, q_end)]
    if month_num is not None and not ytd:
        return out[out["due_at"].isna() | (out["due_at"].dt.month == month_num)]
    if ytd:
        return out[out["due_at"].isna() | (out["due_at"].dt.month <= pd.Timestamp.today().month)]
    return out


def warn_crm_backend(view_name: str, label: str) -> None:
    error = get_crm_view_error(view_name)
    if error:
        st.warning(f"Falha ao ler {label} no CRM/Supabase: {error}")


def build_targets_summary(view_df: pd.DataFrame, period_type: str) -> dict:
    empty_series = pd.DataFrame(columns=["meta_valor", "realizado_valor"])
    if view_df.empty:
        return {
            "kpis": {"meta": 0.0, "realizado": 0.0, "atingimento_pct": 0.0, "delta": 0.0},
            "series": empty_series,
            "uf": pd.DataFrame(columns=["state", "meta_valor", "realizado_valor"]),
            "vendedor": pd.DataFrame(columns=["sales_rep_code", "sales_rep_name", "meta_valor", "realizado_valor"]),
        }

    df = view_df.copy()
    meta_total = float(numeric_column(df, "target_value").sum())
    realizado_total = float(numeric_column(df, "actual_value").sum())
    period_col = "quarter_num" if str(period_type).upper() == "QUARTER" else "month_num"

    series = pd.DataFrame()
    if period_col in df.columns and {"target_value", "actual_value"}.issubset(df.columns):
        series = (
            df.groupby(period_col, dropna=False)[["target_value", "actual_value"]]
            .sum()
            .reset_index()
            .rename(columns={period_col: "periodo", "target_value": "meta_valor", "actual_value": "realizado_valor"})
            .sort_values("periodo")
        )
        if str(period_type).upper() == "QUARTER":
            series = series.rename(columns={"periodo": "quarter"})
        else:
            series = series.rename(columns={"periodo": "mes"})

    uf = pd.DataFrame()
    if "state" in df.columns and {"target_value", "actual_value"}.issubset(df.columns):
        uf = (
            df.groupby("state", dropna=False)[["target_value", "actual_value"]]
            .sum()
            .reset_index()
            .rename(columns={"state": "estado", "target_value": "meta_valor", "actual_value": "realizado_valor"})
            .sort_values("estado")
        )

    vendedor = pd.DataFrame()
    vendor_cols = [c for c in ["sales_rep_code", "sales_rep_name"] if c in df.columns]
    if vendor_cols and {"target_value", "actual_value"}.issubset(df.columns):
        vendedor = (
            df.groupby(vendor_cols, dropna=False)[["target_value", "actual_value"]]
            .sum()
            .reset_index()
            .rename(columns={"target_value": "meta_valor", "actual_value": "realizado_valor"})
            .sort_values(vendor_cols)
        )

    atingimento = (realizado_total / meta_total * 100) if meta_total else 0.0
    return {
        "kpis": {
            "meta": meta_total,
            "realizado": realizado_total,
            "atingimento_pct": atingimento,
            "delta": realizado_total - meta_total,
        },
        "series": series,
        "uf": uf,
        "vendedor": vendedor,
    }


init_db()

if st.sidebar.button("Recarregar base"):
    for loader in [load_sheets, load_sales_targets_view, load_sales_pipeline_view, load_crm_priority_queue]:
        try:
            loader.clear()
        except Exception:
            pass
sheets = load_sheets()
if not sheets:
    st.warning("Base principal nao encontrada. Carregando modo revisao com dados vazios.")
    sheets = {
        "metas": pd.DataFrame(columns=["data", "vendedor", "meta"]),
        "realizado": pd.DataFrame(columns=["data", "vendedor", "receita"]),
        "oportunidades": pd.DataFrame(columns=["cliente", "vendedor", "volume_potencial", "probabilidade", "data_proximo_passo"]),
        "atividades": pd.DataFrame(columns=["data"]),
    }

# Apply ACL on loaded sheets (gestor profile)
for key in ["metas", "realizado", "oportunidades"]:
    if key in sheets and not sheets[key].empty:
        sheets[key] = apply_acl(sheets[key], vendor_col="vendedor")

# Sidebar controls
years = set()
for key in ["metas", "realizado"]:
    df = sheets.get(key, pd.DataFrame())
    if not df.empty and "data" in df.columns:
        years.update(df["data"].dt.year.dropna().astype(int).unique().tolist())
# include years from metas.db (Metas Comerciais)
db_years = list_metas()
if PROFILE == "gestor" and not db_years.empty and "vendedor_id" in db_years.columns:
    acl = load_acl().get("gestor", {})
    allow = _clean_list(acl.get("allow_vendedores", []))
    block = _clean_list(acl.get("block_vendedores", []))
    if allow:
        db_years = db_years[db_years["vendedor_id"].isin(allow)]
    elif block:
        db_years = db_years[~db_years["vendedor_id"].isin(block)]
if not db_years.empty and "ano" in db_years.columns:
    years.update(db_years["ano"].dropna().astype(int).unique().tolist())
years = sorted(years) if years else [DEFAULT_YEAR]

year = st.sidebar.selectbox("Ano", options=years, index=years.index(DEFAULT_YEAR) if DEFAULT_YEAR in years else 0)

months = list(range(1, 13))
month_labels = ["TODOS"] + [pd.Timestamp(year=year, month=m, day=1).strftime("%b").title() for m in months]
month_map = {"TODOS": None}
for m, label in zip(months, month_labels[1:]):
    month_map[label] = m

period_mode = st.sidebar.selectbox("Visao de periodo", options=["YTD", "Mes", "Quarter"], index=0)
selected_month = None
selected_quarter = None
effective_ytd = False

if period_mode == "Mes":
    month_label = st.sidebar.selectbox("Mes", options=month_labels, index=1 if len(month_labels) > 1 else 0)
    selected_month = month_map[month_label]
    effective_ytd = selected_month is None
elif period_mode == "Quarter":
    current_q = ((pd.Timestamp.today().month - 1) // 3) + 1
    selected_quarter = st.sidebar.selectbox("Quarter", options=[1, 2, 3, 4], index=current_q - 1)
else:
    effective_ytd = True

use_bling = st.sidebar.checkbox("Usar realizado do Bling", value=PUBLIC_REVIEW)
if use_bling:
    br = load_bling_realizado()
    if not br.empty:
        sheets["realizado"] = br

show_inactive_vendors = st.sidebar.checkbox("Mostrar inativos/historico", value=False)

# Build vendor list focused on selected period; keep optional historical expansion.
vendor_scores: dict[str, float] = {}
all_vendors_set: set[str] = set()
today = pd.Timestamp.today()

for sheet_name, value_col in [("metas", "meta"), ("realizado", "receita")]:
    dfv = sheets.get(sheet_name, pd.DataFrame())
    if dfv.empty or "vendedor" not in dfv.columns:
        continue
    dfv = dfv.copy()
    all_vendors_set.update([str(v).strip() for v in dfv["vendedor"].dropna().tolist() if str(v).strip()])
    if "data" in dfv.columns:
        mask = dfv["data"].dt.year == year
        if selected_quarter is not None:
            q_start = (selected_quarter - 1) * 3 + 1
            q_end = q_start + 2
            mask &= dfv["data"].dt.month.between(q_start, q_end)
        elif effective_ytd:
            mask &= dfv["data"].dt.month <= today.month
        elif selected_month is not None:
            mask &= dfv["data"].dt.month == selected_month
        dfv = dfv[mask]
    if dfv.empty:
        continue
    if value_col in dfv.columns:
        dfv[value_col] = pd.to_numeric(dfv[value_col], errors="coerce").fillna(0)
        grouped = dfv.groupby("vendedor")[value_col].sum()
    else:
        grouped = dfv.groupby("vendedor").size().astype(float)
    for vend, val in grouped.items():
        vend_txt = str(vend).strip()
        if vend_txt:
            vendor_scores[vend_txt] = vendor_scores.get(vend_txt, 0.0) + float(val)

active_ranked = sorted(
    [v for v, score in vendor_scores.items() if score > 0],
    key=lambda v: (-vendor_scores[v], v),
)
inactive_ranked = sorted([v for v in all_vendors_set if v not in set(active_ranked)])

if show_inactive_vendors:
    vendors = ["TODOS"] + active_ranked + inactive_ranked
else:
    vendors = ["TODOS"] + active_ranked
    if len(vendors) == 1:
        vendors = ["TODOS"] + sorted(all_vendors_set)

sel_vendor = st.sidebar.selectbox("Vendedor", options=vendors, index=0)

page_options = [
    "Executive Cockpit",
    "Finance Control Tower",
    "Pipeline Manager",
    "Performance & Ritmo",
    "Insights & Alertas",
    "Metas Comerciais",
    "Auditoria",
]
if PUBLIC_REVIEW:
    page_options = [
        "Pipeline Manager",
        "Insights & Alertas",
    ]

page = st.sidebar.selectbox("Pagina", options=page_options)

if PROFILE == "gestor":
    acl = load_acl().get("gestor", {})
    title = acl.get("title") or "Clear Agro CRM - Gestor"
else:
    title = APP_TITLE
st.title(title)
if PUBLIC_REVIEW:
    st.info("Modo revisao publica ativo: acesso sem login, somente visualizacao e paginas CRM.")
    st.caption(f"Build: {APP_BUILD}")
period = period_label(year, selected_month, effective_ytd, selected_quarter)
st.caption(f"Periodo: {period}")

if PUBLIC_REVIEW and page == "Metas Comerciais":
    st.warning("Pagina indisponivel no modo de revisao publica.")
    st.stop()

# Apply vendor filter to metas/realizado
if sel_vendor != "TODOS":
    sel_vendor_key = _vendor_key(sel_vendor)
    if "metas" in sheets and "vendedor" in sheets["metas"].columns:
        vm = sheets["metas"]["vendedor"].map(_vendor_key)
        sheets["metas"] = sheets["metas"][vm == sel_vendor_key]
    if "realizado" in sheets and "vendedor" in sheets["realizado"].columns:
        vr = sheets["realizado"]["vendedor"].map(_vendor_key)
        sheets["realizado"] = sheets["realizado"][vr == sel_vendor_key]

# Page A - Executive Cockpit
if page == "Executive Cockpit":
    st.subheader("Executive Cockpit")
    crm_pipeline_view = apply_acl_codes(load_sales_pipeline_view(), vendor_col="sales_rep_code")
    crm_pipeline_view = filter_vendor_scope(crm_pipeline_view, sel_vendor, ["sales_rep_code", "sales_rep_name"])
    crm_pipeline_view = filter_pipeline_period(crm_pipeline_view, year, selected_month, effective_ytd, selected_quarter)
    kpis = compute_kpis(
        sheets,
        year,
        selected_month,
        effective_ytd,
        selected_quarter,
        pipeline_view=crm_pipeline_view,
    )
    if crm_pipeline_view.empty:
        warn_crm_backend("vw_sales_pipeline_summary", "pipeline comercial")

    # fallback meta from metas.db when base_unificada meta is missing
    meta_display = kpis.meta
    if meta_display == 0:
        mf = {"ano": year, "periodo_tipo": "MONTH"}
        if sel_vendor != "TODOS":
            mf["vendedor_id"] = sel_vendor
        dfm_all = list_metas(mf)
        if PROFILE == "gestor" and not dfm_all.empty:
            acl = load_acl().get("gestor", {})
            allow = _clean_list(acl.get("allow_vendedores", []))
            block = _clean_list(acl.get("block_vendedores", []))
            if allow:
                dfm_all = dfm_all[dfm_all["vendedor_id"].isin(allow)]
            elif block:
                dfm_all = dfm_all[~dfm_all["vendedor_id"].isin(block)]
        if not dfm_all.empty:
            dfm = dfm_all.copy()
            if selected_quarter is not None:
                if "quarter" in dfm.columns:
                    dfm = dfm[pd.to_numeric(dfm["quarter"], errors="coerce").fillna(0).astype(int) == int(selected_quarter)]
                elif "mes" in dfm.columns:
                    q_start = (selected_quarter - 1) * 3 + 1
                    q_end = q_start + 2
                    mes_num = pd.to_numeric(dfm["mes"], errors="coerce").fillna(0).astype(int)
                    dfm = dfm[mes_num.between(q_start, q_end)]
                meta_display = float(pd.to_numeric(dfm["meta_valor"], errors="coerce").fillna(0).sum())
            elif selected_month is not None and not effective_ytd:
                dfm = dfm[dfm["mes"] == selected_month]
                meta_display = float(pd.to_numeric(dfm["meta_valor"], errors="coerce").fillna(0).sum())
            else:
                # quando "TODOS", usar meta anual completa
                meta_display = float(pd.to_numeric(dfm_all["meta_valor"], errors="coerce").fillna(0).sum())

    gap_display = meta_display - kpis.realizado
    ating_display = (kpis.realizado / meta_display * 100) if meta_display else 0.0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Realizado", fmt_brl_abbrev(kpis.realizado))
    c2.metric("Meta", fmt_brl_abbrev(meta_display))
    c3.metric("Atingimento %", fmt_pct(ating_display))
    c4.metric("Gap (R$)", fmt_brl_abbrev(gap_display))
    c5.metric("Pipeline Ponderado", fmt_brl_abbrev(kpis.pipeline_ponderado) if kpis.pipeline_ponderado is not None else "-")
    c6.metric("% c/ Proximo Passo", fmt_pct(kpis.pct_proximo_passo) if kpis.pct_proximo_passo is not None else "-")

    series = meta_realizado_mensal(sheets, year)
    if not series.empty:
        st.subheader("Meta vs Realizado")
        if selected_month is None or effective_ytd:
            st.altair_chart(bar_meta_realizado(series), width="stretch")
        else:
            single = series[series["data"].dt.month == selected_month].copy()
            if not single.empty:
                st.altair_chart(bar_meta_realizado_single(single), width="stretch")
                last6 = sparkline_last_months(series, 6)
                if len(last6) >= 3:
                    st.caption("Ultimos 6 meses (realizado)")
                    sp = sparkline(last6)
                    st.altair_chart(sp, width="stretch")
                else:
                    total = last6["receita"].sum() if "receita" in last6.columns else 0
                    avg = last6["receita"].mean() if "receita" in last6.columns else 0
                    best = last6["receita"].max() if "receita" in last6.columns else 0
                    st.info(f"Ultimos 6 meses: Realizado total {fmt_brl_abbrev(total)} | Media {fmt_brl_abbrev(avg)} | Melhor mes {fmt_brl_abbrev(best)}")
            else:
                st.info("Pendencia: sem dados no mes selecionado.")
    else:
        st.info("Pendencia: faltam dados de metas ou realizado com data.")

    # So what
    st.subheader("So what?")
    bullets = []
    perf = vendedor_performance_period(sheets, year, selected_month, effective_ytd, selected_quarter)
    if not perf.empty:
        perf = perf.sort_values("gap", ascending=False)
        top_gap = perf.head(3)
        bullets.append("Top gaps vs meta: " + ", ".join(top_gap["vendedor"].tolist()))

        zero_real = perf[(perf["receita"] == 0) & (perf["meta"] > 0)]
        if not zero_real.empty:
            bullets.append("0 realizado (com meta): " + ", ".join(zero_real["vendedor"].head(5).tolist()))

        total = perf["receita"].sum()
        top5 = perf.sort_values("receita", ascending=False).head(5)
        if total > 0:
            share = top5["receita"].sum() / total * 100
            bullets.append(f"Concentracao top 5: {share:.0f}% do realizado")

    # Mes em risco
    if selected_month is not None and not effective_ytd and selected_quarter is None:
        today = pd.Timestamp.today()
        if today.year == year and today.month == selected_month:
            esperado = (today.day / today.days_in_month) * meta_display
            if meta_display > 0 and kpis.realizado < esperado * 0.8:
                bullets.append("Mes em risco: realizado abaixo do esperado")

    # Disciplina
    opps = sheets.get("oportunidades", pd.DataFrame())
    if not crm_pipeline_view.empty and "opportunities_without_next_step" in crm_pipeline_view.columns:
        sem_passo = int(numeric_column(crm_pipeline_view, "opportunities_without_next_step").sum())
        bullets.append(f"Disciplina: {sem_passo} oportunidades sem proximo passo")
    elif "data_proximo_passo" in opps.columns:
        sem_passo = opps[opps["data_proximo_passo"].isna()]
        bullets.append(f"Disciplina: {len(sem_passo)} oportunidades sem proximo passo")
    else:
        bullets.append("Pendencia: coluna data_proximo_passo ausente")

    while len(bullets) < 5:
        bullets.append("Pendencia: dados insuficientes para este insight")
    for b in bullets[:5]:
        st.write(f"- {b}")

# Page B - Pipeline Manager
if page == "Pipeline Manager":
    st.subheader("Pipeline Manager")
    pipeline_view = apply_acl_codes(load_sales_pipeline_view(), vendor_col="sales_rep_code")
    pipeline_view = filter_vendor_scope(pipeline_view, sel_vendor, ["sales_rep_code", "sales_rep_name"])
    pipeline_view = filter_pipeline_period(pipeline_view, year, selected_month, effective_ytd, selected_quarter)

    if not pipeline_view.empty:
        df = pipeline_view.copy()
        if "customer_state" in df.columns:
            estados = sorted([v for v in df["customer_state"].dropna().astype(str).unique().tolist() if v])
            estados_sel = st.sidebar.multiselect("UF pipeline", estados, key="pipe_estado")
            if estados_sel:
                df = df[df["customer_state"].isin(estados_sel)]
        if "stage" in df.columns:
            etapas = sorted([v for v in df["stage"].dropna().astype(str).unique().tolist() if v])
            etapas_sel = st.sidebar.multiselect("Etapa pipeline", etapas, key="pipe_etapa")
            if etapas_sel:
                df = df[df["stage"].isin(etapas_sel)]

        total_opps = int(numeric_column(df, "opportunities_count").sum())
        pipeline_total = float(numeric_column(df, "pipeline_value").sum())
        weighted_total = float(numeric_column(df, "weighted_pipeline_value").sum())
        sem_passo = int(numeric_column(df, "opportunities_without_next_step").sum())
        overdue = int(numeric_column(df, "opportunities_with_overdue_step").sum())

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Oportunidades", total_opps)
        c2.metric("Pipeline", fmt_brl_abbrev(pipeline_total))
        c3.metric("Pipeline ponderado", fmt_brl_abbrev(weighted_total))
        c4.metric("Sem proximo passo", sem_passo)
        c5.metric("Passos vencidos", overdue)

        if "stage" in df.columns and not df.empty:
            stage_df = (
                df.groupby("stage", dropna=False)[["pipeline_value", "weighted_pipeline_value"]]
                .sum()
                .reset_index()
                .fillna(0)
            )
            stage_long = stage_df.melt(id_vars=["stage"], var_name="tipo", value_name="valor")
            chart = (
                alt.Chart(stage_long)
                .mark_bar()
                .encode(
                    x=alt.X("stage:N", title="Etapa"),
                    y=alt.Y("valor:Q", title="Valor"),
                    color=alt.Color("tipo:N", title="Serie"),
                    xOffset="tipo:N",
                    tooltip=["stage", "tipo", "valor"],
                )
            )
            st.altair_chart(chart, width="stretch")

        view_cols = [
            "sales_rep_name",
            "sales_rep_code",
            "customer_state",
            "stage",
            "expected_close_month",
            "opportunities_count",
            "pipeline_value",
            "weighted_pipeline_value",
            "avg_probability",
            "opportunities_without_next_step",
            "opportunities_with_overdue_step",
            "last_opportunity_update",
        ]
        view = df[[c for c in view_cols if c in df.columns]].copy()
        if "expected_close_month" in view.columns:
            view = sort_by_available(view, [("expected_close_month", True), ("weighted_pipeline_value", False)])
        st.dataframe(view, height=420, width="stretch")

        out = BytesIO()
        view.to_excel(out, index=False, sheet_name="pipeline")
        st.download_button("Exportar Pipeline", data=out.getvalue(), file_name="pipeline_manager.xlsx")

        queue_df = apply_acl_codes(load_crm_priority_queue(), vendor_col="sales_rep_code")
        queue_df = filter_vendor_scope(queue_df, sel_vendor, ["sales_rep_code", "sales_rep_name"])
        queue_df = filter_queue_period(queue_df, year, selected_month, effective_ytd, selected_quarter)
        if not queue_df.empty:
            queue_df = sort_by_available(queue_df, [("priority_score", False), ("due_at", True)])
            st.caption("Fila prioritaria de agentes e atividades")
            st.dataframe(
                queue_df[[c for c in ["queue_source", "severity", "title", "customer_name", "sales_rep_name", "due_at", "priority_score"] if c in queue_df.columns]].head(20),
                height=260,
                width="stretch",
            )
        else:
            warn_crm_backend("vw_crm_agent_priority_queue", "fila prioritaria do CRM")
    else:
        warn_crm_backend("vw_sales_pipeline_summary", "pipeline comercial")
        opps = sheets.get("oportunidades", pd.DataFrame())
        if opps.empty:
            st.info("Pendencia: aba oportunidades vazia")
        else:
            df = opps.copy()
            if "volume_potencial" in df.columns:
                df["valor"] = df["volume_potencial"]
            if "probabilidade" in df.columns:
                df["prob"] = pd.to_numeric(df["probabilidade"], errors="coerce")
            if "data_proximo_passo" in df.columns:
                df["proximo_passo"] = df["data_proximo_passo"]
            df["oportunidade"] = df.get("oportunidade", df.get("cliente", ""))

            if "canal" in df.columns:
                canal = st.sidebar.multiselect("Canal", sorted(df["canal"].dropna().unique().tolist()))
                if canal:
                    df = df[df["canal"].isin(canal)]
            if "etapa" in df.columns:
                etapa = st.sidebar.multiselect("Etapa", sorted(df["etapa"].dropna().unique().tolist()))
                if etapa:
                    df = df[df["etapa"].isin(etapa)]

            df["alerta"] = ""
            if "proximo_passo" in df.columns:
                df.loc[df["proximo_passo"].isna(), "alerta"] = "Sem proximo passo"
            if "valor" in df.columns and "prob" in df.columns:
                df["score"] = df["valor"] * (df["prob"].fillna(0) / 100)
            elif "valor" in df.columns:
                df["score"] = df["valor"]
            else:
                df["score"] = None

            cols = ["cliente", "oportunidade", "etapa", "valor", "prob", "proximo_passo", "alerta", "score", "vendedor"]
            view = df[[c for c in cols if c in df.columns]].copy()
            st.dataframe(view, height=420)

            out = BytesIO()
            view.to_excel(out, index=False, sheet_name="prioridades")
            st.download_button("Exportar Prioridades da Semana", data=out.getvalue(), file_name="prioridades_semana.xlsx")

# Page C - Performance & Ritmo
if page == "Performance & Ritmo":
    st.subheader("Performance & Ritmo")
    perf_pipeline_view = apply_acl_codes(load_sales_pipeline_view(), vendor_col="sales_rep_code")
    perf_pipeline_view = filter_vendor_scope(perf_pipeline_view, sel_vendor, ["sales_rep_code", "sales_rep_name"])
    perf_pipeline_view = filter_pipeline_period(perf_pipeline_view, year, selected_month, effective_ytd, selected_quarter)
    perf_kpis = compute_kpis(
        sheets,
        year,
        selected_month,
        effective_ytd,
        selected_quarter,
        pipeline_view=perf_pipeline_view,
    )
    quarter_suffix = f"Q{selected_quarter}" if selected_quarter is not None else period
    q1, q2, q3 = st.columns(3)
    q1.metric(f"Meta ({quarter_suffix})", fmt_brl_abbrev(perf_kpis.meta))
    q2.metric(f"Realizado ({quarter_suffix})", fmt_brl_abbrev(perf_kpis.realizado))
    q3.metric(f"Saldo a Faturar ({quarter_suffix})", fmt_brl_abbrev(perf_kpis.gap))

    perf = vendedor_performance_period(sheets, year, selected_month, effective_ytd, selected_quarter)
    if perf.empty:
        st.info("Pendencia: metas/realizado por vendedor nao disponivel")
    else:
        perf = perf.sort_values("gap", ascending=False)
        perf["rank"] = range(1, len(perf) + 1)
        perf_disp = perf.copy()
        perf_disp["meta"] = perf_disp["meta"].apply(fmt_brl_abbrev)
        perf_disp["receita"] = perf_disp["receita"].apply(fmt_brl_abbrev)
        perf_disp["gap"] = perf_disp["gap"].apply(fmt_brl_abbrev)
        perf_disp["atingimento_pct"] = perf_disp["atingimento_pct"].apply(fmt_pct)
        perf_disp = perf_disp[["vendedor", "meta", "receita", "atingimento_pct", "gap", "rank"]]
        topn = st.slider("Top N", min_value=5, max_value=30, value=15)
        st.dataframe(
            perf_disp.head(topn),
            height=420,
            column_config={
                "vendedor": st.column_config.TextColumn(width="large"),
                "meta": st.column_config.TextColumn(width="small"),
                "receita": st.column_config.TextColumn(width="small"),
                "atingimento_pct": st.column_config.TextColumn(width="small"),
                "gap": st.column_config.TextColumn(width="small"),
                "rank": st.column_config.NumberColumn(width="small"),
            },
        )

    acts = sheets.get("atividades", pd.DataFrame())
    if acts.empty:
        st.info("Pendencia: sem dados de atividades")

# Page D - Insights & Alertas
if page == "Insights & Alertas":
    st.subheader("Insights & Alertas")
    queue_df = apply_acl_codes(load_crm_priority_queue(), vendor_col="sales_rep_code")
    queue_df = filter_vendor_scope(queue_df, sel_vendor, ["sales_rep_code", "sales_rep_name"])
    queue_df = filter_queue_period(queue_df, year, selected_month, effective_ytd, selected_quarter)

    if not queue_df.empty:
        df = queue_df.copy()
        total_queue = len(df)
        open_findings = int((df.get("queue_source", pd.Series([], dtype=str)) == "finding").sum())
        open_activities = int((df.get("queue_source", pd.Series([], dtype=str)) == "activity").sum())
        critical_count = int(df.get("severity", pd.Series([], dtype=str)).astype(str).str.lower().eq("critical").sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Fila aberta", total_queue)
        c2.metric("Findings", open_findings)
        c3.metric("Atividades", open_activities)
        c4.metric("Criticos", critical_count)

        if "severity" in df.columns:
            sev_df = df["severity"].astype(str).str.upper().value_counts().rename_axis("severity").reset_index(name="quantidade")
            sev_chart = alt.Chart(sev_df).mark_bar().encode(
                x=alt.X("severity:N", title="Severidade"),
                y=alt.Y("quantidade:Q", title="Itens"),
                tooltip=["severity", "quantidade"],
            )
            st.altair_chart(sev_chart, width="stretch")

        display_cols = [
            "queue_source",
            "severity",
            "status",
            "title",
            "customer_name",
            "opportunity_title",
            "sales_rep_name",
            "due_at",
            "priority_score",
        ]
        st.dataframe(
            sort_by_available(df, [("priority_score", False), ("due_at", True)])[[c for c in display_cols if c in df.columns]],
            height=420,
            width="stretch",
        )
    else:
        warn_crm_backend("vw_crm_agent_priority_queue", "fila prioritaria do CRM")
        opps = sheets.get("oportunidades", pd.DataFrame())
        alerts = []
        if "data_proximo_passo" in opps.columns:
            sem_passo = opps[opps["data_proximo_passo"].isna()]
            alerts.append(("Sem proximo passo", len(sem_passo)))
        else:
            alerts.append(("Sem proximo passo", "pendente"))

        for title, val in alerts:
            st.metric(title, val)

# Page G - Finance Control Tower
if page == "Finance Control Tower":
    st.subheader("Finance Control Tower")
    st.caption("Visao unificada de DRE, Caixa, AP/AR e Reconciliaçao.")

    exports_dir = ROOT / "data" / "exports"
    marts_dir = ROOT / "data" / "marts"

    def _read_csv(path: Path) -> pd.DataFrame:
        try:
            if path.exists():
                return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
        return pd.DataFrame()

    df_exec = _read_csv(exports_dir / "finance_executive_summary.csv")
    df_kpis = _read_csv(exports_dir / "finance_kpis_monthly.csv")
    df_exc = _read_csv(exports_dir / "finance_reconciliation_exceptions.csv")
    df_ap_ar = _read_csv(marts_dir / "fact_ap_ar.csv")
    df_cash = _read_csv(marts_dir / "fact_cashflow_detailed.csv")
    df_dre = _read_csv(marts_dir / "fact_dre_finance.csv")

    tabs = st.tabs(["Executive", "KPIs Mensais", "Reconciliacao", "Detalhes"])

    with tabs[0]:
        if df_exec.empty:
            st.warning("Arquivo finance_executive_summary.csv nao encontrado. Rode: python src/reports/build_finance_pack.py")
        else:
            show = df_exec.copy()
            show["value_num"] = pd.to_numeric(show["value"], errors="coerce")
            m = {r["metric"]: r["value"] for _, r in show.iterrows()}

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Periodo Caixa", str(m.get("Periodo ultimo caixa", "-")))
            c2.metric("Periodo AP", str(m.get("Periodo ultimo AP", "-")))
            c3.metric("AP Realizado (Caixa)", fmt_brl_abbrev(float(pd.to_numeric(m.get("AP realizado (Caixa) - ultimo caixa", 0), errors="coerce") or 0)))
            c4.metric("AP Total (Ultimo AP)", fmt_brl_abbrev(float(pd.to_numeric(m.get("AP total - ultimo AP", 0), errors="coerce") or 0)))

            st.dataframe(show[["metric", "value"]], width="stretch", height=360)

    with tabs[1]:
        if df_kpis.empty:
            st.info("Sem KPIs mensais ainda.")
        else:
            plot = df_kpis.copy()
            plot["periodo"] = plot["ano"].astype(int).astype(str) + "-" + plot["mes_num"].astype(int).astype(str).str.zfill(2)
            num_cols = [
                c
                for c in [
                    "dre_receita",
                    "dre_despesa",
                    "cash_in",
                    "cash_out",
                    "ap_total",
                    "ap_realizado",
                    "ap_realizado_caixa",
                    "ar_total",
                    "ebitda_proxy",
                    "fcf_proxy",
                    "capital_giro_proxy",
                ]
                if c in plot.columns
            ]
            for c in num_cols:
                plot[c] = pd.to_numeric(plot[c], errors="coerce").fillna(0)

            kpi_sel = st.multiselect(
                "Series para visualizar",
                options=num_cols,
                default=[c for c in ["cash_in", "cash_out", "ap_total", "ap_realizado_caixa", "fcf_proxy"] if c in num_cols],
            )
            if kpi_sel:
                series = plot[["periodo"] + kpi_sel].copy()
                long_df = series.melt(id_vars=["periodo"], var_name="kpi", value_name="valor")
                chart = (
                    alt.Chart(long_df)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("periodo:O", title="Periodo"),
                        y=alt.Y("valor:Q", title="Valor"),
                        color=alt.Color("kpi:N", title="KPI"),
                        tooltip=["periodo", "kpi", "valor"],
                    )
                )
                st.altair_chart(chart, width="stretch")

            st.dataframe(plot, width="stretch", height=360)

    with tabs[2]:
        if df_exc.empty:
            st.success("Sem excecoes materiais no recorte atual.")
        else:
            if "exception_material" in df_exc.columns:
                exc_only = df_exc[df_exc["exception_material"] == True]  # noqa: E712
            else:
                exc_only = df_exc
            st.metric("Excecoes materiais", len(exc_only))
            st.dataframe(exc_only, width="stretch", height=360)

    with tabs[3]:
        col1, col2 = st.columns(2)
        with col1:
            st.caption("AP/AR (Bling)")
            if df_ap_ar.empty:
                st.info("fact_ap_ar.csv nao encontrado.")
            else:
                st.dataframe(df_ap_ar, width="stretch", height=280)
        with col2:
            st.caption("Caixa detalhado (Bancos)")
            if df_cash.empty:
                st.info("fact_cashflow_detailed.csv nao encontrado.")
            else:
                st.dataframe(df_cash, width="stretch", height=280)

        st.caption("DRE Finance (CR/CZ/EMPRESA)")
        if df_dre.empty:
            st.info("fact_dre_finance.csv nao encontrado.")
        else:
            st.dataframe(df_dre, width="stretch", height=280)

# Page F - Auditoria
if page == "Auditoria":
    st.subheader("Auditoria: Planilha vs Bling (NFe)")
    st.write("Comparativo mensal entre realizado da planilha e faturamento NFe do Bling.")

    # Planilha (realizado)
    real = sheets.get("realizado", pd.DataFrame()).copy()
    if "data" not in real.columns:
        real["data"] = pd.NaT
    if "receita" not in real.columns:
        real["receita"] = 0.0
    if "data" in real.columns:
        real["data"] = pd.to_datetime(real["data"], errors="coerce")
    if "receita" in real.columns:
        real["receita"] = pd.to_numeric(real["receita"], errors="coerce")
    real = real.dropna(subset=["data"])
    real = real[real["data"].dt.year == year]
    real_m = real.groupby(real["data"].dt.to_period("M"))["receita"].sum().reset_index()
    real_m["data"] = real_m["data"].dt.to_timestamp()

    # Bling NFe
    nfe = load_bling_nfe(year)
    if nfe.empty:
        if not PUBLIC_REVIEW:
            st.warning("Cache NFe do Bling nao encontrado. Exibindo valores do Bling como zero para revisao.")
        nfe_m = real_m[["data"]].copy() if not real_m.empty else pd.DataFrame(columns=["data"])
        nfe_m["valor"] = 0.0
    else:
        nfe = nfe.copy()
        nfe = nfe[nfe["data"].dt.year == year]
        nfe_m = nfe.groupby(nfe["data"].dt.to_period("M"))["valor"].sum().reset_index()
        nfe_m["data"] = nfe_m["data"].dt.to_timestamp()

    # Merge
    df = pd.merge(real_m, nfe_m, on="data", how="outer").fillna(0)
    df["delta"] = df["receita"] - df["valor"]
    df["delta_pct"] = df.apply(lambda r: (r["delta"] / r["valor"] * 100) if r["valor"] else 0, axis=1)
    df = df.sort_values("data")
    df["mes"] = df["data"].dt.strftime("%b/%Y").str.upper()

    st.bar_chart(df.set_index("mes")[["receita", "valor"]])
    st.dataframe(
        df[["mes", "receita", "valor", "delta", "delta_pct"]],
        height=420,
    )
    audit_alerts = [("Delta total (Planilha - NFe)", float(df["delta"].sum()))]

    st.divider()
    st.subheader("Financeiro Bling: Contas a Receber/Pagar")
    contas_r = load_bling_contas("receber")
    contas_p = load_bling_contas("pagar")
    total_r = float(contas_r["valor"].fillna(0).sum()) if not contas_r.empty else 0.0
    total_p = float(contas_p["valor"].fillna(0).sum()) if not contas_p.empty else 0.0
    saldo_fin = total_r - total_p
    c1, c2, c3 = st.columns(3)
    c1.metric("Contas a Receber", fmt_brl_abbrev(total_r))
    c2.metric("Contas a Pagar", fmt_brl_abbrev(total_p))
    c3.metric("Saldo Financeiro", fmt_brl_abbrev(saldo_fin))

    colr, colp = st.columns(2)
    with colr:
        st.caption("Receber (top 20)")
        if contas_r.empty:
            st.info("Cache contas_receber_cache.jsonl nao encontrado.")
        else:
            st.dataframe(contas_r.sort_values("valor", ascending=False).head(20), height=300)
    with colp:
        st.caption("Pagar (top 20)")
        if contas_p.empty:
            st.info("Cache contas_pagar_cache.jsonl nao encontrado.")
        else:
            st.dataframe(contas_p.sort_values("valor", ascending=False).head(20), height=300)

    st.divider()
    st.subheader("Estoque Bling")
    est = load_bling_estoque()
    if est.empty:
        st.info("Cache estoque_cache.jsonl nao encontrado.")
    else:
        ce1, ce2 = st.columns(2)
        ce1.metric("Itens em estoque", f"{len(est):,}".replace(",", "."))
        ce2.metric("Saldo total", f"{est['saldo'].sum():,.0f}".replace(",", "."))
        st.dataframe(est.sort_values("saldo", ascending=False).head(50), height=340)

    st.divider()
    if PUBLIC_REVIEW:
        st.caption("Modo revisao publica: envio de alertas desativado (somente visualizacao).")
    else:
        st.caption("Telegram (opcional): configure TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID para enviar alertas.")
        if st.button("Enviar alertas para Telegram", key="send_telegram_alerts"):
            if not telegram_enabled():
                st.error("Telegram nao configurado. Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID no ambiente.")
            else:
                msg = build_alerts_message(APP_TITLE + " - Auditoria", period, audit_alerts)
                ok, detail = send_telegram_message(msg)
                if ok:
                    st.success(detail)
                else:
                    st.error(detail)

# Page E - Metas Comerciais
if page == "Metas Comerciais":
    st.subheader("Metas Comerciais")
    tabs = st.tabs(["Executive Summary", "Metas", "Cadastro", "Transferencia"])
    targets_view_all = apply_acl_codes(load_sales_targets_view(), vendor_col="sales_rep_code")
    targets_view_all = filter_vendor_scope(targets_view_all, sel_vendor, ["sales_rep_code", "sales_rep_name"])
    periodo_tipo = "MONTH"
    uf = ""
    vend = ""
    status = []
    mes = None
    quarter = None

    with tabs[0]:
        st.write("Resumo executivo das metas por UF, vendedor e periodo.")
        colf1, colf2, colf3, colf4, colf5 = st.columns(5)
        periodo_tipo = colf1.selectbox("Periodo", ["MONTH", "QUARTER"], key="metas_periodo_tipo")

        if not targets_view_all.empty:
            all_targets_year = filter_targets_view(targets_view_all, year, periodo_tipo)
            uf_opts = [""] + sorted([v for v in all_targets_year.get("state", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if v])
            vend_opts = [""] + sorted([v for v in all_targets_year.get("sales_rep_code", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if v])
        else:
            all_metas = list_metas({"ano": year})
            if PROFILE == "gestor" and not all_metas.empty:
                acl = load_acl().get("gestor", {})
                allow = _clean_list(acl.get("allow_vendedores", []))
                block = _clean_list(acl.get("block_vendedores", []))
                if allow:
                    all_metas = all_metas[all_metas["vendedor_id"].isin(allow)]
                elif block:
                    all_metas = all_metas[~all_metas["vendedor_id"].isin(block)]
            uf_opts = [""] + sorted(all_metas["estado"].dropna().unique().tolist()) if not all_metas.empty else [""]
            vend_opts = [""] + sorted(all_metas["vendedor_id"].dropna().unique().tolist()) if not all_metas.empty else [""]

        uf = colf2.selectbox("UF (opcional)", options=uf_opts, key="metas_uf")
        vend = colf3.selectbox("Vendedor ID (opcional)", options=vend_opts, key="metas_vendedor")
        status = colf4.multiselect("Status", ["ATIVO", "PAUSADO", "DESLIGADO", "TRANSFERIDO"], key="metas_status")
        if colf5.button("Criar dados demo", key="metas_seed"):
            seed_demo()
            load_sales_targets_view.clear()
            st.success("Dados demo criados.")

        if periodo_tipo == "MONTH":
            mes = st.selectbox("Mes", [""] + list(range(1, 13)), key="metas_mes")
            quarter = None
        else:
            quarter = st.selectbox("Quarter", [""] + [1, 2, 3, 4], key="metas_quarter")
            mes = None

        if not targets_view_all.empty:
            filtered_view = filter_targets_view(
                targets_view_all,
                year,
                periodo_tipo,
                month_num=mes or None,
                quarter_num=quarter or None,
                state=uf or None,
                sales_rep_code=vend or None,
                statuses=status or None,
            )
            res = build_targets_summary(filtered_view, periodo_tipo)
            k = res["kpis"]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Meta", fmt_brl_abbrev(k.get("meta", 0)))
            c2.metric("Realizado", fmt_brl_abbrev(k.get("realizado", 0)))
            c3.metric("Atingimento %", fmt_pct(k.get("atingimento_pct", 0)))
            c4.metric("Delta", fmt_brl_abbrev(k.get("delta", 0)))

            if not res["series"].empty:
                ser = res["series"].rename(columns={"meta_valor": "meta", "realizado_valor": "receita"}).copy()
                if periodo_tipo == "QUARTER":
                    ser["periodo"] = ser.get("quarter")
                else:
                    ser["periodo"] = ser.get("mes")
                line = alt.Chart(ser).transform_fold(
                    ["meta", "receita"], as_=["tipo", "valor"]
                ).mark_line(point=True).encode(
                    x=alt.X("periodo:O", title="Periodo"),
                    y=alt.Y("valor:Q", title="Valor"),
                    color=alt.Color("tipo:N", title=""),
                    tooltip=["periodo:O", "tipo:N", "valor:Q"],
                )
                st.altair_chart(line, width="stretch")

            if not res["uf"].empty:
                uf_df = res["uf"].copy()
                uf_df["ating"] = (uf_df["realizado_valor"] / uf_df["meta_valor"] * 100).fillna(0)
                st.write("Atingimento por UF")
                st.bar_chart(uf_df.set_index("estado")[["meta_valor", "realizado_valor"]])
                uf_df["delta"] = uf_df["realizado_valor"] - uf_df["meta_valor"]
                st.write("Delta por UF")
                st.bar_chart(uf_df.set_index("estado")[["delta"]])

            if not filtered_view.empty:
                heat = filtered_view.copy()
                heat["periodo"] = heat["quarter_num"] if periodo_tipo == "QUARTER" else heat["month_num"]
                heat_src = (
                    heat.groupby(["state", "periodo"], dropna=False)["actual_value"]
                    .sum()
                    .reset_index()
                    .rename(columns={"state": "estado", "actual_value": "realizado"})
                )
                st.write("Heatmap UF x periodo")
                hm = alt.Chart(heat_src).mark_rect().encode(
                    x=alt.X("periodo:O", title="Periodo"),
                    y=alt.Y("estado:N", title="UF"),
                    color=alt.Color("realizado:Q", title="Realizado"),
                    tooltip=["estado", "periodo", "realizado"],
                )
                st.altair_chart(hm, width="stretch")
            else:
                st.info("Sem metas na view CRM para os filtros selecionados.")
        else:
            filtros = {
                "ano": year,
                "periodo_tipo": periodo_tipo,
                "mes": mes or None,
                "quarter": quarter or None,
                "estado": uf or None,
                "vendedor_id": vend or None,
                "status": status or None,
            }
            if PROFILE == "gestor":
                acl = load_acl().get("gestor", {})
                allow = _clean_list(acl.get("allow_vendedores", []))
                block = _clean_list(acl.get("block_vendedores", []))
                if allow:
                    filtros["vendedor_id"] = allow
                elif block:
                    filtros["vendedor_id"] = [v for v in vend_opts if v and v not in block]
            res = summary_targets(filtros)
            k = res["kpis"]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Meta", fmt_brl_abbrev(k.get("meta", 0)))
            c2.metric("Realizado", fmt_brl_abbrev(k.get("realizado", 0)))
            c3.metric("Atingimento %", fmt_pct(k.get("atingimento_pct", 0)))
            c4.metric("Delta", fmt_brl_abbrev(k.get("delta", 0)))

            if not res["series"].empty:
                ser = res["series"].rename(columns={"meta_valor": "meta", "realizado_valor": "receita"}).copy()
                if periodo_tipo == "QUARTER":
                    if "quarter" in ser.columns:
                        ser["periodo"] = ser["quarter"]
                    else:
                        ser["periodo"] = ser["mes"].apply(lambda m: ((int(m) - 1) // 3 + 1) if pd.notna(m) else None)
                else:
                    ser["periodo"] = ser["mes"]
                line = alt.Chart(ser).transform_fold(
                    ["meta", "receita"], as_=["tipo", "valor"]
                ).mark_line(point=True).encode(
                    x=alt.X("periodo:O", title="Periodo"),
                    y=alt.Y("valor:Q", title="Valor"),
                    color=alt.Color("tipo:N", title=""),
                    tooltip=["periodo:O", "tipo:N", "valor:Q"],
                )
                st.altair_chart(line, width="stretch")

            if not res["uf"].empty:
                uf_df = res["uf"].copy()
                uf_df["ating"] = (uf_df["realizado_valor"] / uf_df["meta_valor"] * 100).fillna(0)
                st.write("Atingimento por UF")
                st.bar_chart(uf_df.set_index("estado")[["meta_valor", "realizado_valor"]])
                uf_df["delta"] = uf_df["realizado_valor"] - uf_df["meta_valor"]
                st.write("Delta por UF")
                st.bar_chart(uf_df.set_index("estado")[["delta"]])

            dfm = list_metas(filtros)
            if PROFILE == "gestor" and not dfm.empty:
                acl = load_acl().get("gestor", {})
                allow = _clean_list(acl.get("allow_vendedores", []))
                block = _clean_list(acl.get("block_vendedores", []))
                if allow:
                    dfm = dfm[dfm["vendedor_id"].isin(allow)]
                elif block:
                    dfm = dfm[~dfm["vendedor_id"].isin(block)]
            if not dfm.empty:
                if periodo_tipo == "MONTH":
                    dfm["periodo"] = dfm["mes"]
                else:
                    dfm["periodo"] = dfm["quarter"]
                pivot = dfm.pivot_table(index="estado", columns="periodo", values="realizado_valor", aggfunc="sum", fill_value=0)
                heat = pivot.reset_index().melt(id_vars=["estado"], var_name="periodo", value_name="realizado")
                st.write("Heatmap UF x periodo")
                hm = alt.Chart(heat).mark_rect().encode(
                    x=alt.X("periodo:O", title="Periodo"),
                    y=alt.Y("estado:N", title="UF"),
                    color=alt.Color("realizado:Q", title="Realizado"),
                    tooltip=["estado", "periodo", "realizado"],
                )
                st.altair_chart(hm, width="stretch")

    with tabs[1]:
        st.write("Listagem de metas")
        if not targets_view_all.empty:
            df = filter_targets_view(
                targets_view_all,
                year,
                periodo_tipo,
                month_num=mes or None,
                quarter_num=quarter or None,
                state=uf or None,
                sales_rep_code=vend or None,
                statuses=status or None,
            )
            if not df.empty:
                df = df.rename(
                    columns={
                        "target_year": "ano",
                        "period_type": "periodo_tipo",
                        "month_num": "mes",
                        "quarter_num": "quarter",
                        "state": "estado",
                        "sales_rep_code": "vendedor_id",
                        "sales_rep_name": "vendedor",
                        "target_value": "meta_valor",
                        "actual_value": "realizado_valor",
                        "attainment_pct": "atingimento_pct",
                        "gap_value": "gap_valor",
                    }
                )
                if "status" in df.columns:
                    df["status"] = df["status"].apply(status_chip)
            st.dataframe(df, height=420, width="stretch")
        else:
            df = list_metas({"ano": year})
            if PROFILE == "gestor" and not df.empty:
                acl = load_acl().get("gestor", {})
                allow = _clean_list(acl.get("allow_vendedores", []))
                block = _clean_list(acl.get("block_vendedores", []))
                if allow:
                    df = df[df["vendedor_id"].isin(allow)]
                elif block:
                    df = df[~df["vendedor_id"].isin(block)]
            if not df.empty and "status" in df.columns:
                df = df.copy()
                df["status"] = df["status"].apply(status_chip)
            st.dataframe(df, height=420)

    with tabs[2]:
        st.write("Cadastrar nova meta")
        step = st.radio("Etapa", ["Periodo", "Segmentacao", "Valores", "Revisao"], horizontal=True, key="meta_step")

        if "meta_form" not in st.session_state:
            st.session_state["meta_form"] = {}

        mf = st.session_state["meta_form"]

        if step == "Periodo":
            mf["periodo_tipo"] = st.selectbox("Periodo", ["MONTH", "QUARTER"], index=0, key="meta_periodo_tipo")
            mf["mes"] = st.selectbox("Mes", list(range(1, 13)), key="meta_mes") if mf["periodo_tipo"] == "MONTH" else None
            mf["quarter"] = st.selectbox("Quarter", [1,2,3,4], key="meta_quarter") if mf["periodo_tipo"] == "QUARTER" else None

        if step == "Segmentacao":
            mf["estado"] = st.text_input("UF (ex: PR, RS)", key="meta_uf")
            mf["vendedor_id"] = st.text_input("Vendedor ID", key="meta_vendedor")
            mf["canal"] = st.text_input("Canal (opcional)", key="meta_canal")
            mf["cultura"] = st.text_input("Cultura (opcional)", key="meta_cultura")

        if step == "Valores":
            mf["meta_valor"] = st.number_input("Meta (R$)", min_value=0.0, step=1000.0, key="meta_valor")
            mf["meta_volume"] = st.number_input("Meta Volume (opcional)", min_value=0.0, step=1.0, key="meta_volume")
            mf["status"] = st.selectbox("Status", ["ATIVO","PAUSADO","DESLIGADO","TRANSFERIDO"], key="meta_status")

        if step == "Revisao":
            st.write("Revise os dados antes de salvar.")
            st.json(mf)
            if st.button("Salvar"):
                # validacoes simples
                if mf.get("periodo_tipo") == "MONTH" and not mf.get("mes"):
                    st.error("Mes obrigatorio para MONTH.")
                elif mf.get("periodo_tipo") == "QUARTER" and not mf.get("quarter"):
                    st.error("Quarter obrigatorio para QUARTER.")
                elif not mf.get("estado") or len(mf.get("estado","")) != 2:
                    st.error("UF obrigatoria (2 letras).")
                elif mf.get("meta_valor") is None:
                    st.error("Meta obrigatoria.")
                else:
                    create_meta({
                        "ano": year,
                        "periodo_tipo": mf.get("periodo_tipo"),
                        "mes": mf.get("mes"),
                        "quarter": mf.get("quarter"),
                        "estado": mf.get("estado"),
                        "vendedor_id": mf.get("vendedor_id"),
                        "canal": mf.get("canal"),
                        "cultura": mf.get("cultura"),
                        "meta_valor": mf.get("meta_valor"),
                        "meta_volume": mf.get("meta_volume"),
                        "realizado_valor": None,
                        "realizado_volume": None,
                        "status": mf.get("status") or "ATIVO",
                        "observacoes": None,
                    }, actor_id="ui")
                    load_sales_targets_view.clear()
                    st.success("Meta criada.")

    with tabs[3]:
        st.write("Transferencia de ativos/metas")
        col1, col2 = st.columns(2)
        with col1:
            origem = st.text_input("Vendedor origem")
        with col2:
            destino = st.text_input("Vendedor destino")
        if st.button("Transferir ativos"):
            transfer_assets(origem, destino, actor_id="ui")
            st.success("Ativos transferidos")
        if st.button("Transferir metas futuras"):
            transfer_metas_futuras(origem, destino, actor_id="ui")
            load_sales_targets_view.clear()
            st.success("Metas transferidas")
