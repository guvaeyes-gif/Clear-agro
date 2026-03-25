import os
import json
import base64
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
    load_bling_vendor_map,
    load_vendor_links,
    load_bling_sales_detail,
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
APP_TITLE = "CRM"
ACL_PATH = ROOT / "data" / "access_control.json"
DEFAULT_YEAR = 2026
LOGO_CANDIDATES = [
    ROOT / "data" / "CLEAR.png",
    Path(r"C:\Users\admin\OneDrive\Imágenes\LOGOS\Logo clear agro-2025.pdf"),
    ROOT / "data" / "logo_empresa.png",
    ROOT / "data" / "logo_empresa.jpg",
    ROOT / "data" / "logo_empresa.jpeg",
    ROOT / "data" / "logo_empresa.svg",
    ROOT / "app" / "logo_empresa.png",
    ROOT / "app" / "logo_empresa.jpg",
    ROOT / "app" / "logo_empresa.jpeg",
    ROOT / "app" / "logo_empresa.svg",
]
BLING_REALIZADO_CACHES = [
    ROOT / "bling_api" / "nfe_2026_cache.jsonl",
    ROOT / "bling_api" / "nfe_2026_cache_cr.jsonl",
    ROOT / "bling_api" / "nfe_2025_cache.jsonl",
    ROOT / "bling_api" / "nfe_2025_cache_cr.jsonl",
    ROOT / "bling_api" / "vendas_2026_cache.jsonl",
    ROOT / "bling_api" / "vendas_2026_cache_cr.jsonl",
    ROOT / "bling_api" / "vendas_2025_cache.jsonl",
    ROOT / "bling_api" / "vendas_2025_cache_cr.jsonl",
]

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


def fmt_brl_full(value: object) -> str:
    try:
        num = float(value)
    except Exception:
        return "-"
    return f"R$ {num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_int_br(value: object) -> str:
    try:
        num = int(float(value))
    except Exception:
        return "-"
    return f"{num:,}".replace(",", ".")


def fmt_brl_table(value: object) -> str:
    try:
        num = float(value)
    except Exception:
        return "-"
    return f"R$ {num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _upper_dashboard_value(value: object) -> object:
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return value
        return stripped.upper()
    return value


def upper_dashboard_text(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    skip_keywords = {"id", "code", "cpf", "cnpj", "email", "url", "fone", "phone", "numero"}
    text_cols = []
    for col in out.columns:
        col_key = str(col).strip().lower()
        if any(token in col_key for token in skip_keywords):
            continue
        if pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col]):
            text_cols.append(col)
    for col in text_cols:
        out[col] = out[col].map(_upper_dashboard_value)
    return out


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


def find_dashboard_logo() -> Path | None:
    env_logo = os.getenv("CRM_LOGO_PATH", "").strip()
    if env_logo:
        logo_path = Path(env_logo)
        if not logo_path.is_absolute():
            logo_path = ROOT / logo_path
        if logo_path.exists():
            return logo_path
    for path in LOGO_CANDIDATES:
        if path.exists():
            return path
    return None


def render_dashboard_logo(path: Path, width: int = 180) -> None:
    suffix = path.suffix.lower()
    if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
        st.image(str(path), width=width)
        return
    if suffix == ".svg":
        svg = path.read_text(encoding="utf-8")
        st.markdown(
            f"<div style='max-width:{width}px'>{svg}</div>",
            unsafe_allow_html=True,
        )
        return
    if suffix == ".pdf":
        pdf_b64 = base64.b64encode(path.read_bytes()).decode("ascii")
        st.markdown(
            f"""
            <object
                data="data:application/pdf;base64,{pdf_b64}#page=1&view=FitH"
                type="application/pdf"
                width="{width}"
                height="120"
                style="border:none;"
            >
            </object>
            """,
            unsafe_allow_html=True,
        )
        return
    st.caption(path.name)


def render_header(title: str, logo_path: Path | None) -> None:
    if logo_path is None or logo_path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
        st.title(title)
        if logo_path is not None:
            render_dashboard_logo(logo_path, width=260)
        return
    mime = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
    }[logo_path.suffix.lower()]
    img_b64 = base64.b64encode(logo_path.read_bytes()).decode("ascii")
    st.markdown(
        f"""
        <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:24px; margin:1.1rem 0 0.5rem 0;">
            <h1 style="margin:34px 0 0 0; color:#1f264d; flex:1 1 auto; font-size:5.2rem; line-height:0.92; font-family:inherit; font-weight:700; letter-spacing:0.02em; text-align:left;">{title}</h1>
            <img
                src="data:{mime};base64,{img_b64}"
                alt="Logo"
                style="width:380px; max-width:34vw; height:auto; object-fit:contain; display:block; margin-top:22px;"
            />
        </div>
        """,
        unsafe_allow_html=True,
    )


def should_default_to_bling_realizado() -> bool:
    if PUBLIC_REVIEW:
        return True
    if not BASE.exists():
        return True
    try:
        base_mtime = BASE.stat().st_mtime
        cache_mtimes = [path.stat().st_mtime for path in BLING_REALIZADO_CACHES if path.exists()]
        return bool(cache_mtimes) and max(cache_mtimes) > base_mtime
    except Exception:
        return False


def _clean_list(values):
    return [v for v in values if v not in (None, "", "TODOS")]


def _vendor_key(value: object) -> str:
    txt = str(value or "").strip().upper()
    if not txt:
        return ""
    txt = "".join(ch for ch in unicodedata.normalize("NFKD", txt) if not unicodedata.combining(ch))
    return " ".join(txt.split())


def _extract_vendor_id_from_label(value: object) -> str:
    txt = str(value or "").strip()
    if txt.endswith(")") and "(" in txt:
        possible_id = txt.rsplit("(", 1)[-1].rstrip(")").strip()
        if possible_id:
            return possible_id
    return ""


def _build_vendor_alias_map(
    realizado_df: pd.DataFrame,
    vendor_map: pd.DataFrame,
    vendor_links: pd.DataFrame | None = None,
) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    if vendor_links is not None and not vendor_links.empty:
        links = vendor_links.copy()
        links["vendedor_id"] = links["vendedor_id"].fillna("").astype(str).str.strip()
        links["nome_meta"] = links["nome_meta"].fillna("").astype(str).str.strip()
        links["nome_exibicao"] = links["nome_exibicao"].fillna("").astype(str).str.strip()
        links = links[links["vendedor_id"] != ""]
        for _, row in links.iterrows():
            preferred_name = row["nome_meta"] or row["nome_exibicao"]
            if preferred_name:
                alias_map[row["vendedor_id"]] = preferred_name
    if not vendor_map.empty:
        vm = vendor_map.copy()
        vm["vendedor_id"] = vm["vendedor_id"].fillna("").astype(str).str.strip()
        vm["vendedor"] = vm["vendedor"].fillna("").astype(str).str.strip()
        vm = vm[(vm["vendedor_id"] != "") & (vm["vendedor"] != "")]
        for _, row in vm.iterrows():
            alias_map.setdefault(row["vendedor_id"], row["vendedor"])

    if not realizado_df.empty and {"vendedor_id", "vendedor"}.issubset(realizado_df.columns):
        rr = realizado_df.copy()
        rr["vendedor_id"] = rr["vendedor_id"].fillna("").astype(str).str.strip()
        rr["vendedor"] = rr["vendedor"].fillna("").astype(str).str.strip()
        rr = rr[
            (rr["vendedor_id"] != "")
            & (rr["vendedor"] != "")
            & rr["vendedor"].ne("SEM_VENDEDOR")
            & rr["vendedor"].map(_vendor_key).ne(rr["vendedor_id"].map(_vendor_key))
        ]
        if not rr.empty:
            pairs = (
                rr.groupby(["vendedor_id", "vendedor"], dropna=False)
                .size()
                .reset_index(name="cnt")
                .sort_values(["vendedor_id", "cnt", "vendedor"], ascending=[True, False, True])
                .drop_duplicates(subset=["vendedor_id"], keep="first")
            )
            for _, row in pairs.iterrows():
                alias_map.setdefault(row["vendedor_id"], row["vendedor"])
    return alias_map


def _vendor_candidates(
    selected_vendor: str,
    vendor_map: pd.DataFrame | None = None,
    vendor_alias_map: dict[str, str] | None = None,
) -> set[str]:
    selected_txt = str(selected_vendor or "").strip()
    selected_key = _vendor_key(selected_txt)
    keys = {selected_key}
    possible_id = _extract_vendor_id_from_label(selected_txt)
    if possible_id:
        keys.add(_vendor_key(possible_id))
    if vendor_alias_map:
        for vendor_id, vendor_name in vendor_alias_map.items():
            if _vendor_key(vendor_id) in keys or _vendor_key(vendor_name) in keys:
                keys.add(_vendor_key(vendor_id))
                keys.add(_vendor_key(vendor_name))
    if vendor_map is None or vendor_map.empty:
        return {key for key in keys if key}
    match = vendor_map[
        vendor_map["vendedor_id"].map(_vendor_key).isin(keys)
        | vendor_map["vendedor"].map(_vendor_key).isin(keys)
    ]
    for column in ["vendedor_id", "vendedor"]:
        if column in match.columns:
            keys.update({_vendor_key(v) for v in match[column].dropna().tolist()})
    return {key for key in keys if key}


def _vendor_display(name: object, vendor_id: object) -> str:
    name_txt = str(name or "").strip()
    vendor_id_txt = str(vendor_id or "").strip()
    if name_txt and vendor_id_txt and _vendor_key(name_txt) != _vendor_key(vendor_id_txt):
        return f"{name_txt} ({vendor_id_txt})"
    return name_txt or vendor_id_txt or "SEM_VENDEDOR"


def _vendor_display_label(option: str, option_counts: dict[str, int] | None = None) -> str:
    txt = str(option or "").strip()
    if txt in {"", "TODOS"}:
        return txt or "TODOS"
    vendor_id = _extract_vendor_id_from_label(txt)
    if vendor_id:
        base_name = txt.rsplit("(", 1)[0].strip()
        if base_name and (option_counts or {}).get(_vendor_key(base_name), 0) <= 1:
            return base_name
        return f"{base_name} ({vendor_id})" if base_name else f"ID {vendor_id}"
    if txt.isdigit():
        return f"ID {txt}"
    return txt


def _collect_vendor_metrics(
    df: pd.DataFrame,
    value_col: str,
    vendor_map: pd.DataFrame,
    vendor_alias_map: dict[str, str],
    year: int,
    selected_month: int | None,
    effective_ytd: bool,
    selected_quarter: int | None,
    today: pd.Timestamp,
) -> dict[str, float]:
    if df.empty:
        return {}
    out = df.copy()
    if "vendedor_id" not in out.columns:
        out["vendedor_id"] = ""
    if "vendedor" not in out.columns:
        out["vendedor"] = ""
    out["vendedor_id"] = out["vendedor_id"].fillna("").astype(str).str.strip()
    out["vendedor"] = out["vendedor"].fillna("").astype(str).str.strip()
    reverse_alias_map = {
        _vendor_key(vendor_name): vendor_id
        for vendor_id, vendor_name in vendor_alias_map.items()
        if str(vendor_name or "").strip()
    }
    if reverse_alias_map:
        missing_ids = out["vendedor_id"].eq("") & out["vendedor"].map(_vendor_key).isin(reverse_alias_map.keys())
        out.loc[missing_ids, "vendedor_id"] = out.loc[missing_ids, "vendedor"].map(lambda v: reverse_alias_map.get(_vendor_key(v), ""))
    if not vendor_map.empty:
        out = out.merge(
            vendor_map[["vendedor_id", "vendedor"]].rename(columns={"vendedor": "__vendor_name"}),
            on="vendedor_id",
            how="left",
        )
        missing = out["vendedor"].eq("")
        out.loc[missing, "vendedor"] = out.loc[missing, "__vendor_name"].fillna("")
        out = out.drop(columns=["__vendor_name"], errors="ignore")
    if vendor_alias_map:
        resolved = out["vendedor_id"].map(vendor_alias_map).fillna("")
        missing = out["vendedor"].eq("") | out["vendedor"].map(_vendor_key).eq(out["vendedor_id"].map(_vendor_key))
        out.loc[missing, "vendedor"] = resolved.loc[missing]
    if "data" in out.columns:
        mask = out["data"].dt.year == year
        if selected_quarter is not None:
            q_start = (selected_quarter - 1) * 3 + 1
            q_end = q_start + 2
            mask &= out["data"].dt.month.between(q_start, q_end)
        elif effective_ytd:
            mask &= out["data"].dt.month <= today.month
        elif selected_month is not None:
            mask &= out["data"].dt.month == selected_month
        out = out[mask]
    if out.empty:
        return {}
    out["vendor_label"] = [_vendor_display(name, vendor_id) for name, vendor_id in zip(out["vendedor"], out["vendedor_id"])]
    if value_col in out.columns:
        out[value_col] = pd.to_numeric(out[value_col], errors="coerce").fillna(0)
        grouped = out.groupby("vendor_label")[value_col].sum()
    else:
        grouped = out.groupby("vendor_label").size().astype(float)
    return {str(vendor).strip(): float(value) for vendor, value in grouped.items() if str(vendor).strip()}


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


def filter_vendor_scope(
    df: pd.DataFrame,
    selected_vendor: str,
    columns: list[str],
    vendor_candidates: set[str] | None = None,
) -> pd.DataFrame:
    if df.empty or selected_vendor == "TODOS":
        return df
    vendor_candidates = vendor_candidates or _vendor_candidates(
        selected_vendor,
        load_bling_vendor_map(),
        _build_vendor_alias_map(load_bling_realizado(), load_bling_vendor_map()),
    )
    mask = pd.Series(False, index=df.index)
    for column in columns:
        if column in df.columns:
            mask = mask | df[column].map(_vendor_key).isin(vendor_candidates)
    return df[mask]


def filter_company_scope(df: pd.DataFrame, selected_company: str) -> pd.DataFrame:
    if df.empty or selected_company == "TODOS" or "empresa" not in df.columns:
        return df
    return df[df["empresa"].astype(str).str.upper() == str(selected_company).upper()]


def filter_period_scope(
    df: pd.DataFrame,
    year: int,
    selected_month: int | None,
    effective_ytd: bool,
    selected_quarter: int | None,
    date_col: str = "data",
) -> pd.DataFrame:
    if df.empty or date_col not in df.columns:
        return df
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out = out[out[date_col].notna()]
    if out.empty:
        return out
    mask = out[date_col].dt.year == int(year)
    if selected_quarter is not None:
        q_start = (selected_quarter - 1) * 3 + 1
        q_end = q_start + 2
        mask &= out[date_col].dt.month.between(q_start, q_end)
    elif effective_ytd:
        ref_today = pd.Timestamp.today()
        if int(year) == ref_today.year:
            mask &= out[date_col].dt.month <= ref_today.month
    elif selected_month is not None:
        mask &= out[date_col].dt.month == int(selected_month)
    return out[mask]


def top_client_movement_table(
    df: pd.DataFrame,
    year: int,
    selected_month: int | None,
    effective_ytd: bool,
    selected_quarter: int | None,
    limit: int = 3,
    sort_by_abs: bool = False,
) -> pd.DataFrame:
    if df.empty or not {"data", "cliente", "receita"}.issubset(df.columns):
        return pd.DataFrame()
    out = df.copy()
    out["data"] = pd.to_datetime(out["data"], errors="coerce")
    out["receita"] = pd.to_numeric(out["receita"], errors="coerce").fillna(0)
    out["cliente"] = out["cliente"].fillna("").astype(str).str.strip()
    out = out[(out["data"].notna()) & (out["cliente"] != "")]
    out = filter_period_scope(out, year, selected_month, effective_ytd, selected_quarter)
    if out.empty:
        return pd.DataFrame()
    grouped = (
        out.groupby("cliente", dropna=False)["receita"]
        .sum()
        .reset_index()
    )
    if sort_by_abs:
        grouped["movimento_abs"] = grouped["receita"].abs()
        grouped = grouped.sort_values(["movimento_abs", "receita", "cliente"], ascending=[False, False, True])
    else:
        grouped = grouped.sort_values(["receita", "cliente"], ascending=[False, True])
    grouped = grouped.head(limit).copy()
    grouped["receita_fmt"] = grouped["receita"].map(fmt_brl_table)
    return grouped[["cliente", "receita_fmt"]]


def filter_sales_nature_scope(df: pd.DataFrame, selected_scope: str) -> pd.DataFrame:
    if df.empty or selected_scope == "Tudo":
        return df
    out = df.copy()
    natureza_txt = (
        out.get("natureza_label", pd.Series("", index=out.index)).fillna("").astype(str)
        + " "
        + out.get("natureza", pd.Series("", index=out.index)).fillna("").astype(str)
    ).str.upper()
    receita = pd.to_numeric(out.get("receita", pd.Series(0, index=out.index)), errors="coerce").fillna(0)
    vendedor_id = out.get("vendedor_id", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    is_devolucao = natureza_txt.str.contains("DEVOL|RETORNO|ESTORNO|CANCEL", regex=True) | receita.lt(0)
    is_non_sale = natureza_txt.str.contains("REMESSA|CONSIGN", regex=True)
    is_vendor_zero = vendedor_id.eq("0")
    if selected_scope == "Vendas efetivas":
        return out[~is_devolucao & ~is_non_sale & ~is_vendor_zero & receita.gt(0)]
    if selected_scope == "Devolucoes/Ajustes":
        return out[is_devolucao | is_vendor_zero]
    if selected_scope == "Nao vendas":
        return out[is_non_sale & ~is_devolucao & ~is_vendor_zero]
    return out


def filter_targets_view(
    view_df: pd.DataFrame,
    target_year: int,
    period_type: str,
    month_num: int | None = None,
    quarter_num: int | None = None,
    ytd: bool = False,
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
    if ytd and month_num is None and quarter_num is None and str(period_type).upper() == "MONTH" and "month_num" in df.columns:
        ref_today = pd.Timestamp.today()
        max_month = ref_today.month if int(target_year) == ref_today.year else 12
        df = df[pd.to_numeric(df["month_num"], errors="coerce").between(1, max_month)]
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


def current_targets_period_params(
    target_year: int,
    month_num: int | None,
    ytd: bool,
    quarter_num: int | None,
) -> dict[str, object]:
    if quarter_num is not None:
        return {
            "periodo_tipo": "QUARTER",
            "mes": None,
            "quarter": int(quarter_num),
            "period_label": f"Q{int(quarter_num)}",
        }
    if month_num is not None and not ytd:
        return {
            "periodo_tipo": "MONTH",
            "mes": int(month_num),
            "quarter": None,
            "period_label": pd.Timestamp(year=target_year, month=int(month_num), day=1).strftime("%b/%Y").upper(),
        }
    return {
        "periodo_tipo": "MONTH",
        "mes": None,
        "quarter": None,
        "period_label": f"YTD {target_year}",
    }


def format_targets_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "periodo_tipo" in out.columns:
        out["periodo_tipo"] = out["periodo_tipo"].astype(str).str.upper()
    if "meta_valor" in out.columns:
        out["meta_valor"] = pd.to_numeric(out["meta_valor"], errors="coerce").fillna(0)
    if "realizado_valor" in out.columns:
        out["realizado_valor"] = pd.to_numeric(out["realizado_valor"], errors="coerce").fillna(0)
    if "atingimento_pct" not in out.columns and {"meta_valor", "realizado_valor"}.issubset(out.columns):
        out["atingimento_pct"] = out.apply(
            lambda row: (row["realizado_valor"] / row["meta_valor"] * 100) if row["meta_valor"] else 0.0,
            axis=1,
        )
    if "gap_valor" not in out.columns and {"meta_valor", "realizado_valor"}.issubset(out.columns):
        out["gap_valor"] = out["realizado_valor"] - out["meta_valor"]
    if "status" in out.columns:
        out["status"] = out["status"].apply(status_chip)
    return out


init_db()

sidebar_logo_path = find_dashboard_logo()
if sidebar_logo_path is not None and sidebar_logo_path.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
    sidebar_logo_b64 = base64.b64encode(sidebar_logo_path.read_bytes()).decode("ascii")
    sidebar_logo_mime = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
    }[sidebar_logo_path.suffix.lower()]
    st.sidebar.markdown(
        f"""
        <div style="display:flex; justify-content:center; margin:-1.45rem 0 1.35rem 0;">
            <img
                src="data:{sidebar_logo_mime};base64,{sidebar_logo_b64}"
                alt="Logo"
                style="width:205px; max-width:92%; height:auto; object-fit:contain; display:block;"
            />
        </div>
        """,
        unsafe_allow_html=True,
    )

if st.sidebar.button("Recarregar base"):
    for loader in [
        load_sheets,
        load_sales_targets_view,
        load_sales_pipeline_view,
        load_crm_priority_queue,
        load_bling_realizado,
        load_bling_nfe,
        load_bling_sales_detail,
    ]:
        try:
            loader.clear()
        except Exception:
            pass
use_bling = st.sidebar.checkbox("Usar realizado do Bling (NF-e)", value=should_default_to_bling_realizado())
show_inactive_vendors = st.sidebar.checkbox("Mostrar inativos/historico", value=False)

sheets = load_sheets()
if not sheets:
    st.warning("Base principal nao encontrada. Carregando modo revisao com dados vazios.")
    sheets = {
        "metas": pd.DataFrame(columns=["data", "vendedor", "meta"]),
        "realizado": pd.DataFrame(columns=["data", "vendedor", "receita"]),
        "oportunidades": pd.DataFrame(columns=["cliente", "vendedor", "volume_potencial", "probabilidade", "data_proximo_passo"]),
        "atividades": pd.DataFrame(columns=["data"]),
    }
for key, value in list(sheets.items()):
    if isinstance(value, pd.DataFrame):
        sheets[key] = upper_dashboard_text(value)

# Apply ACL on loaded sheets (gestor profile)
for key in ["metas", "realizado", "oportunidades"]:
    if key in sheets and not sheets[key].empty:
        sheets[key] = apply_acl(sheets[key], vendor_col="vendedor")

# Sidebar controls
page_options = [
    "Executive Cockpit",
    "Lab Comercial",
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

st.sidebar.markdown("### Periodo")
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

sales_scope = st.sidebar.selectbox(
    "Movimento fiscal",
    options=["Vendas efetivas", "Tudo", "Devolucoes/Ajustes", "Nao vendas"],
    index=0,
)
if use_bling:
    br = load_bling_realizado()
    if not br.empty:
        sheets["realizado"] = upper_dashboard_text(filter_sales_nature_scope(br, sales_scope))
        if "origem" in br.columns and not br["origem"].astype(str).eq("bling_nfe").any():
            st.sidebar.warning("NF-e nao encontrada no cache local. Realizado ainda em fallback por pedido.")

st.sidebar.markdown("### Filtros")
company_options = ["TODOS", "CZ", "CR"]
sel_company = st.sidebar.selectbox("Empresa", options=company_options, index=0)

vendor_map = upper_dashboard_text(load_bling_vendor_map())
vendor_links = upper_dashboard_text(load_vendor_links())
vendor_alias_map = _build_vendor_alias_map(sheets.get("realizado", pd.DataFrame()), vendor_map, vendor_links)

# Build vendor list focused on selected period; keep optional historical expansion.
vendor_scores: dict[str, float] = {}
all_vendors_set: set[str] = set()
today = pd.Timestamp.today()

for sheet_name, value_col in [("metas", "meta"), ("realizado", "receita")]:
    dfv = sheets.get(sheet_name, pd.DataFrame())
    if dfv.empty:
        continue
    all_vendors_set.update(
        _collect_vendor_metrics(dfv, value_col, vendor_map, vendor_alias_map, year, None, False, None, today).keys()
    )
    period_scores = _collect_vendor_metrics(
        dfv,
        value_col,
        vendor_map,
        vendor_alias_map,
        year,
        selected_month,
        effective_ytd,
        selected_quarter,
        today,
    )
    for vend, val in period_scores.items():
        vendor_scores[vend] = vendor_scores.get(vend, 0.0) + float(val)

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

vendor_name_counts: dict[str, int] = {}
for option in vendors:
    vendor_id = _extract_vendor_id_from_label(option)
    if vendor_id:
        base_name = option.rsplit("(", 1)[0].strip()
        if base_name:
            vendor_name_counts[_vendor_key(base_name)] = vendor_name_counts.get(_vendor_key(base_name), 0) + 1
display_vendor_map = {option: _vendor_display_label(option, vendor_name_counts) for option in vendors}
sel_vendor = st.sidebar.selectbox(
    "Vendedor",
    options=vendors,
    index=0,
    format_func=lambda option: display_vendor_map.get(option, str(option)),
)
selected_vendor_candidates = _vendor_candidates(sel_vendor, vendor_map, vendor_alias_map) if sel_vendor != "TODOS" else set()

if PROFILE == "gestor":
    acl = load_acl().get("gestor", {})
    title = acl.get("title") or "Clear Agro CRM - Gestor"
else:
    title = APP_TITLE
logo_path = find_dashboard_logo()
render_header(title, logo_path)
if PUBLIC_REVIEW:
    st.info("Modo revisao publica ativo: acesso sem login, somente visualizacao e paginas CRM.")
    st.caption(f"Build: {APP_BUILD}")
period = period_label(year, selected_month, effective_ytd, selected_quarter)
st.caption(f"Periodo: {period}")
st.caption(f"Filtros: Empresa={sel_company} | Movimento={sales_scope}")

if PUBLIC_REVIEW and page == "Metas Comerciais":
    st.warning("Pagina indisponivel no modo de revisao publica.")
    st.stop()

# Apply vendor filter to metas/realizado
if sel_company != "TODOS":
    for key in ["realizado", "oportunidades", "atividades"]:
        if key in sheets and isinstance(sheets[key], pd.DataFrame):
            sheets[key] = filter_company_scope(sheets[key], sel_company)

if sel_vendor != "TODOS":
    if "metas" in sheets:
        mask = pd.Series(False, index=sheets["metas"].index)
        for column in ["vendedor", "vendedor_id"]:
            if column in sheets["metas"].columns:
                mask = mask | sheets["metas"][column].map(_vendor_key).isin(selected_vendor_candidates)
        sheets["metas"] = sheets["metas"][mask]
    if "realizado" in sheets:
        mask = pd.Series(False, index=sheets["realizado"].index)
        for column in ["vendedor", "vendedor_id"]:
            if column in sheets["realizado"].columns:
                mask = mask | sheets["realizado"][column].map(_vendor_key).isin(selected_vendor_candidates)
        sheets["realizado"] = sheets["realizado"][mask]

if sales_scope != "Vendas efetivas" and "metas" in sheets:
    sheets["metas"] = sheets["metas"].iloc[0:0].copy()

# Page A - Executive Cockpit
if page == "Executive Cockpit":
    st.subheader("Executive Cockpit")
    crm_pipeline_view = upper_dashboard_text(apply_acl_codes(load_sales_pipeline_view(), vendor_col="sales_rep_code"))
    crm_pipeline_view = filter_vendor_scope(
        crm_pipeline_view, sel_vendor, ["sales_rep_code", "sales_rep_name"], selected_vendor_candidates
    )
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
    if meta_display == 0 and sales_scope == "Vendas efetivas":
        mf = {"ano": year, "periodo_tipo": "MONTH"}
        if sel_vendor != "TODOS":
            mf["vendedor_id"] = next(
                (value for value in vendor_map["vendedor_id"].tolist() if _vendor_key(value) in selected_vendor_candidates),
                sel_vendor,
            )
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
    c2.metric("Meta", fmt_brl_abbrev(meta_display) if sales_scope == "Vendas efetivas" else "-")
    c3.metric("Atingimento %", fmt_pct(ating_display) if sales_scope == "Vendas efetivas" else "-")
    c4.metric("Gap (R$)", fmt_brl_abbrev(gap_display) if sales_scope == "Vendas efetivas" else "-")
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

    if sales_scope == "Vendas efetivas":
        top_clients = top_client_movement_table(
            sheets.get("realizado", pd.DataFrame()),
            year,
            selected_month,
            effective_ytd,
            selected_quarter,
            limit=5,
        )
        if not top_clients.empty:
            st.caption("Top 5 clientes por vendas efetivas")
            st.dataframe(top_clients, hide_index=True, width="stretch")
    elif sales_scope in {"Devolucoes/Ajustes", "Nao vendas"}:
        top_clients = top_client_movement_table(
            sheets.get("realizado", pd.DataFrame()),
            year,
            selected_month,
            effective_ytd,
            selected_quarter,
            limit=3,
            sort_by_abs=True,
        )
        if not top_clients.empty:
            st.caption(f"Top 3 clientes por maior valor de movimento em {sales_scope.lower()}")
            st.dataframe(top_clients, hide_index=True, width="stretch")

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

# Page A2 - Lab Comercial
if page == "Lab Comercial":
    st.subheader("Lab Comercial")
    st.caption("Pagina paralela para testar visoes comerciais sem alterar o cockpit principal.")

    lab_realizado = sheets.get("realizado", pd.DataFrame()).copy()
    if lab_realizado.empty or not {"data", "receita"}.issubset(lab_realizado.columns):
        st.info("Pendencia: sem base suficiente de realizado para o laboratorio comercial.")
    else:
        lab_realizado = filter_period_scope(lab_realizado, year, selected_month, effective_ytd, selected_quarter)
        lab_realizado["data"] = pd.to_datetime(lab_realizado["data"], errors="coerce")
        lab_realizado["receita"] = pd.to_numeric(lab_realizado["receita"], errors="coerce").fillna(0)
        lab_realizado = lab_realizado[lab_realizado["data"].notna()].copy()
        st.caption(f"RECORTE ATUAL: {period} | EMPRESA={sel_company} | MOVIMENTO={sales_scope} | VENDEDOR={display_vendor_map.get(sel_vendor, sel_vendor)}")

        if lab_realizado.empty:
            st.info("Sem dados para os filtros selecionados.")
            st.stop()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Realizado", fmt_brl_abbrev(float(lab_realizado["receita"].sum())))
        c2.metric("Clientes", int(lab_realizado["cliente"].nunique()) if "cliente" in lab_realizado.columns else 0)
        c3.metric("Vendedores", int(lab_realizado["vendedor_id"].astype(str).nunique()) if "vendedor_id" in lab_realizado.columns else 0)
        ticket_medio = float(lab_realizado["receita"].mean()) if not lab_realizado.empty else 0.0
        c4.metric("Ticket medio", fmt_brl_abbrev(ticket_medio))

        monthly = (
            lab_realizado.assign(mes=lab_realizado["data"].dt.to_period("M").astype(str))
            .groupby("mes", dropna=False)["receita"]
            .sum()
            .reset_index()
        )
        if not monthly.empty:
            st.markdown("#### Evolucao")
            monthly_chart = (
                alt.Chart(monthly)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color="#1f264d")
                .encode(
                    x=alt.X("mes:N", title="Mes"),
                    y=alt.Y("receita:Q", title="Receita"),
                    tooltip=["mes", alt.Tooltip("receita:Q", format=",.2f", title="Receita")],
                )
                .properties(height=280)
            )
            st.altair_chart(monthly_chart, width="stretch")

            monthly_sorted = monthly.copy()
            monthly_sorted["receita_prev"] = monthly_sorted["receita"].shift(1)
            monthly_sorted["variacao_pct"] = (
                (monthly_sorted["receita"] - monthly_sorted["receita_prev"])
                / monthly_sorted["receita_prev"].replace(0, pd.NA)
            ) * 100
            if len(monthly_sorted) >= 2:
                latest_row = monthly_sorted.iloc[-1]
                prev_row = monthly_sorted.iloc[-2]
                latest_delta = float(latest_row["variacao_pct"]) if pd.notna(latest_row["variacao_pct"]) else 0.0
                st.caption(
                    f"Ritmo mensal: {latest_row['mes']} vs {prev_row['mes']} = {fmt_pct(latest_delta)}"
                )

        left_col, right_col = st.columns(2)

        with left_col:
            if "vendedor" in lab_realizado.columns:
                st.markdown("#### Ranking Comercial")
                top_vendedores = (
                    lab_realizado.assign(vendedor_lab=lab_realizado["vendedor"].fillna("").replace("", "SEM_VENDEDOR"))
                    .groupby("vendedor_lab", dropna=False)["receita"]
                    .sum()
                    .reset_index()
                    .sort_values("receita", ascending=False)
                    .head(10)
                )
                if not top_vendedores.empty:
                    st.caption("Top 10 vendedores")
                    chart_vendedores = (
                        alt.Chart(top_vendedores)
                        .mark_bar(cornerRadiusEnd=4, color="#00b6b9")
                        .encode(
                            y=alt.Y("vendedor_lab:N", sort="-x", title="Vendedor"),
                            x=alt.X("receita:Q", title="Receita"),
                            tooltip=["vendedor_lab", alt.Tooltip("receita:Q", format=",.2f", title="Receita")],
                        )
                        .properties(height=320)
                    )
                    st.altair_chart(chart_vendedores, width="stretch")

        with right_col:
            if "cliente" in lab_realizado.columns:
                st.markdown("#### Ranking de Clientes")
                top_clientes_lab = (
                    lab_realizado.groupby("cliente", dropna=False)["receita"]
                    .sum()
                    .reset_index()
                    .sort_values("receita", ascending=False)
                    .head(10)
                )
                if not top_clientes_lab.empty:
                    st.caption("Top 10 clientes")
                    chart_clientes = (
                        alt.Chart(top_clientes_lab)
                        .mark_bar(cornerRadiusEnd=4, color="#5fbd65")
                        .encode(
                            y=alt.Y("cliente:N", sort="-x", title="Cliente"),
                            x=alt.X("receita:Q", title="Receita"),
                            tooltip=["cliente", alt.Tooltip("receita:Q", format=",.2f", title="Receita")],
                        )
                        .properties(height=320)
                    )
                    st.altair_chart(chart_clientes, width="stretch")

        curve_col, mix_col = st.columns(2)

        with curve_col:
            if "cliente" in lab_realizado.columns:
                st.markdown("#### Curva ABC")
                abc = (
                    lab_realizado.groupby("cliente", dropna=False)["receita"]
                    .sum()
                    .reset_index()
                    .sort_values("receita", ascending=False)
                )
                if not abc.empty:
                    abc["share_pct"] = abc["receita"] / abc["receita"].sum() * 100
                    abc["cum_pct"] = abc["share_pct"].cumsum()
                    abc["faixa"] = pd.cut(
                        abc["cum_pct"],
                        bins=[-1, 80, 95, 100],
                        labels=["A", "B", "C"],
                    )
                    abc_summary = (
                        abc.groupby("faixa", dropna=False)
                        .agg(clientes=("cliente", "count"), receita=("receita", "sum"))
                        .reset_index()
                    )
                    st.caption("Curva ABC de clientes")
                    abc_chart = (
                        alt.Chart(abc_summary)
                        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                        .encode(
                            x=alt.X("faixa:N", title="Faixa"),
                            y=alt.Y("receita:Q", title="Receita"),
                            color=alt.Color("faixa:N", scale=alt.Scale(range=["#1f264d", "#00b6b9", "#5fbd65"])),
                            tooltip=[
                                "faixa",
                                alt.Tooltip("clientes:Q", format=",.0f", title="Clientes"),
                                alt.Tooltip("receita:Q", format=",.2f", title="Receita"),
                            ],
                        )
                        .properties(height=260)
                    )
                    st.altair_chart(abc_chart, width="stretch")

        with mix_col:
            if "empresa" in lab_realizado.columns:
                st.markdown("#### Mix")
                mix_empresa = (
                    lab_realizado.groupby("empresa", dropna=False)["receita"]
                    .sum()
                    .reset_index()
                    .sort_values("receita", ascending=False)
                )
                if not mix_empresa.empty:
                    total_mix = float(mix_empresa["receita"].sum())
                    mix_empresa["participacao"] = mix_empresa["receita"] / total_mix * 100 if total_mix else 0.0
                    st.caption("Mix por empresa")
                    mix_chart = (
                        alt.Chart(mix_empresa)
                        .mark_arc(innerRadius=55)
                        .encode(
                            theta=alt.Theta("receita:Q"),
                            color=alt.Color("empresa:N", scale=alt.Scale(range=["#1f264d", "#5fbd65", "#00b6b9"])),
                            tooltip=[
                                "empresa",
                                alt.Tooltip("receita:Q", format=",.2f", title="Receita"),
                                alt.Tooltip("participacao:Q", format=",.2f", title="Participacao %"),
                            ],
                        )
                        .properties(height=260)
                    )
                    st.altair_chart(mix_chart, width="stretch")

        concentration_col, growth_col = st.columns(2)

        with concentration_col:
            if "vendedor" in lab_realizado.columns:
                st.markdown("#### Concentracao")
                vendedor_conc = (
                    lab_realizado.assign(vendedor_lab=lab_realizado["vendedor"].fillna("").replace("", "SEM_VENDEDOR"))
                    .groupby("vendedor_lab", dropna=False)["receita"]
                    .sum()
                    .reset_index()
                    .sort_values("receita", ascending=False)
                )
                if not vendedor_conc.empty:
                    total_vendedores = float(vendedor_conc["receita"].sum())
                    top3_share = vendedor_conc["receita"].head(3).sum() / total_vendedores * 100 if total_vendedores else 0.0
                    top5_share = vendedor_conc["receita"].head(5).sum() / total_vendedores * 100 if total_vendedores else 0.0
                    st.caption(
                        f"Concentracao comercial: Top 3 = {fmt_pct(top3_share)} | Top 5 = {fmt_pct(top5_share)}"
                    )
                    conc_view = vendedor_conc.head(8).assign(
                        share_pct=lambda df: df["receita"] / total_vendedores * 100 if total_vendedores else 0.0
                    )[["vendedor_lab", "receita", "share_pct"]]
                    conc_view["receita"] = conc_view["receita"].map(fmt_brl_full)
                    conc_view["share_pct"] = conc_view["share_pct"].map(fmt_pct)
                    st.dataframe(
                        conc_view,
                        hide_index=True,
                        width="stretch",
                    )

        with growth_col:
            if "cliente" in lab_realizado.columns:
                st.markdown("#### Crescimento")
                client_month = (
                    lab_realizado.assign(mes=lab_realizado["data"].dt.to_period("M").astype(str))
                    .groupby(["cliente", "mes"], dropna=False)["receita"]
                    .sum()
                    .reset_index()
                )
                if not client_month.empty and len(monthly) >= 2:
                    recent_months = monthly["mes"].tolist()[-2:]
                    growth = (
                        client_month[client_month["mes"].isin(recent_months)]
                        .pivot_table(index="cliente", columns="mes", values="receita", aggfunc="sum", fill_value=0)
                        .reset_index()
                    )
                    if len(recent_months) == 2 and all(month in growth.columns for month in recent_months):
                        growth["delta"] = growth[recent_months[1]] - growth[recent_months[0]]
                        growth = growth.sort_values("delta", ascending=False).head(8)
                        growth_view = growth[["cliente", recent_months[0], recent_months[1], "delta"]].copy()
                        for col in [recent_months[0], recent_months[1], "delta"]:
                            growth_view[col] = growth_view[col].map(fmt_brl_full)
                        st.caption(f"Clientes que mais cresceram: {recent_months[0]} -> {recent_months[1]}")
                        st.dataframe(
                            growth_view,
                            hide_index=True,
                            width="stretch",
                        )

        if "cliente" in lab_realizado.columns:
            st.markdown("#### Carteira")
            recency = (
                lab_realizado.groupby("cliente", dropna=False)
                .agg(ultima_compra=("data", "max"), receita=("receita", "sum"))
                .reset_index()
            )
            if not recency.empty:
                recency["dias_sem_compra"] = (today.normalize() - recency["ultima_compra"].dt.normalize()).dt.days
                carteira_risco = recency[recency["dias_sem_compra"] >= 30].sort_values(
                    ["dias_sem_compra", "receita"], ascending=[False, False]
                ).head(10)
                if not carteira_risco.empty:
                    carteira_view = carteira_risco[["cliente", "ultima_compra", "dias_sem_compra", "receita"]].copy()
                    carteira_view["ultima_compra"] = carteira_view["ultima_compra"].dt.strftime("%d/%m/%Y")
                    carteira_view["dias_sem_compra"] = carteira_view["dias_sem_compra"].map(fmt_int_br)
                    carteira_view["receita"] = carteira_view["receita"].map(fmt_brl_full)
                    st.caption("Carteira sem compra recente")
                    st.dataframe(
                        carteira_view,
                        hide_index=True,
                        width="stretch",
                    )

        if "empresa" in lab_realizado.columns and sel_company == "TODOS":
            st.markdown("#### Comparativo Entre Empresas")
            company_month = (
                lab_realizado.assign(mes=lab_realizado["data"].dt.to_period("M").astype(str))
                .groupby(["mes", "empresa"], dropna=False)["receita"]
                .sum()
                .reset_index()
            )
            if not company_month.empty:
                st.caption("Evolucao mensal por empresa")
                company_chart = (
                    alt.Chart(company_month)
                    .mark_line(point=True, strokeWidth=3)
                    .encode(
                        x=alt.X("mes:N", title="Mes"),
                        y=alt.Y("receita:Q", title="Receita"),
                        color=alt.Color("empresa:N", title="Empresa"),
                        tooltip=["mes", "empresa", alt.Tooltip("receita:Q", format=",.2f", title="Receita")],
                    )
                    .properties(height=280)
                )
                st.altair_chart(company_chart, width="stretch")

        if sales_scope == "Vendas efetivas" and "metas" in sheets and not sheets["metas"].empty:
            metas_lab = filter_period_scope(sheets["metas"].copy(), year, selected_month, effective_ytd, selected_quarter)
            metas_lab["meta"] = pd.to_numeric(metas_lab["meta"], errors="coerce").fillna(0)
            meta_total_lab = float(metas_lab["meta"].sum())
            realizado_total_lab = float(lab_realizado["receita"].sum())
            ating_lab = (realizado_total_lab / meta_total_lab * 100) if meta_total_lab else 0.0
            st.caption(
                f"Comparativo laboratorio: Meta {fmt_brl_abbrev(meta_total_lab)} | Realizado {fmt_brl_abbrev(realizado_total_lab)} | Atingimento {fmt_pct(ating_lab)}"
            )

# Page B - Pipeline Manager
if page == "Pipeline Manager":
    st.subheader("Pipeline Manager")
    pipeline_view = upper_dashboard_text(apply_acl_codes(load_sales_pipeline_view(), vendor_col="sales_rep_code"))
    pipeline_view = filter_vendor_scope(
        pipeline_view, sel_vendor, ["sales_rep_code", "sales_rep_name"], selected_vendor_candidates
    )
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

        queue_df = upper_dashboard_text(apply_acl_codes(load_crm_priority_queue(), vendor_col="sales_rep_code"))
        queue_df = filter_vendor_scope(
            queue_df, sel_vendor, ["sales_rep_code", "sales_rep_name"], selected_vendor_candidates
        )
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
    perf_pipeline_view = upper_dashboard_text(apply_acl_codes(load_sales_pipeline_view(), vendor_col="sales_rep_code"))
    perf_pipeline_view = filter_vendor_scope(
        perf_pipeline_view, sel_vendor, ["sales_rep_code", "sales_rep_name"], selected_vendor_candidates
    )
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
    queue_df = filter_vendor_scope(
        queue_df, sel_vendor, ["sales_rep_code", "sales_rep_name"], selected_vendor_candidates
    )
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
    quality_path = ROOT / "data" / "quality" / "finance_pack_report.json"
    finance_inputs = {
        "DRE CR": ROOT / "data" / "staging" / "stg_dre_cr.csv",
        "DRE CZ": ROOT / "data" / "staging" / "stg_dre_cz.csv",
        "DRE EMPRESA": ROOT / "data" / "staging" / "stg_dre_empresa.csv",
        "Bancos": ROOT / "data" / "staging" / "stg_banks.csv",
        "Contas a pagar": ROOT / "bling_api" / "contas_pagar_cache.jsonl",
        "Contas a receber": ROOT / "bling_api" / "contas_receber_cache.jsonl",
    }

    def _read_csv(path: Path) -> pd.DataFrame:
        try:
            if path.exists():
                return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
        return pd.DataFrame()

    def _fmt_ts(path: Path) -> str:
        if not path.exists():
            return "ausente"
        return pd.Timestamp(path.stat().st_mtime, unit="s").strftime("%Y-%m-%d %H:%M")

    def _load_finance_quality() -> dict:
        if not quality_path.exists():
            return {}
        try:
            with quality_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    df_exec = _read_csv(exports_dir / "finance_executive_summary.csv")
    df_kpis = _read_csv(exports_dir / "finance_kpis_monthly.csv")
    df_exc = _read_csv(exports_dir / "finance_reconciliation_exceptions.csv")
    df_ap_ar = _read_csv(marts_dir / "fact_ap_ar.csv")
    df_cash = _read_csv(marts_dir / "fact_cashflow_detailed.csv")
    df_dre = _read_csv(marts_dir / "fact_dre_finance.csv")
    quality_payload = _load_finance_quality()
    quality_rows = quality_payload.get("rows", {}) if isinstance(quality_payload.get("rows"), dict) else {}
    missing_inputs = [label for label, path in finance_inputs.items() if not path.exists()]
    empty_outputs = [
        label
        for label, rows in [
            ("DRE", quality_rows.get("fact_dre_finance")),
            ("AP/AR", quality_rows.get("fact_ap_ar")),
            ("Caixa", quality_rows.get("fact_cashflow_detailed")),
        ]
        if rows in (None, 0)
    ]

    if missing_inputs or empty_outputs:
        parts = []
        if missing_inputs:
            parts.append("fontes ausentes: " + ", ".join(missing_inputs))
        if empty_outputs:
            parts.append("saidas vazias: " + ", ".join(empty_outputs))
        st.warning("Base financeira incompleta. " + " | ".join(parts))
    else:
        st.success("Base financeira carregada com insumos locais disponiveis.")

    with st.expander("Status das fontes financeiras", expanded=False):
        status_rows = pd.DataFrame(
            [
                {
                    "fonte": label,
                    "arquivo": str(path.relative_to(ROOT)),
                    "status": "ok" if path.exists() else "ausente",
                    "ultima_atualizacao": _fmt_ts(path),
                }
                for label, path in finance_inputs.items()
            ]
        )
        st.dataframe(status_rows, width="stretch", height=250)
        if quality_payload:
            st.caption(
                f"finance_pack_report.json: status={quality_payload.get('status', 'desconhecido')} | "
                f"gerado_em={_fmt_ts(quality_path)}"
            )

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
        nfe = filter_company_scope(nfe, sel_company)
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
    targets_view_all = filter_vendor_scope(
        targets_view_all, sel_vendor, ["sales_rep_code", "sales_rep_name"], selected_vendor_candidates
    )
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
