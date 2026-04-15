import os
import json
import base64
import subprocess
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
    load_bling_nfe_detail,
    load_bling_nfe_detail_years,
    load_bling_contas,
    load_bling_estoque,
    load_bling_sales_realized_view,
    load_sales_targets_view,
    load_sales_pipeline_view,
    load_sales_realized_view,
    load_crm_priority_queue,
    get_crm_view_error,
)
from src.metrics import compute_kpis, vendedor_performance_period, meta_realizado_mensal, sparkline_last_months, period_label
from src.viz import fmt_brl_abbrev, fmt_pct, bar_meta_realizado, bar_meta_realizado_single, sparkline
from src.metas_db import (
    init_db,
    list_metas,
    create_meta,
    update_meta,
    pause_metas,
    summary_targets,
    transfer_assets,
    transfer_metas_futuras,
    seed_demo,
    prepare_sales_targets_import,
)
from src.telegram import build_alerts_message, send_telegram_message, telegram_enabled
from src.vendor_utils import build_vendor_selector_options, normalize_vendor_identity

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
    .crm-kpi-row div[data-testid="stMetricValue"] {font-size: 24px;}
    .crm-kpi-row div[data-testid="stMetricLabel"] {font-size: 13px;}
    .sidebar-logo-shell {
        display: flex;
        justify-content: center;
        margin: -1.1rem 0 1.35rem 0;
    }
    .sidebar-logo-badge {
        display: inline-flex;
        justify-content: center;
        align-items: center;
        width: 100%;
        max-width: 230px;
        padding: 0.85rem 0.7rem;
        background: linear-gradient(135deg, rgba(255,255,255,0.94) 0%, rgba(240,245,252,0.98) 100%);
        border: 1px solid rgba(31,42,68,0.12);
        border-radius: 18px;
        box-shadow: 0 10px 18px rgba(31,42,68,0.06), 0 22px 42px rgba(31,42,68,0.11);
    }
    .hero-card {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 28px;
        margin: 2.8rem 0 1.6rem 0;
        padding: 2.1rem 2.4rem;
        background: linear-gradient(135deg, rgba(255,255,255,0.92) 0%, rgba(240,245,252,0.98) 100%);
        border: 1px solid rgba(31,42,68,0.12);
        border-radius: 28px;
        box-shadow: 0 22px 44px rgba(31,42,68,0.10);
        position: relative;
        overflow: hidden;
    }
    .hero-card::before {
        content: "";
        position: absolute;
        inset: 0 auto 0 0;
        width: 8px;
        background: linear-gradient(180deg, #b88b2f 0%, #1f2a44 100%);
    }
    .hero-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 28px;
        width: 100%;
    }
    .hero-title-wrap {
        padding-left: 1rem;
        flex: 1 1 auto;
        min-width: 0;
    }
    .hero-title {
        margin: 0;
        color: #1f264d;
        font-size: 9.6rem;
        line-height: 0.88;
        font-family: inherit;
        font-weight: 700;
        letter-spacing: 0.04em;
        text-align: left;
    }
    .hero-logo-box {
        flex: 0 0 24%;
        display: flex;
        justify-content: flex-end;
        align-items: center;
    }
    .hero-logo-box img {
        width: 100%;
        max-width: 320px;
        height: auto;
        object-fit: contain;
        display: block;
    }
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
        <div class="hero-card">
            <div class="hero-header">
                <div class="hero-title-wrap">
                    <h1 class="hero-title">{title}</h1>
                </div>
                <div class="hero-logo-box">
                    <img
                        src="data:{mime};base64,{img_b64}"
                        alt="Logo"
                    />
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def load_consignacao_prompt() -> str:
    prompt_path = ROOT / "01_crm_comercial" / "Prompt Consignacao - Executivo.md"
    if not prompt_path.exists():
        return ""
    try:
        return prompt_path.read_text(encoding="utf-8")
    except Exception:
        try:
            return prompt_path.read_text(encoding="latin-1")
        except Exception:
            return ""


def render_consignacao_page(logo_path: Path | None) -> None:
    consign_patterns = [
        "REMESSA DE MERCADORIA EM CONSIGNACAO",
        "REMESSA DE MERCADORIA EM CONSIGNACAO MERCANTIL OU INDUSTRIAL",
        "CONSIGNACAO/SEM FATURAMENTO",
    ]

    detail = load_bling_nfe_detail(0)
    if detail.empty:
        st.warning("Nao foi possivel carregar detalhes de NF-e do Bling para consignacao.")
        return

    detail = filter_company_scope(detail, sel_company)
    detail = filter_period_scope(detail, year, selected_month, effective_ytd, selected_quarter)
    detail = filter_vendor_scope(detail, sel_vendor, ["vendedor", "vendedor_id"], selected_vendor_candidates)

    natureza_base = detail["natureza_label"].fillna("").astype(str).str.upper()
    consign_mask = pd.Series(False, index=detail.index)
    for pattern in consign_patterns:
        consign_mask = consign_mask | natureza_base.str.contains(pattern, regex=False)
    detail = detail[consign_mask].copy()

    if detail.empty:
        st.info("Nao ha movimentacoes de consignacao no filtro atual.")
        return

    customer_options = ["TODOS"] + sorted(detail["cliente"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().unique().tolist())
    sel_consign_customer = st.sidebar.selectbox("Cliente consignacao", options=customer_options, index=0)
    if sel_consign_customer != "TODOS":
        detail = filter_customer_scope(detail, sel_consign_customer, ["cliente"])
        if detail.empty:
            st.info("Nao ha movimentacoes de consignacao para o cliente selecionado.")
            return

    detail["lote"] = detail["lote"].fillna("S/N").replace("", "S/N")
    detail["vencimento_lote"] = pd.to_datetime(detail["vencimento_lote"], errors="coerce")

    nf_view = (
        detail.groupby(["empresa", "nfe_id", "numero_nf", "data", "cliente", "vendedor", "vendedor_id"], dropna=False)
        .agg(valor_nf=("valor_nf", "max"), itens=("produto", "count"), qtd_total=("quantidade", "sum"))
        .reset_index()
    )
    nf_view["dias_em_aberto"] = (pd.Timestamp.today().normalize() - nf_view["data"].dt.normalize()).dt.days

    aging_bins = [-1, 30, 60, 90, 10_000]
    aging_labels = ["0-30 dias", "31-60 dias", "61-90 dias", "90+ dias"]
    nf_view["aging"] = pd.cut(nf_view["dias_em_aberto"], bins=aging_bins, labels=aging_labels)
    aging_df = (
        nf_view.groupby("aging", dropna=False)["valor_nf"]
        .sum()
        .reset_index()
        .rename(columns={"valor_nf": "valor"})
    )
    aging_df["aging"] = aging_df["aging"].astype(str)

    mensal_df = (
        nf_view.groupby("data", dropna=False)["valor_nf"]
        .sum()
        .reset_index()
    )
    mensal_df["mes"] = mensal_df["data"].dt.to_period("M").dt.to_timestamp()
    mensal_df = mensal_df.groupby("mes", dropna=False)["valor_nf"].sum().reset_index()

    consolidado_df = (
        detail.groupby(["empresa", "vendedor", "cliente"], dropna=False)
        .agg(
            valor_remetido=("valor_total", "sum"),
            quantidade_total=("quantidade", "sum"),
            produtos_distintos=("produto", "nunique"),
            nfs=("nfe_id", "nunique"),
            primeiro_envio=("data", "min"),
            ultimo_envio=("data", "max"),
            dias_em_aberto_max=("dias_em_aberto", "max"),
        )
        .reset_index()
        .sort_values("valor_remetido", ascending=False)
    )
    consolidado_df["lotes"] = "S/N"
    consolidado_df["vencimento_lote"] = "S/N"
    consolidado_df["saldo_lote"] = 0.0

    resumo_vendedor_df = (
        consolidado_df.groupby("vendedor", dropna=False)
        .agg(
            valor_remetido=("valor_remetido", "sum"),
            quantidade_total=("quantidade_total", "sum"),
            produtos_distintos=("produtos_distintos", "sum"),
            clientes=("cliente", "nunique"),
            nfs=("nfs", "sum"),
            saldo_lote=("saldo_lote", "sum"),
        )
        .reset_index()
        .sort_values("valor_remetido", ascending=False)
        .head(10)
    )

    clientes_consignados_df = (
        detail.groupby(["empresa", "cliente", "produto"], dropna=False)
        .agg(
            valor_remetido=("valor_total", "sum"),
            quantidade=("quantidade", "sum"),
            lotes=("lote", lambda s: ", ".join(sorted({str(x).strip() for x in s if str(x).strip() and str(x).strip() != 'S/N'})[:8])),
            vencimento=("vencimento_lote", lambda s: ", ".join(sorted({d.strftime("%d/%m/%Y") for d in pd.to_datetime(s, errors="coerce").dropna()})[:8])),
        )
        .reset_index()
        .sort_values(["cliente", "valor_remetido"], ascending=[True, False])
    )

    st.markdown("### Clientes Consignados")
    clientes_display = clientes_consignados_df.copy()
    clientes_display["lotes"] = clientes_display["lotes"].replace("", "S/N")
    clientes_display["vencimento"] = clientes_display["vencimento"].replace("", "S/N")
    ordered_cols = ["empresa", "cliente", "produto", "quantidade", "valor_remetido", "lotes", "vencimento"]
    clientes_display = clientes_display[[col for col in ordered_cols if col in clientes_display.columns]]
    clientes_display["valor_remetido"] = clientes_display["valor_remetido"].map(fmt_brl_table)
    clientes_display["quantidade"] = clientes_display["quantidade"].map(fmt_int_br)
    st.dataframe(clientes_display, use_container_width=True, hide_index=True)

    st.markdown("### Carteira consolidada por vendedor / cliente")
    consolidado_display = consolidado_df.copy()
    consolidado_display["valor_remetido"] = consolidado_display["valor_remetido"].map(fmt_brl_table)
    consolidado_display["quantidade_total"] = consolidado_display["quantidade_total"].map(fmt_int_br)
    consolidado_display["produtos_distintos"] = consolidado_display["produtos_distintos"].map(fmt_int_br)
    consolidado_display["nfs"] = consolidado_display["nfs"].map(fmt_int_br)
    consolidado_display["primeiro_envio"] = pd.to_datetime(consolidado_display["primeiro_envio"], errors="coerce").dt.strftime("%d/%m/%Y")
    consolidado_display["ultimo_envio"] = pd.to_datetime(consolidado_display["ultimo_envio"], errors="coerce").dt.strftime("%d/%m/%Y")
    consolidado_display["dias_em_aberto_max"] = consolidado_display["dias_em_aberto_max"].map(fmt_int_br)
    consolidado_display["saldo_lote"] = consolidado_display["saldo_lote"].map(fmt_int_br)
    consolidado_display = consolidado_display.drop(
        columns=["vendedor", "nfs", "dias_em_aberto_max", "produtos_distintos"],
        errors="ignore",
    )
    ordered_consolidado_cols = [
        "empresa",
        "cliente",
        "quantidade_total",
        "valor_remetido",
        "lotes",
        "vencimento_lote",
        "saldo_lote",
        "primeiro_envio",
        "ultimo_envio",
    ]
    consolidado_display = consolidado_display[[col for col in ordered_consolidado_cols if col in consolidado_display.columns]]
    st.dataframe(consolidado_display, use_container_width=True, hide_index=True)

    st.markdown("### Detalhamento da NF e itens")
    det = detail[
        [
            "data",
            "empresa",
            "numero_nf",
            "cliente",
            "vendedor",
            "natureza_label",
            "cfop",
            "produto_codigo",
            "produto",
            "quantidade",
            "valor_unitario",
            "valor_total",
            "lote",
            "vencimento_lote",
            "dias_em_aberto",
        ]
    ].copy()
    det["quantidade"] = det["quantidade"].map(fmt_int_br)
    det["valor_unitario"] = det["valor_unitario"].map(fmt_brl_table)
    det["valor_total"] = det["valor_total"].map(fmt_brl_table)
    det["vencimento_lote"] = pd.to_datetime(det["vencimento_lote"], errors="coerce").dt.strftime("%d/%m/%Y").fillna("S/N")
    det["lote"] = det["lote"].fillna("S/N")
    st.dataframe(det.sort_values("data", ascending=False), use_container_width=True, hide_index=True)


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


def filter_customer_scope(df: pd.DataFrame, selected_customer: str, columns: list[str] | None = None) -> pd.DataFrame:
    if df.empty or selected_customer == "TODOS":
        return df
    columns = columns or ["cliente", "customer_name"]
    mask = pd.Series(False, index=df.index)
    selected_key = str(selected_customer).strip().upper()
    for column in columns:
        if column in df.columns:
            mask = mask | df[column].fillna("").astype(str).str.strip().str.upper().eq(selected_key)
    return df[mask]


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


def build_remote_realizado_sheet(view_df: pd.DataFrame) -> pd.DataFrame:
    if view_df.empty:
        return pd.DataFrame()
    out = view_df.copy()
    if "data" in out.columns:
        out["data"] = pd.to_datetime(out["data"], errors="coerce")
    if "receita" in out.columns:
        out["receita"] = pd.to_numeric(out["receita"], errors="coerce").fillna(0)
    for column in ["cliente", "vendedor", "vendedor_id", "empresa"]:
        if column not in out.columns:
            out[column] = ""
        out[column] = out[column].fillna("").astype(str).str.strip()
    return out


def canonicalize_targets_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    def _fill_alias(target: str, source: str) -> None:
        if source not in out.columns:
            return
        source_series = out[source]
        if target not in out.columns:
            out[target] = source_series
            return
        target_series = out[target]
        if pd.api.types.is_numeric_dtype(target_series):
            missing = target_series.isna()
        else:
            missing = target_series.fillna("").astype(str).str.strip().eq("")
        if missing.any():
            out.loc[missing, target] = source_series.loc[missing]

    rename_map = {}
    for source, target in [
        ("target_year", "ano"),
        ("period_type", "periodo_tipo"),
        ("month_num", "mes"),
        ("quarter_num", "quarter"),
        ("state", "estado"),
        ("company", "empresa"),
        ("company_code", "empresa"),
        ("sales_rep_code", "vendedor_id"),
        ("sales_rep_id", "vendedor_id"),
        ("sales_rep_name", "vendedor"),
        ("target_value", "meta_valor"),
        ("actual_value", "realizado_valor"),
        ("target_volume", "meta_volume"),
        ("actual_volume", "realizado_volume"),
        ("gap_value", "gap_valor"),
        ("attainment_pct", "atingimento_pct"),
        ("notes", "observacoes"),
    ]:
        if source in out.columns and target not in out.columns:
            rename_map[source] = target
    if rename_map:
        out = out.rename(columns=rename_map)

    for source, target in [
        ("target_year", "ano"),
        ("period_type", "periodo_tipo"),
        ("month_num", "mes"),
        ("quarter_num", "quarter"),
        ("state", "estado"),
        ("company", "empresa"),
        ("company_code", "empresa"),
        ("sales_rep_code", "vendedor_id"),
        ("sales_rep_id", "vendedor_id"),
        ("sales_rep_name", "vendedor"),
        ("target_value", "meta_valor"),
        ("actual_value", "realizado_valor"),
        ("target_volume", "meta_volume"),
        ("actual_volume", "realizado_volume"),
        ("gap_value", "gap_valor"),
        ("attainment_pct", "atingimento_pct"),
        ("notes", "observacoes"),
    ]:
        _fill_alias(target, source)

    defaults = {
        "ano": pd.NA,
        "periodo_tipo": "",
        "mes": pd.NA,
        "quarter": pd.NA,
        "estado": "",
        "empresa": "",
        "vendedor_id": "",
        "vendedor": "",
        "meta_valor": 0.0,
        "realizado_valor": 0.0,
        "meta_volume": pd.NA,
        "realizado_volume": pd.NA,
        "gap_valor": 0.0,
        "atingimento_pct": 0.0,
        "status": "",
        "observacoes": "",
    }
    for column, default in defaults.items():
        if column not in out.columns:
            out[column] = default

    for column in ["ano", "mes", "quarter", "meta_valor", "realizado_valor", "gap_valor", "atingimento_pct"]:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    for column in ["periodo_tipo", "estado", "empresa", "vendedor_id", "vendedor", "status", "observacoes"]:
        out[column] = out[column].fillna("").astype(str).str.strip()
    out["periodo_tipo"] = out["periodo_tipo"].str.upper()
    out["estado"] = out["estado"].str.upper()
    out["empresa"] = out["empresa"].str.upper()
    return out


def build_remote_metas_sheet(view_df: pd.DataFrame) -> pd.DataFrame:
    if view_df.empty:
        return pd.DataFrame()
    out = canonicalize_targets_frame(view_df)
    if "periodo_tipo" not in out.columns:
        return pd.DataFrame()
    out["ano"] = pd.to_numeric(out.get("ano"), errors="coerce")
    out["mes"] = pd.to_numeric(out.get("mes"), errors="coerce")
    out["quarter"] = pd.to_numeric(out.get("quarter"), errors="coerce")
    period_type = out["periodo_tipo"].astype(str).str.upper()
    month_mask = period_type == "MONTH"
    quarter_mask = period_type == "QUARTER"
    out = out[
        (month_mask & out["ano"].notna() & out["mes"].notna())
        | (quarter_mask & out["ano"].notna() & out["quarter"].notna())
    ].copy()
    if out.empty:
        return pd.DataFrame(columns=["data", "vendedor", "vendedor_id", "meta", "realizado"])
    out["data"] = pd.NaT
    if month_mask.any():
        month_rows = out.index[month_mask.loc[out.index] if hasattr(month_mask, "loc") else month_mask]
        if len(month_rows):
            out.loc[month_rows, "data"] = pd.to_datetime(
                dict(
                    year=out.loc[month_rows, "ano"].astype(int),
                    month=out.loc[month_rows, "mes"].astype(int),
                    day=1,
                ),
                errors="coerce",
            )
    if quarter_mask.any():
        quarter_rows = out.index[quarter_mask.loc[out.index] if hasattr(quarter_mask, "loc") else quarter_mask]
        if len(quarter_rows):
            quarter_start_month = ((out.loc[quarter_rows, "quarter"].astype(int) - 1) * 3) + 1
            out.loc[quarter_rows, "data"] = pd.to_datetime(
                dict(
                    year=out.loc[quarter_rows, "ano"].astype(int),
                    month=quarter_start_month.astype(int),
                    day=1,
                ),
                errors="coerce",
            )
    out["meta"] = pd.to_numeric(out.get("meta_valor"), errors="coerce").fillna(0)
    out["realizado"] = pd.to_numeric(out.get("realizado_valor"), errors="coerce").fillna(0)
    out["vendedor"] = out.get("vendedor", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    out["vendedor_id"] = out.get("vendedor_id", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    return out[["data", "vendedor", "vendedor_id", "meta", "realizado"]]


def build_targets_realizado_summary(
    realized_df: pd.DataFrame,
    *,
    target_year: int,
    period_type: str,
    month_num: int | None,
    quarter_num: int | None,
    ytd: bool,
    state: str | None,
    selected_company: str,
    selected_vendor: str,
    selected_vendor_candidates: set[str],
) -> dict:
    empty_series = pd.DataFrame(columns=["periodo", "realizado_valor"])
    empty_uf = pd.DataFrame(columns=["estado", "realizado_valor"])
    empty_vendor = pd.DataFrame(columns=["vendedor_label", "vendedor_id", "vendedor", "realizado_valor"])
    empty_heatmap = pd.DataFrame(columns=["estado", "periodo", "realizado"])
    if realized_df.empty or "data" not in realized_df.columns or "receita" not in realized_df.columns:
        return {
            "realizado": 0.0,
            "series": empty_series,
            "uf": empty_uf,
            "vendedor": empty_vendor,
            "heatmap": empty_heatmap,
        }

    out = realized_df.copy()
    out["data"] = pd.to_datetime(out["data"], errors="coerce")
    out["receita"] = pd.to_numeric(out["receita"], errors="coerce").fillna(0)
    out = out[out["data"].notna()].copy()
    if out.empty:
        return {
            "realizado": 0.0,
            "series": empty_series,
            "uf": empty_uf,
            "vendedor": empty_vendor,
            "heatmap": empty_heatmap,
        }

    out = normalize_vendor_identity(
        out,
        load_bling_vendor_map(),
        _build_vendor_alias_map(pd.DataFrame(), load_bling_vendor_map(), load_vendor_links()),
    )
    out["vendedor_label"] = out.get("vendedor", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    if "vendedor_id" in out.columns:
        out["vendedor_label"] = out["vendedor_label"].mask(
            out["vendedor_label"].eq(""),
            out["vendedor_id"].fillna("").astype(str).str.strip(),
        )
    out["vendedor_label"] = out["vendedor_label"].replace("", "SEM_VENDEDOR")

    if selected_company != "TODOS":
        out = filter_company_scope(out, selected_company)
    if selected_vendor != "TODOS":
        out = filter_vendor_scope(out, selected_vendor, ["vendedor", "vendedor_id"], selected_vendor_candidates)
    if state:
        state_value = str(state).strip().upper()
        state_series = out.get("customer_state", out.get("estado", pd.Series("", index=out.index)))
        out = out[state_series.fillna("").astype(str).str.strip().str.upper() == state_value]
    out = filter_period_scope(out, target_year, month_num, ytd, quarter_num)
    if out.empty:
        return {
            "realizado": 0.0,
            "series": empty_series,
            "uf": empty_uf,
            "vendedor": empty_vendor,
            "heatmap": empty_heatmap,
        }

    if str(period_type).upper() == "QUARTER":
        series = (
            out.assign(periodo=((out["data"].dt.month - 1) // 3 + 1))
            .groupby("periodo", dropna=False)["receita"]
            .sum()
            .reset_index()
            .rename(columns={"receita": "realizado_valor"})
        )
    else:
        series = (
            out.assign(periodo=out["data"].dt.month)
            .groupby("periodo", dropna=False)["receita"]
            .sum()
            .reset_index()
            .rename(columns={"receita": "realizado_valor"})
        )
    uf = (
        out.assign(estado=out.get("customer_state", out.get("estado", pd.Series("", index=out.index))).fillna("").astype(str).str.strip().str.upper())
        .groupby("estado", dropna=False)["receita"]
        .sum()
        .reset_index()
        .rename(columns={"receita": "realizado_valor"})
    )
    group_cols = ["vendedor_label"]
    agg_map: dict[str, str] = {"receita": "sum"}
    if "vendedor_id" in out.columns:
        agg_map["vendedor_id"] = "first"
    if "vendedor" in out.columns:
        agg_map["vendedor"] = "first"
    vendedor = out.groupby(group_cols, dropna=False).agg(agg_map).reset_index().rename(columns={"receita": "realizado_valor"})
    heatmap = (
        out.assign(
            estado=out.get("customer_state", out.get("estado", pd.Series("", index=out.index))).fillna("").astype(str).str.strip().str.upper(),
            periodo=((out["data"].dt.month - 1) // 3 + 1) if str(period_type).upper() == "QUARTER" else out["data"].dt.month,
        )
        .groupby(["estado", "periodo"], dropna=False)["receita"]
        .sum()
        .reset_index()
        .rename(columns={"receita": "realizado"})
    )
    return {"realizado": float(out["receita"].sum()), "series": series, "uf": uf, "vendedor": vendedor, "heatmap": heatmap}


def resolve_realizado_sheet(sales_scope: str, use_bling_source: bool) -> tuple[pd.DataFrame, str]:
    if use_bling_source:
        remote_bling = build_remote_realizado_sheet(load_bling_sales_realized_view())
        if not remote_bling.empty:
            return upper_dashboard_text(filter_sales_nature_scope(remote_bling, sales_scope)), "vw_bling_sales_realized"

        remote_finance = build_remote_realizado_sheet(load_sales_realized_view())
        if not remote_finance.empty:
            return upper_dashboard_text(filter_sales_nature_scope(remote_finance, sales_scope)), "vw_sales_realized_summary"

        local_bling = load_bling_realizado()
        if not local_bling.empty:
            return upper_dashboard_text(filter_sales_nature_scope(local_bling, sales_scope)), "bling_cache_local"

    return pd.DataFrame(), ""


def resolve_metas_sheet() -> tuple[pd.DataFrame, str]:
    remote_metas = build_remote_metas_sheet(load_sales_targets_view())
    if remote_metas.empty:
        return pd.DataFrame(), ""
    return upper_dashboard_text(remote_metas), "vw_sales_targets_summary"


def build_nfe_monthly_audit_base(
    target_year: int,
    selected_company: str,
    selected_vendor: str,
    selected_vendor_candidates: set[str],
    selected_month: int | None,
    effective_ytd: bool,
    selected_quarter: int | None,
) -> pd.DataFrame:
    detail = load_bling_nfe_detail(0)
    if detail.empty:
        return pd.DataFrame(columns=["data", "valor"])

    out = detail.copy()
    out["data"] = pd.to_datetime(out["data"], errors="coerce")
    out["valor_nf"] = pd.to_numeric(out["valor_nf"], errors="coerce")
    out = out[out["data"].notna()].copy()
    out = out[out["data"].dt.year == int(target_year)]
    if selected_company != "TODOS":
        out = filter_company_scope(out, selected_company)
    if selected_vendor != "TODOS":
        out = filter_vendor_scope(out, selected_vendor, ["vendedor", "vendedor_id"], selected_vendor_candidates)
    out = filter_period_scope(out, target_year, selected_month, effective_ytd, selected_quarter)
    if out.empty:
        return pd.DataFrame(columns=["data", "valor"])

    nf_view = (
        out.groupby(["nfe_id", "data"], dropna=False)["valor_nf"]
        .max()
        .reset_index()
        .rename(columns={"valor_nf": "valor"})
    )
    monthly = nf_view.groupby(nf_view["data"].dt.to_period("M"))["valor"].sum().reset_index()
    monthly["data"] = monthly["data"].dt.to_timestamp()
    return monthly


def overlay_targets_actuals_from_realizado(
    targets_df: pd.DataFrame,
    realized_df: pd.DataFrame,
    *,
    year_col: str,
    period_type_col: str,
    month_col: str | None,
    quarter_col: str | None,
    state_col: str | None,
    vendor_col: str | None,
    company_col: str | None,
    actual_col: str,
    gap_col: str | None = None,
) -> pd.DataFrame:
    if targets_df.empty or realized_df.empty:
        return targets_df

    out = canonicalize_targets_frame(targets_df)
    sales = realized_df.copy()
    if "data" not in sales.columns or "receita" not in sales.columns:
        return out
    sales["data"] = pd.to_datetime(sales["data"], errors="coerce")
    sales["receita"] = pd.to_numeric(sales["receita"], errors="coerce").fillna(0)
    sales = sales[sales["data"].notna()].copy()
    if sales.empty:
        return out

    sales["ano"] = sales["data"].dt.year.astype(int)
    sales["mes"] = sales["data"].dt.month.astype(int)
    sales["quarter"] = ((sales["mes"] - 1) // 3 + 1).astype(int)
    sales["empresa_norm"] = sales.get("empresa", pd.Series("", index=sales.index)).fillna("").astype(str).str.strip().str.upper()
    sales["vendedor_id_norm"] = sales.get("vendedor_id", pd.Series("", index=sales.index)).fillna("").astype(str).str.strip().map(_vendor_key)
    sales["vendedor_name_norm"] = sales.get("vendedor", pd.Series("", index=sales.index)).fillna("").astype(str).str.strip().map(_vendor_key)
    sales["estado_norm"] = sales.get("customer_state", sales.get("estado", pd.Series("", index=sales.index))).fillna("").astype(str).str.strip().str.upper()

    def _pick_vendor_value(row: pd.Series, columns: list[str | None]) -> str:
        for column in columns:
            if not column:
                continue
            if column not in row.index:
                continue
            value = str(row.get(column, "") or "").strip()
            if value:
                return value
        return ""

    def _row_realizado(row: pd.Series) -> float:
        try:
            ano = int(pd.to_numeric(row.get(year_col), errors="coerce"))
        except Exception:
            return 0.0
        period_type = str(row.get(period_type_col, "")).strip().upper()
        month_value = int(pd.to_numeric(row.get(month_col), errors="coerce")) if month_col and pd.notna(pd.to_numeric(row.get(month_col), errors="coerce")) else None
        quarter_value = int(pd.to_numeric(row.get(quarter_col), errors="coerce")) if quarter_col and pd.notna(pd.to_numeric(row.get(quarter_col), errors="coerce")) else None
        state_value = str(row.get(state_col, "") or "").strip().upper() if state_col else ""
        vendor_value = _vendor_key(
            _pick_vendor_value(
                row,
                [
                    vendor_col,
                    "vendedor_label",
                    "vendedor",
                    "sales_rep_name",
                    "sales_rep_code",
                    "vendedor_id",
                ],
            )
        )
        company_value = str(row.get(company_col, "") or "").strip().upper() if company_col else ""

        match = sales[sales["ano"] == ano]
        if period_type == "MONTH" and month_value is not None:
            match = match[match["mes"] == month_value]
        elif period_type == "QUARTER" and quarter_value is not None:
            match = match[match["quarter"] == quarter_value]
        if company_value:
            match = match[match["empresa_norm"] == company_value]
        if vendor_value:
            match = match[
                (match["vendedor_id_norm"] == vendor_value)
                | (match["vendedor_name_norm"] == vendor_value)
            ]
        if state_value:
            match = match[match["estado_norm"] == state_value]
        if match.empty:
            return 0.0
        return float(match["receita"].sum())

    out[actual_col] = out.apply(_row_realizado, axis=1)
    if gap_col:
        target_candidates = ["meta_valor", "target_value"]
        target_col = next((column for column in target_candidates if column in out.columns), None)
        if target_col:
            out[gap_col] = pd.to_numeric(out[actual_col], errors="coerce").fillna(0) - pd.to_numeric(
                out[target_col], errors="coerce"
            ).fillna(0)
    return out


def filter_sales_nature_scope(df: pd.DataFrame, selected_scope: str) -> pd.DataFrame:
    if df.empty or selected_scope == "Tudo":
        return df
    out = df.copy()
    cfop = out.get("cfop", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    natureza_txt = (
        out.get("natureza_label", pd.Series("", index=out.index)).fillna("").astype(str)
        + " "
        + out.get("natureza", pd.Series("", index=out.index)).fillna("").astype(str)
        + " "
        + cfop
    ).str.upper()
    receita_source = out.get("receita", out.get("valor_total", pd.Series(0, index=out.index)))
    receita = pd.to_numeric(receita_source, errors="coerce").fillna(0)
    vendedor_id = out.get("vendedor_id", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    is_return_cfop = cfop.str.match(r"^[1267]20[1-9]$|^[12]201$|^[56]202$")
    is_non_sale_cfop = cfop.isin({"5910", "6910", "5911", "6911", "5917", "6917", "5949", "6949"})
    is_devolucao = natureza_txt.str.contains("DEVOL|RETORNO|ESTORNO|CANCEL", regex=True) | receita.lt(0) | is_return_cfop
    is_non_sale = natureza_txt.str.contains("REMESSA|CONSIGN|BONIFIC", regex=True) | is_non_sale_cfop
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
    df = canonicalize_targets_frame(view_df)
    if "ano" in df.columns:
        df = df[pd.to_numeric(df["ano"], errors="coerce").eq(int(target_year))]
    if "periodo_tipo" in df.columns:
        df = df[df["periodo_tipo"].astype(str).str.upper() == str(period_type).upper()]
    if month_num is not None and "mes" in df.columns:
        df = df[pd.to_numeric(df["mes"], errors="coerce").eq(int(month_num))]
    if quarter_num is not None and "quarter" in df.columns:
        df = df[pd.to_numeric(df["quarter"], errors="coerce").eq(int(quarter_num))]
    if ytd and month_num is None and quarter_num is None and str(period_type).upper() == "MONTH" and "mes" in df.columns:
        ref_today = pd.Timestamp.today()
        max_month = ref_today.month if int(target_year) == ref_today.year else 12
        df = df[pd.to_numeric(df["mes"], errors="coerce").between(1, max_month)]
    if state and "estado" in df.columns:
        df = df[df["estado"].astype(str).str.upper() == str(state).upper()]
    if sales_rep_code and "vendedor_id" in df.columns:
        df = df[df["vendedor_id"].map(_vendor_key) == _vendor_key(sales_rep_code)]
    if statuses and "status" in df.columns:
        df = df[df["status"].astype(str).str.upper().isin([str(item).upper() for item in statuses])]
    return df


def format_targets_listing(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = canonicalize_targets_frame(df)
    if out.columns.duplicated().any():
        # Keep the first occurrence to avoid pandas returning a DataFrame for duplicate labels.
        out = out.loc[:, ~out.columns.duplicated()].copy()
    out = upper_dashboard_text(out)
    if "meta_valor" in out.columns:
        out["meta_valor"] = pd.to_numeric(out["meta_valor"], errors="coerce").fillna(0)
    if "realizado_valor" in out.columns:
        out["realizado_valor"] = pd.to_numeric(out["realizado_valor"], errors="coerce").fillna(0)
    if {"meta_valor", "realizado_valor"}.issubset(out.columns):
        out["atingimento_pct"] = out.apply(
            lambda row: (row["realizado_valor"] / row["meta_valor"] * 100) if row["meta_valor"] else 0.0,
            axis=1,
        )
        out["gap_valor"] = out["realizado_valor"] - out["meta_valor"]
    if "status" in out.columns:
        out["status"] = out["status"].apply(status_chip)
    if "meta_valor" in out.columns:
        out["meta_valor"] = out["meta_valor"].map(fmt_brl_full)
    if "realizado_valor" in out.columns:
        out["realizado_valor"] = out["realizado_valor"].map(fmt_brl_full)
    if "gap_valor" in out.columns:
        out["gap_valor"] = out["gap_valor"].map(fmt_brl_full)
    if "atingimento_pct" in out.columns:
        out["atingimento_pct"] = out["atingimento_pct"].map(fmt_pct)
    return out


def filter_targets_company_scope(df: pd.DataFrame, selected_company: str) -> pd.DataFrame:
    if df.empty or selected_company == "TODOS":
        return df
    company = str(selected_company).strip().upper()
    if not company:
        return df
    if "empresa" in df.columns:
        series = df["empresa"].fillna("").astype(str).str.strip().str.upper()
        return df[series.eq(company)]

    vendor_ids: set[str] = set()
    vendor_names: set[str] = set()
    vendor_links = load_vendor_links()
    if not vendor_links.empty and {"vendedor_id", "empresa"}.issubset(vendor_links.columns):
        links_company = vendor_links[vendor_links["empresa"].astype(str).str.upper() == company].copy()
        link_ids = links_company["vendedor_id"].dropna().astype(str).str.strip().tolist()
        vendor_ids.update([v for v in link_ids if v])
        for column in ["nome_meta", "nome_exibicao"]:
            if column in links_company.columns:
                names = links_company[column].dropna().astype(str).str.strip().tolist()
                vendor_names.update([_vendor_key(v) for v in names if str(v).strip()])

    vendor_map = load_bling_vendor_map()
    if not vendor_map.empty and {"vendedor_id", "empresa"}.issubset(vendor_map.columns):
        map_company = vendor_map[vendor_map["empresa"].astype(str).str.upper() == company].copy()
        map_ids = map_company["vendedor_id"].dropna().astype(str).str.strip().tolist()
        vendor_ids.update([v for v in map_ids if v])
        if "vendedor" in map_company.columns:
            names = map_company["vendedor"].dropna().astype(str).str.strip().tolist()
            vendor_names.update([_vendor_key(v) for v in names if str(v).strip()])

    if not vendor_ids and not vendor_names:
        return df.iloc[0:0].copy()

    out = df.copy()
    if "sales_rep_code" in out.columns:
        series = out["sales_rep_code"].fillna("").astype(str).str.strip()
        return out[series.isin(vendor_ids) | series.map(_vendor_key).isin(vendor_names)]
    if "vendedor_id" in out.columns:
        series = out["vendedor_id"].fillna("").astype(str).str.strip()
        return out[series.isin(vendor_ids) | series.map(_vendor_key).isin(vendor_names)]
    return out


def filter_local_targets_scope(
    df: pd.DataFrame,
    selected_company: str,
    selected_vendor: str,
    selected_vendor_candidates: set[str],
) -> pd.DataFrame:
    out = filter_targets_company_scope(df, selected_company)
    if selected_vendor != "TODOS" and not out.empty and "vendedor_id" in out.columns:
        out = out[
            out["vendedor_id"].fillna("").astype(str).str.strip().map(_vendor_key).isin(selected_vendor_candidates)
        ]
    return out


def collapse_targets_rows(df: pd.DataFrame, *, include_state: bool = True) -> pd.DataFrame:
    if df.empty:
        return df
    out = canonicalize_targets_frame(df)

    out = normalize_vendor_identity(
        out,
        load_bling_vendor_map(),
        _build_vendor_alias_map(pd.DataFrame(), load_bling_vendor_map(), load_vendor_links()),
    )
    out["vendedor_label"] = out.get("vendedor", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    if "vendedor_id" in out.columns:
        out["vendedor_label"] = out["vendedor_label"].mask(
            out["vendedor_label"].eq(""),
            out["vendedor_id"].fillna("").astype(str).str.strip(),
        )
    out["vendedor_label"] = out["vendedor_label"].replace("", "SEM_VENDEDOR")

    if "empresa" in out.columns:
        out["empresa"] = out["empresa"].fillna("").astype(str).str.strip().str.upper()
    if include_state and "estado" in out.columns:
        out["estado"] = out["estado"].fillna("").astype(str).str.strip().str.upper()
    elif not include_state and "estado" in out.columns:
        out = out.drop(columns=["estado"])
    if "periodo_tipo" in out.columns:
        out["periodo_tipo"] = out["periodo_tipo"].fillna("").astype(str).str.strip().str.upper()
    if "vendedor_id" in out.columns:
        out["vendedor_id"] = out["vendedor_id"].fillna("").astype(str).str.strip()
    if "vendedor" in out.columns:
        out["vendedor"] = out["vendedor"].fillna("").astype(str).str.strip()
    if "mes" in out.columns:
        out["mes"] = pd.to_numeric(out["mes"], errors="coerce")
    if "quarter" in out.columns:
        out["quarter"] = pd.to_numeric(out["quarter"], errors="coerce")
    if "ano" in out.columns:
        out["ano"] = pd.to_numeric(out["ano"], errors="coerce")

    key_cols = [c for c in ["ano", "periodo_tipo", "mes", "quarter", "empresa"] if c in out.columns]
    if include_state and "estado" in out.columns:
        key_cols.append("estado")
    key_cols.append("vendedor_label")

    if not key_cols:
        return out

    agg: dict[str, str] = {}
    for column in out.columns:
        if column in key_cols:
            continue
        if column in {"meta_valor", "meta_volume"}:
            agg[column] = "sum"
        elif column in {"realizado_valor", "realizado_volume"}:
            agg[column] = "max"
        else:
            agg[column] = "first"

    grouped = out.groupby(key_cols, dropna=False).agg(agg).reset_index()
    if "vendedor" not in grouped.columns:
        grouped["vendedor"] = grouped["vendedor_label"]
    else:
        grouped["vendedor"] = grouped["vendedor"].where(grouped["vendedor"].astype(str).str.strip() != "", grouped["vendedor_label"])
    return grouped


def build_local_targets_summary(
    df: pd.DataFrame,
    period_type: str,
    realized_summary: dict | None = None,
) -> dict:
    empty_series = pd.DataFrame(columns=["meta_valor", "realizado_valor"])
    df = collapse_targets_rows(df)
    if df.empty:
        return {
            "kpis": {"meta": 0.0, "realizado": 0.0, "atingimento_pct": 0.0, "delta": 0.0},
            "series": empty_series,
            "uf": pd.DataFrame(columns=["estado", "meta_valor", "realizado_valor"]),
            "vendedor": pd.DataFrame(columns=["vendedor_label", "vendedor_id", "vendedor", "meta_valor", "realizado_valor"]),
        }
    out = df.copy()
    out["meta_valor"] = pd.to_numeric(out["meta_valor"], errors="coerce").fillna(0)
    if "quarter" not in out.columns or out["quarter"].isna().all():
        out["quarter"] = out["mes"].apply(lambda m: ((int(m) - 1) // 3 + 1) if pd.notna(m) else None)
    if str(period_type).upper() == "QUARTER":
        series = out.groupby(["quarter"], dropna=False)[["meta_valor"]].sum().reset_index()
    else:
        series = out.groupby(["mes"], dropna=False)[["meta_valor"]].sum().reset_index()
    uf = out.groupby("estado", dropna=False)[["meta_valor"]].sum().reset_index()
    vendedor = out.groupby("vendedor_label", dropna=False)[["meta_valor"]].sum().reset_index()
    if "vendedor_id" in out.columns:
        first_ids = out.groupby("vendedor_label", dropna=False)["vendedor_id"].first().reset_index()
        vendedor = vendedor.merge(first_ids, on="vendedor_label", how="left")
    if "vendedor" in out.columns:
        first_names = out.groupby("vendedor_label", dropna=False)["vendedor"].first().reset_index()
        vendedor = vendedor.merge(first_names, on="vendedor_label", how="left")
    meta_total = float(out["meta_valor"].sum())
    if realized_summary and not realized_summary.get("series", pd.DataFrame()).empty and not series.empty:
        actual_series = realized_summary["series"].copy()
        if "quarter" in series.columns and "periodo" in actual_series.columns:
            series = series.merge(actual_series, left_on="quarter", right_on="periodo", how="left").drop(columns=["periodo"], errors="ignore")
        elif "mes" in series.columns and "periodo" in actual_series.columns:
            series = series.merge(actual_series, left_on="mes", right_on="periodo", how="left").drop(columns=["periodo"], errors="ignore")
    if realized_summary and not realized_summary.get("uf", pd.DataFrame()).empty and not uf.empty:
        uf = uf.merge(realized_summary["uf"], on="estado", how="left")
    if realized_summary and not realized_summary.get("vendedor", pd.DataFrame()).empty and not vendedor.empty:
        actual_vendor = realized_summary["vendedor"].copy()
        if "vendedor_label" in actual_vendor.columns:
            vendedor = vendedor.merge(
                actual_vendor[[c for c in ["vendedor_label", "realizado_valor"] if c in actual_vendor.columns]],
                on="vendedor_label",
                how="left",
            )
    realizado_total = (
        float(realized_summary["realizado"])
        if realized_summary and "realizado" in realized_summary
        else 0.0
    )
    return {
        "kpis": {
            "meta": meta_total,
            "realizado": realizado_total,
            "atingimento_pct": (realizado_total / meta_total * 100) if meta_total else 0.0,
            "delta": realizado_total - meta_total,
        },
        "series": series,
        "uf": uf,
        "vendedor": vendedor,
    }


def render_targets_executive_rankings(
    vendedor_df: pd.DataFrame,
    uf_df: pd.DataFrame,
    vendedor_name_col: str,
    vendedor_id_col: str,
) -> None:
    rank_left, rank_right = st.columns(2)

    with rank_left:
        if not vendedor_df.empty:
            vendor_rank = vendedor_df.copy()
            vendor_rank["meta_valor"] = pd.to_numeric(vendor_rank["meta_valor"], errors="coerce").fillna(0)
            vendor_rank["realizado_valor"] = pd.to_numeric(vendor_rank["realizado_valor"], errors="coerce").fillna(0)
            vendor_rank["gap_valor"] = vendor_rank["realizado_valor"] - vendor_rank["meta_valor"]
            vendor_rank["atingimento_pct"] = vendor_rank.apply(
                lambda row: (row["realizado_valor"] / row["meta_valor"] * 100) if row["meta_valor"] else 0.0,
                axis=1,
            )
            if vendedor_name_col in vendor_rank.columns:
                vendor_rank["vendedor_label"] = vendor_rank[vendedor_name_col].fillna("").astype(str).str.strip()
            else:
                vendor_rank["vendedor_label"] = ""
            if vendedor_id_col in vendor_rank.columns:
                fallback_ids = vendor_rank[vendedor_id_col].fillna("").astype(str).str.strip()
                vendor_rank["vendedor_label"] = vendor_rank["vendedor_label"].mask(
                    vendor_rank["vendedor_label"].eq(""),
                    fallback_ids,
                )
            vendor_rank["vendedor_label"] = vendor_rank["vendedor_label"].replace("", "SEM_VENDEDOR")

            top_gap = vendor_rank.sort_values("gap_valor").head(10).copy()
            top_gap["meta_valor"] = top_gap["meta_valor"].map(fmt_brl_full)
            top_gap["realizado_valor"] = top_gap["realizado_valor"].map(fmt_brl_full)
            top_gap["gap_valor"] = top_gap["gap_valor"].map(fmt_brl_full)
            top_gap["atingimento_pct"] = top_gap["atingimento_pct"].map(fmt_pct)
            st.markdown("#### Maiores Gaps")
            st.dataframe(
                top_gap[["vendedor_label", "meta_valor", "realizado_valor", "gap_valor", "atingimento_pct"]],
                hide_index=True,
                width="stretch",
            )

    with rank_right:
        if not vendedor_df.empty:
            vendor_att = vendedor_df.copy()
            vendor_att["meta_valor"] = pd.to_numeric(vendor_att["meta_valor"], errors="coerce").fillna(0)
            vendor_att["realizado_valor"] = pd.to_numeric(vendor_att["realizado_valor"], errors="coerce").fillna(0)
            vendor_att["atingimento_pct"] = vendor_att.apply(
                lambda row: (row["realizado_valor"] / row["meta_valor"] * 100) if row["meta_valor"] else 0.0,
                axis=1,
            )
            if vendedor_name_col in vendor_att.columns:
                vendor_att["vendedor_label"] = vendor_att[vendedor_name_col].fillna("").astype(str).str.strip()
            else:
                vendor_att["vendedor_label"] = ""
            if vendedor_id_col in vendor_att.columns:
                fallback_ids = vendor_att[vendedor_id_col].fillna("").astype(str).str.strip()
                vendor_att["vendedor_label"] = vendor_att["vendedor_label"].mask(
                    vendor_att["vendedor_label"].eq(""),
                    fallback_ids,
                )
            vendor_att["vendedor_label"] = vendor_att["vendedor_label"].replace("", "SEM_VENDEDOR")

            low_att = vendor_att.sort_values(["atingimento_pct", "meta_valor"], ascending=[True, False]).head(10).copy()
            low_att["meta_valor"] = low_att["meta_valor"].map(fmt_brl_full)
            low_att["realizado_valor"] = low_att["realizado_valor"].map(fmt_brl_full)
            low_att["atingimento_pct"] = low_att["atingimento_pct"].map(fmt_pct)
            st.markdown("#### Menor Atingimento")
            st.dataframe(
                low_att[["vendedor_label", "meta_valor", "realizado_valor", "atingimento_pct"]],
                hide_index=True,
                width="stretch",
            )

    if not uf_df.empty:
        uf_exec = uf_df.copy()
        uf_exec["meta_valor"] = pd.to_numeric(uf_exec["meta_valor"], errors="coerce").fillna(0)
        uf_exec["realizado_valor"] = pd.to_numeric(uf_exec["realizado_valor"], errors="coerce").fillna(0)
        uf_exec["atingimento_pct"] = uf_exec.apply(
            lambda row: (row["realizado_valor"] / row["meta_valor"] * 100) if row["meta_valor"] else 0.0,
            axis=1,
        )
        uf_exec["gap_valor"] = uf_exec["realizado_valor"] - uf_exec["meta_valor"]
        uf_exec = uf_exec.sort_values(["gap_valor", "atingimento_pct"], ascending=[True, True]).copy()
        uf_exec["meta_valor"] = uf_exec["meta_valor"].map(fmt_brl_full)
        uf_exec["realizado_valor"] = uf_exec["realizado_valor"].map(fmt_brl_full)
        uf_exec["gap_valor"] = uf_exec["gap_valor"].map(fmt_brl_full)
        uf_exec["atingimento_pct"] = uf_exec["atingimento_pct"].map(fmt_pct)
        st.markdown("#### Ranking por UF")
        st.dataframe(
            uf_exec[["estado", "meta_valor", "realizado_valor", "gap_valor", "atingimento_pct"]],
            hide_index=True,
            width="stretch",
        )


def numeric_column(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(0.0, index=df.index, dtype=float)
    return pd.to_numeric(df[column], errors="coerce").fillna(0)


def build_historical_sales_comparison(
    df: pd.DataFrame,
    target_year: int,
    selected_month: int | None,
    effective_ytd: bool,
    selected_quarter: int | None,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    if df.empty or not {"data", "receita"}.issubset(df.columns):
        return pd.DataFrame(), pd.DataFrame(), ""

    out = df.copy()
    out["data"] = pd.to_datetime(out["data"], errors="coerce")
    out["receita"] = pd.to_numeric(out["receita"], errors="coerce").fillna(0)
    out = out[out["data"].notna()].copy()
    if out.empty:
        return pd.DataFrame(), pd.DataFrame(), ""

    out["ano"] = out["data"].dt.year
    out["mes"] = out["data"].dt.month
    available_years = sorted(int(year_item) for year_item in out["ano"].dropna().astype(int).unique().tolist())
    if not available_years:
        return pd.DataFrame(), pd.DataFrame(), ""

    if selected_quarter is not None:
        q_start = (selected_quarter - 1) * 3 + 1
        q_end = q_start + 2
        period_df = out[out["mes"].between(q_start, q_end)].copy()
        period_label_txt = f"Q{selected_quarter}"
        period_months = list(range(q_start, q_end + 1))
    elif selected_month is not None and not effective_ytd:
        period_df = out[out["mes"] == int(selected_month)].copy()
        period_label_txt = pd.Timestamp(year=target_year, month=int(selected_month), day=1).strftime("%b").upper()
        period_months = [int(selected_month)]
    elif effective_ytd:
        ref_month = pd.Timestamp.today().month
        period_df = out[out["mes"] <= ref_month].copy()
        period_label_txt = f"YTD ate mes {ref_month:02d}"
        period_months = list(range(1, ref_month + 1))
    else:
        period_df = out.copy()
        period_label_txt = "Ano completo"
        period_months = list(range(1, 13))

    yearly = (
        period_df.groupby("ano", dropna=False)["receita"]
        .sum()
        .reset_index()
        .sort_values("ano")
    )
    if yearly.empty:
        return pd.DataFrame(), pd.DataFrame(), period_label_txt

    yearly["receita_anterior"] = yearly["receita"].shift(1)
    yearly["variacao_abs"] = yearly["receita"] - yearly["receita_anterior"]
    yearly["variacao_pct"] = (
        yearly["variacao_abs"] / yearly["receita_anterior"].replace(0, pd.NA) * 100
    ).fillna(0)

    monthly = (
        out[out["mes"].isin(period_months)]
        .groupby(["ano", "mes"], dropna=False)["receita"]
        .sum()
        .reset_index()
        .sort_values(["ano", "mes"])
    )
    if not monthly.empty:
        monthly["mes_label"] = monthly["mes"].apply(
            lambda month_item: pd.Timestamp(year=2000, month=int(month_item), day=1).strftime("%b").upper()
        )
        monthly["ano"] = monthly["ano"].astype(int)

    return yearly, monthly, period_label_txt


def comparison_period_months(
    target_year: int,
    selected_month: int | None,
    effective_ytd: bool,
    selected_quarter: int | None,
) -> tuple[list[int], str]:
    if selected_quarter is not None:
        q_start = (selected_quarter - 1) * 3 + 1
        q_end = q_start + 2
        return list(range(q_start, q_end + 1)), f"Q{selected_quarter}"
    if selected_month is not None and not effective_ytd:
        month_label = pd.Timestamp(year=target_year, month=int(selected_month), day=1).strftime("%b").upper()
        return [int(selected_month)], month_label
    if effective_ytd:
        ref_month = pd.Timestamp.today().month if int(target_year) == pd.Timestamp.today().year else 12
        return list(range(1, ref_month + 1)), f"YTD ate mes {ref_month:02d}"
    return list(range(1, 13)), "Ano completo"


def prepare_sales_comparison_actual(
    selected_years: list[int],
    target_year: int,
    selected_company: str,
    selected_vendor: str,
    selected_customer: str,
    selected_vendor_candidates: set[str],
    selected_scope: str,
    selected_month: int | None,
    effective_ytd: bool,
    selected_quarter: int | None,
) -> tuple[pd.DataFrame, str]:
    selected_years = sorted({int(item) for item in selected_years})
    period_months, period_label_txt = comparison_period_months(
        target_year,
        selected_month,
        effective_ytd,
        selected_quarter,
    )
    frames: list[pd.DataFrame] = []

    actual = sheets.get("realizado", pd.DataFrame()).copy()
    if selected_customer == "TODOS" and not actual.empty and {"data", "receita"}.issubset(actual.columns):
        actual["data"] = pd.to_datetime(actual["data"], errors="coerce")
        actual["receita"] = pd.to_numeric(actual["receita"], errors="coerce").fillna(0)
        actual = actual[actual["data"].notna()].copy()
        if not actual.empty:
            actual["ano"] = actual["data"].dt.year.astype(int)
            actual["mes"] = actual["data"].dt.month.astype(int)
            actual = actual[actual["ano"].isin(selected_years) & actual["mes"].isin(period_months)].copy()
            if not actual.empty:
                frames.append(actual[["data", "receita", "ano", "mes"]].copy())

    missing_years = sorted(set(selected_years) - set(frames[0]["ano"].unique().tolist()) if frames else set(selected_years))
    if missing_years:
        detail = load_bling_nfe_detail_years(tuple(selected_years))
        if not detail.empty and {"data", "valor_total"}.issubset(detail.columns):
            hist = detail.copy()
            hist["data"] = pd.to_datetime(hist["data"], errors="coerce")
            hist["receita"] = pd.to_numeric(hist["valor_total"], errors="coerce").fillna(0)
            hist = hist[hist["data"].notna()].copy()
            if selected_company != "TODOS":
                hist = filter_company_scope(hist, selected_company)
            if selected_vendor != "TODOS":
                hist = filter_vendor_scope(hist, selected_vendor, ["vendedor", "vendedor_id"], selected_vendor_candidates)
            if selected_customer != "TODOS":
                hist = filter_customer_scope(hist, selected_customer)
            if not hist.empty:
                hist = filter_sales_nature_scope(hist, selected_scope)
                hist["ano"] = hist["data"].dt.year.astype(int)
                hist["mes"] = hist["data"].dt.month.astype(int)
                hist = hist[hist["ano"].isin(missing_years) & hist["mes"].isin(period_months)].copy()
                if not hist.empty:
                    group_cols = ["nfe_id", "data", "ano", "mes"]
                    if "empresa" in hist.columns:
                        group_cols.append("empresa")
                    hist = hist.groupby(group_cols, dropna=False)["receita"].max().reset_index()
                    frames.append(hist[["data", "receita", "ano", "mes"]].copy())

    if not frames:
        return pd.DataFrame(), period_label_txt

    out = pd.concat(frames, ignore_index=True)
    out = out[out["ano"].isin(selected_years) & out["mes"].isin(period_months)].copy()
    if out.empty:
        return pd.DataFrame(), period_label_txt

    out["mes_label"] = out["mes"].apply(
        lambda month_item: pd.Timestamp(year=2000, month=int(month_item), day=1).strftime("%b").upper()
    )
    out["ano_label"] = out["ano"].astype(str)
    return out, period_label_txt


def prepare_sales_comparison_detail(
    selected_years: list[int],
    target_year: int,
    selected_company: str,
    selected_vendor: str,
    selected_customer: str,
    selected_vendor_candidates: set[str],
    selected_scope: str,
    selected_month: int | None,
    effective_ytd: bool,
    selected_quarter: int | None,
) -> tuple[pd.DataFrame, str]:
    detail = load_bling_nfe_detail(0)
    if detail.empty:
        return pd.DataFrame(), ""

    out = detail.copy()
    out["data"] = pd.to_datetime(out["data"], errors="coerce")
    out["valor_total"] = pd.to_numeric(out["valor_total"], errors="coerce").fillna(0)
    out["quantidade"] = pd.to_numeric(out["quantidade"], errors="coerce").fillna(0)
    out = out[out["data"].notna()].copy()
    if out.empty:
        return pd.DataFrame(), ""

    if selected_company != "TODOS":
        out = filter_company_scope(out, selected_company)
    if selected_vendor != "TODOS":
        out = filter_vendor_scope(out, selected_vendor, ["vendedor", "vendedor_id"], selected_vendor_candidates)
    if selected_customer != "TODOS":
        out = filter_customer_scope(out, selected_customer)

    if out.empty:
        return pd.DataFrame(), ""
    out["receita"] = out["valor_total"]
    out = filter_sales_nature_scope(out, selected_scope)
    out = out.drop(columns=["receita"], errors="ignore")
    if out.empty:
        return pd.DataFrame(), ""

    out["ano"] = out["data"].dt.year.astype(int)
    selected_years = sorted({int(item) for item in selected_years})
    out = out[out["ano"].isin(selected_years)].copy()
    if out.empty:
        return pd.DataFrame(), ""

    period_months, period_label_txt = comparison_period_months(
        target_year,
        selected_month,
        effective_ytd,
        selected_quarter,
    )
    out["mes"] = out["data"].dt.month.astype(int)
    out = out[out["mes"].isin(period_months)].copy()
    if out.empty:
        return pd.DataFrame(), period_label_txt

    out["mes_label"] = out["mes"].apply(
        lambda month_item: pd.Timestamp(year=2000, month=int(month_item), day=1).strftime("%b").upper()
    )
    out["produto"] = out["produto"].fillna("N/D").astype(str).str.strip().str.upper().replace("", "N/D")
    if "tipo_produto" not in out.columns:
        out["tipo_produto"] = "NF-E"
    out["tipo_produto"] = out["tipo_produto"].fillna("N/D").astype(str).str.strip().replace("", "N/D")
    out["cliente"] = out["cliente"].fillna("SEM_CLIENTE").astype(str).str.strip().str.upper().replace("", "SEM_CLIENTE")
    out["empresa"] = out["empresa"].fillna("").astype(str).str.upper()
    out["ano_label"] = out["ano"].astype(str)
    return out, period_label_txt


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


def build_targets_summary(
    view_df: pd.DataFrame,
    period_type: str,
    realized_summary: dict | None = None,
) -> dict:
    empty_series = pd.DataFrame(columns=["meta_valor", "realizado_valor"])
    view_df = collapse_targets_rows(view_df)
    if view_df.empty:
        return {
            "kpis": {"meta": 0.0, "realizado": 0.0, "atingimento_pct": 0.0, "delta": 0.0},
            "series": empty_series,
            "uf": pd.DataFrame(columns=["estado", "meta_valor", "realizado_valor"]),
            "vendedor": pd.DataFrame(columns=["vendedor_id", "vendedor", "meta_valor", "realizado_valor"]),
        }

    df = view_df.copy()
    if "meta_valor" in df.columns:
        df["meta_valor"] = pd.to_numeric(df["meta_valor"], errors="coerce").fillna(0)
    if "realizado_valor" in df.columns:
        df["realizado_valor"] = pd.to_numeric(df["realizado_valor"], errors="coerce").fillna(0)
    meta_total = float(numeric_column(df, "meta_valor").sum())
    period_col = "quarter" if str(period_type).upper() == "QUARTER" else "mes"

    series = pd.DataFrame()
    if period_col in df.columns and "meta_valor" in df.columns:
        series = (
            df.groupby(period_col, dropna=False)[["meta_valor"]]
            .sum()
            .reset_index()
            .rename(columns={period_col: "periodo"})
            .sort_values("periodo")
        )
        if str(period_type).upper() == "QUARTER":
            series = series.rename(columns={"periodo": "quarter"})
        else:
            series = series.rename(columns={"periodo": "mes"})

    uf = pd.DataFrame()
    if "estado" in df.columns and "meta_valor" in df.columns:
        uf = (
            df.groupby("estado", dropna=False)[["meta_valor"]]
            .sum()
            .reset_index()
            .sort_values("estado")
        )

    vendedor = pd.DataFrame()
    vendor_cols = [c for c in ["vendedor_id", "vendedor"] if c in df.columns]
    if vendor_cols and "meta_valor" in df.columns:
        vendedor = (
            df.groupby(vendor_cols, dropna=False)[["meta_valor"]]
            .sum()
            .reset_index()
            .sort_values(vendor_cols)
        )

    if realized_summary and not realized_summary.get("series", pd.DataFrame()).empty and not series.empty:
        actual_series = realized_summary["series"].copy()
        if "quarter" in series.columns and "periodo" in actual_series.columns:
            series = series.merge(actual_series, left_on="quarter", right_on="periodo", how="left").drop(columns=["periodo"], errors="ignore")
        elif "mes" in series.columns and "periodo" in actual_series.columns:
            series = series.merge(actual_series, left_on="mes", right_on="periodo", how="left").drop(columns=["periodo"], errors="ignore")
    if realized_summary and not realized_summary.get("uf", pd.DataFrame()).empty and not uf.empty:
        uf = uf.merge(realized_summary["uf"], on="estado", how="left")
    if realized_summary and not realized_summary.get("vendedor", pd.DataFrame()).empty and not vendedor.empty:
        actual_vendor = realized_summary["vendedor"].copy()
        join_cols = [c for c in vendor_cols if c in actual_vendor.columns]
        if join_cols:
            vendedor = vendedor.merge(actual_vendor, on=join_cols, how="left")

    realizado_total = (
        float(realized_summary["realizado"])
        if realized_summary and "realizado" in realized_summary
        else float(numeric_column(df, "realizado_valor").sum())
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
    if {"meta_valor", "realizado_valor"}.issubset(out.columns):
        out["atingimento_pct"] = out.apply(
            lambda row: (row["realizado_valor"] / row["meta_valor"] * 100) if row["meta_valor"] else 0.0,
            axis=1,
        )
        out["gap_valor"] = out["realizado_valor"] - out["meta_valor"]
    if "status" in out.columns:
        out["status"] = out["status"].apply(status_chip)
    return out


def build_sales_targets_template() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "ano",
            "periodo_tipo",
            "mes",
            "quarter",
            "estado",
            "vendedor_id",
            "empresa",
            "canal",
            "cultura",
            "meta_valor",
            "meta_volume",
            "realizado_valor",
            "realizado_volume",
            "status",
            "observacoes",
        ]
    )


def read_uploaded_targets_file(uploaded_file) -> pd.DataFrame:
    file_name = getattr(uploaded_file, "name", "planilha")
    suffix = Path(file_name).suffix.lower()
    raw = uploaded_file.getvalue()
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        xls = pd.ExcelFile(BytesIO(raw))
        sheet_name = "metas" if "metas" in xls.sheet_names else xls.sheet_names[0]
        return pd.read_excel(BytesIO(raw), sheet_name=sheet_name)
    if suffix in {".csv", ".txt"}:
        return pd.read_csv(BytesIO(raw), sep=None, engine="python", encoding="utf-8-sig")
    raise ValueError("Formato nao suportado. Use CSV ou XLSX.")


def save_uploaded_targets_file(uploaded_file, target_path: Path) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(uploaded_file.getvalue())
    return target_path


def run_sales_targets_import_script(input_path: Path, default_empresa: str, dry_run: bool = False) -> tuple[bool, str]:
    script_path = ROOT / "scripts" / "import_sales_targets.py"
    if not script_path.exists():
        return False, f"Script nao encontrado: {script_path}"
    args = [
        sys.executable,
        str(script_path),
        "--input",
        str(input_path),
        "--default-company",
        default_empresa,
    ]
    if dry_run:
        args.append("--dry-run")
    proc = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, check=False)
    output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    return proc.returncode == 0, output.strip()


def run_shared_targets_sync_script(default_empresa: str, dry_run: bool = False) -> tuple[bool, str]:
    script_path = ROOT / "scripts" / "import_sales_targets.py"
    if not script_path.exists():
        return False, f"Script nao encontrado: {script_path}"
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    sheet_name = os.getenv("GOOGLE_SHEETS_TARGETS_SHEET", "metas").strip()
    sheet_range = os.getenv("GOOGLE_SHEETS_TARGETS_RANGE", "A:O").strip()
    if not spreadsheet_id:
        return False, "GOOGLE_SHEETS_SPREADSHEET_ID nao configurado."
    args = [
        sys.executable,
        str(script_path),
        "--source",
        "google-sheet",
        "--spreadsheet-id",
        spreadsheet_id,
        "--sheet",
        sheet_name,
        "--range",
        sheet_range,
        "--default-company",
        default_empresa,
    ]
    if dry_run:
        args.append("--dry-run")
    proc = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, check=False)
    output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    return proc.returncode == 0, output.strip()


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
        <div class="sidebar-logo-shell">
            <div class="sidebar-logo-badge">
                <img
                    src="data:{sidebar_logo_mime};base64,{sidebar_logo_b64}"
                    alt="Logo"
                    style="width:205px; max-width:92%; height:auto; object-fit:contain; display:block;"
                />
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

if st.sidebar.button("Recarregar base"):
    for loader in [
        load_sheets,
        load_sales_targets_view,
        load_sales_pipeline_view,
        load_bling_sales_realized_view,
        load_sales_realized_view,
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
    "Consignacao",
    "Lab Comercial",
    "Comparativo de Vendas",
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
realizado_override, realizado_source = resolve_realizado_sheet(sales_scope, use_bling)
if not realizado_override.empty:
    sheets["realizado"] = realizado_override
    if use_bling and realizado_source == "bling_cache_local":
        st.sidebar.warning("Fonte remota indisponivel. Realizado em fallback pelo cache local do Bling.")

metas_override, metas_source = resolve_metas_sheet()
if not metas_override.empty:
    sheets["metas"] = metas_override

if use_bling and realizado_source:
    st.sidebar.caption(f"Realizado: {realizado_source}")
if metas_source:
    st.sidebar.caption(f"Metas: {metas_source}")

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

vendors = build_vendor_selector_options(
    vendor_scores,
    all_vendors_set,
    vendor_map,
    vendor_alias_map,
    show_inactive_vendors=show_inactive_vendors,
)
display_vendor_map = {option: option for option in vendors}
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

if PUBLIC_REVIEW and page == "Metas Comerciais":
    st.warning("Pagina indisponivel no modo de revisao publica.")
    st.stop()

metas_source_all = sheets.get("metas", pd.DataFrame()).copy()
realizado_source_all = sheets.get("realizado", pd.DataFrame()).copy()

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
    metas_source_all = metas_source_all.iloc[0:0].copy()

# Page A - Executive Cockpit
if page == "Executive Cockpit":
    st.subheader("Executive Cockpit")
    crm_pipeline_view = upper_dashboard_text(apply_acl_codes(load_sales_pipeline_view(), vendor_col="sales_rep_code"))
    crm_pipeline_view = filter_vendor_scope(
        crm_pipeline_view, sel_vendor, ["sales_rep_code", "sales_rep_name"], selected_vendor_candidates
    )
    crm_pipeline_view = filter_company_scope(crm_pipeline_view, sel_company)
    crm_pipeline_view = filter_pipeline_period(crm_pipeline_view, year, selected_month, effective_ytd, selected_quarter)
    cockpit_sheets = {key: value.copy() if isinstance(value, pd.DataFrame) else value for key, value in sheets.items()}
    kpis = compute_kpis(
        cockpit_sheets,
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
    run_rate_display = 0.0
    need_per_business_day = 0.0
    business_days_elapsed = 0
    business_days_total = 0
    business_days_remaining = 0

    if sales_scope == "Vendas efetivas" and meta_display > 0:
        if selected_month is not None and not effective_ytd and selected_quarter is None:
            period_start = pd.Timestamp(year=year, month=int(selected_month), day=1)
            period_end = period_start + pd.offsets.MonthEnd(0)
            if period_start.year == pd.Timestamp.today().year and period_start.month == pd.Timestamp.today().month:
                ref_end = min(pd.Timestamp.today().normalize(), period_end)
            else:
                ref_end = period_end
        elif selected_quarter is not None:
            q_start_month = (selected_quarter - 1) * 3 + 1
            period_start = pd.Timestamp(year=year, month=q_start_month, day=1)
            period_end = pd.Timestamp(year=year, month=q_start_month + 2, day=1) + pd.offsets.MonthEnd(0)
            if year == pd.Timestamp.today().year and pd.Timestamp.today().month <= q_start_month + 2:
                ref_end = min(pd.Timestamp.today().normalize(), period_end)
            else:
                ref_end = period_end
        else:
            period_start = pd.Timestamp(year=year, month=1, day=1)
            period_end = pd.Timestamp(year=year, month=12, day=31)
            if year == pd.Timestamp.today().year:
                ref_end = min(pd.Timestamp.today().normalize(), period_end)
            else:
                ref_end = period_end

        if ref_end >= period_start:
            business_days_elapsed = len(pd.bdate_range(period_start, ref_end))
            business_days_total = len(pd.bdate_range(period_start, period_end))
            business_days_remaining = max(business_days_total - business_days_elapsed, 0)
            run_rate_display = (kpis.realizado / business_days_elapsed) if business_days_elapsed else 0.0
            need_per_business_day = (max(meta_display - kpis.realizado, 0.0) / business_days_remaining) if business_days_remaining else 0.0

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown('<div class="crm-kpi-row">', unsafe_allow_html=True)
        st.metric("Realizado", fmt_brl_abbrev(kpis.realizado))
        st.markdown("</div>", unsafe_allow_html=True)
    with k2:
        st.markdown('<div class="crm-kpi-row">', unsafe_allow_html=True)
        st.metric("Meta", fmt_brl_abbrev(meta_display) if sales_scope == "Vendas efetivas" else "-")
        st.markdown("</div>", unsafe_allow_html=True)
    with k3:
        st.markdown('<div class="crm-kpi-row">', unsafe_allow_html=True)
        st.metric("Atingimento %", fmt_pct(ating_display) if sales_scope == "Vendas efetivas" else "-")
        st.markdown("</div>", unsafe_allow_html=True)
    with k4:
        st.markdown('<div class="crm-kpi-row">', unsafe_allow_html=True)
        st.metric("Run-rate", fmt_brl_abbrev(run_rate_display) if sales_scope == "Vendas efetivas" else "-")
        st.markdown("</div>", unsafe_allow_html=True)

    series = meta_realizado_mensal(cockpit_sheets, year)
    if not series.empty:
        series = series.copy()
        if "data" in series.columns:
            series["data"] = pd.to_datetime(series["data"], errors="coerce")
            series = series[series["data"].notna()]
            if selected_quarter is not None:
                q_start = (selected_quarter - 1) * 3 + 1
                q_end = q_start + 2
                series = series[series["data"].dt.month.between(q_start, q_end)]
            elif effective_ytd and year == pd.Timestamp.today().year:
                series = series[series["data"].dt.month <= pd.Timestamp.today().month]
            elif selected_month is not None and not effective_ytd:
                series = series[series["data"].dt.month == selected_month]
    if not series.empty:
        st.subheader("Meta vs Realizado")
        if selected_month is None or effective_ytd or selected_quarter is not None:
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
            cockpit_sheets.get("realizado", pd.DataFrame()),
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
            cockpit_sheets.get("realizado", pd.DataFrame()),
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
    perf = vendedor_performance_period(cockpit_sheets, year, selected_month, effective_ytd, selected_quarter)
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

if page == "Consignacao":
    render_consignacao_page(logo_path)

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
        st.caption(f"RECORTE ATUAL: {period} | EMPRESA={sel_company} | MOVIMENTO={sales_scope} | VENDEDOR={sel_vendor}")

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

# Page A3 - Comparativo de Vendas
if page == "Comparativo de Vendas":
    st.subheader("Comparativo de Vendas")
    st.caption("Analitico comparativo de vendas entre anos, com cortes por mes, produto e cliente.")

    # Comparativo: por enquanto consideramos somente 2025 vs 2026.
    allowed_compare_years = [2025, 2026]
    detail_all = load_bling_nfe_detail_years(tuple(allowed_compare_years))
    if detail_all.empty or "data" not in detail_all.columns:
        st.info("Sem base detalhada de vendas para montar o comparativo.")
    else:
        detail_all = detail_all.copy()
        detail_all["data"] = pd.to_datetime(detail_all["data"], errors="coerce")
        detail_all = detail_all[detail_all["data"].notna()].copy()
        available_years_set = set(detail_all["data"].dt.year.dropna().astype(int).unique().tolist())
        missing_allowed = [y for y in allowed_compare_years if y not in available_years_set]
        if missing_allowed:
            st.warning("Sem dados do Bling para: " + ", ".join(str(y) for y in missing_allowed))
        default_years = [y for y in allowed_compare_years if y in available_years_set] or allowed_compare_years
        compare_years = st.multiselect(
            "Anos para comparar",
            options=allowed_compare_years,
            default=default_years,
            key="compare_sales_years",
        )
        customer_scope = detail_all.copy()
        if sel_company != "TODOS":
            customer_scope = filter_company_scope(customer_scope, sel_company)
        if sel_vendor != "TODOS":
            customer_scope = filter_vendor_scope(
                customer_scope,
                sel_vendor,
                ["vendedor", "vendedor_id"],
                selected_vendor_candidates,
            )
        customer_scope = filter_sales_nature_scope(customer_scope, sales_scope)
        if compare_years:
            customer_scope = customer_scope[customer_scope["data"].dt.year.isin(compare_years)].copy()
        customer_options = ["TODOS"]
        if "cliente" in customer_scope.columns and not customer_scope.empty:
            customer_values = (
                customer_scope["cliente"].fillna("").astype(str).str.strip().str.upper().replace("", pd.NA).dropna().drop_duplicates()
            )
            customer_options.extend(sorted(customer_values.tolist()))
        selected_customer = st.selectbox(
            "Cliente",
            options=customer_options,
            index=0,
            key="compare_sales_customer",
        )
        view_mode = st.radio(
            "Visao analitica",
            ["Mensal", "Produto", "Cliente"],
            horizontal=True,
            key="compare_sales_mode",
        )
        top_n = st.slider("Top produtos/clientes", min_value=5, max_value=30, value=10, step=1, key="compare_sales_top_n")

        if len(compare_years) < 2:
            st.info("Selecione pelo menos 2 anos para comparar.")
        else:
            actual_compare, comp_period_label = prepare_sales_comparison_actual(
                compare_years,
                year,
                sel_company,
                sel_vendor,
                selected_customer,
                selected_vendor_candidates,
                sales_scope,
                selected_month,
                effective_ytd,
                selected_quarter,
            )
            comp_detail, _ = prepare_sales_comparison_detail(
                compare_years,
                year,
                sel_company,
                sel_vendor,
                selected_customer,
                selected_vendor_candidates,
                sales_scope,
                selected_month,
                effective_ytd,
                selected_quarter,
            )

            if actual_compare.empty:
                st.info("Sem realizado para os filtros atuais nessa combinacao de anos.")
            else:
                compare_years_sorted = sorted(compare_years)
                recorte_text = (
                    f"Empresa={sel_company} | Vendedor={sel_vendor} | "
                    f"Cliente={selected_customer} | Movimento={sales_scope} | Periodo={comp_period_label or '-'} | "
                    f"Anos={', '.join(str(item) for item in compare_years_sorted)}"
                )
                st.caption(recorte_text)

                yearly_totals = (
                    actual_compare.groupby("ano", dropna=False)["receita"]
                    .sum()
                    .reset_index()
                    .sort_values("ano")
                )
                yearly_totals["valor_anterior"] = yearly_totals["receita"].shift(1)
                yearly_totals["delta"] = yearly_totals["receita"] - yearly_totals["valor_anterior"]
                yearly_totals["delta_pct"] = (
                    yearly_totals["delta"] / yearly_totals["valor_anterior"].replace(0, pd.NA) * 100
                ).fillna(0)

                current_compare_year = max(compare_years)
                current_total = float(
                    yearly_totals.loc[yearly_totals["ano"] == current_compare_year, "receita"].sum()
                )
                previous_compare = yearly_totals[yearly_totals["ano"] < current_compare_year].tail(1)
                previous_total = float(previous_compare["receita"].iloc[0]) if not previous_compare.empty else 0.0
                current_delta = current_total - previous_total
                current_delta_pct = (current_delta / previous_total * 100) if previous_total else 0.0

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Periodo analisado", comp_period_label or "-")
                c2.metric(f"Vendas {current_compare_year}", fmt_brl_abbrev(current_total))
                c3.metric(
                    f"Vs {int(previous_compare['ano'].iloc[0])}" if not previous_compare.empty else "Vs ano anterior",
                    fmt_brl_abbrev(current_delta),
                )
                c4.metric("Variacao %", fmt_pct(current_delta_pct))

                overview = yearly_totals.copy()
                overview["ano"] = overview["ano"].astype(str)
                overview_chart = (
                    alt.Chart(overview)
                    .mark_bar(cornerRadiusTopLeft=8, cornerRadiusTopRight=8)
                    .encode(
                        x=alt.X("ano:N", title="Ano"),
                        y=alt.Y("receita:Q", title="Vendas reais"),
                        color=alt.Color(
                            "ano:N",
                            title="Ano",
                            scale=alt.Scale(range=["#1f264d", "#00b6b9", "#5fbd65", "#e07a1f", "#8c5fbf"]),
                        ),
                        tooltip=[
                            "ano:N",
                            alt.Tooltip("receita:Q", format=",.2f", title="Vendas reais"),
                            alt.Tooltip("delta:Q", format=",.2f", title="Delta"),
                            alt.Tooltip("delta_pct:Q", format=",.2f", title="Variacao %"),
                        ],
                    )
                    .properties(height=250)
                )
                st.altair_chart(overview_chart, width="stretch")

                if view_mode == "Mensal":
                    monthly = (
                        actual_compare.groupby(["ano_label", "mes", "mes_label"], dropna=False)["receita"]
                        .sum()
                        .reset_index()
                        .sort_values(["ano_label", "mes"])
                    )
                    month_sort = monthly.sort_values("mes")["mes_label"].drop_duplicates().tolist()
                    monthly_chart = (
                        alt.Chart(monthly)
                        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                        .encode(
                            x=alt.X(
                                "mes_label:N",
                                title="Mes",
                                sort=month_sort,
                                axis=alt.Axis(labelAngle=0),
                            ),
                            y=alt.Y("receita:Q", title="Vendas reais"),
                            color=alt.Color(
                                "ano_label:N",
                                title="Ano",
                                scale=alt.Scale(range=["#1f264d", "#00b6b9", "#5fbd65", "#e07a1f", "#8c5fbf"]),
                            ),
                            xOffset="ano_label:N",
                            tooltip=[
                                "ano_label:N",
                                "mes_label:N",
                                alt.Tooltip("receita:Q", format=",.2f", title="Vendas reais"),
                            ],
                        )
                        .properties(height=360)
                    )
                    st.markdown("#### Comparativo mensal entre anos")
                    st.altair_chart(monthly_chart, width="stretch")

                    monthly_pivot = (
                        monthly.pivot_table(index="mes_label", columns="ano_label", values="receita", aggfunc="sum", fill_value=0)
                        .reset_index()
                    )
                    monthly_pivot = monthly_pivot.rename(columns={"mes_label": "Mes"})
                    monthly_pivot["__ord"] = pd.Categorical(monthly_pivot["Mes"], categories=month_sort, ordered=True)
                    monthly_pivot = monthly_pivot.sort_values("__ord").drop(columns=["__ord"])
                    value_cols = [column for column in monthly_pivot.columns if column != "Mes"]
                    if len(value_cols) >= 2:
                        latest_col = value_cols[-1]
                        previous_col = value_cols[-2]
                        monthly_pivot["Delta"] = monthly_pivot[latest_col] - monthly_pivot[previous_col]
                        monthly_pivot["Variacao %"] = (
                            monthly_pivot["Delta"] / monthly_pivot[previous_col].replace(0, pd.NA) * 100
                        ).fillna(0)
                    for column in monthly_pivot.columns:
                        if column not in {"Mes", "Variacao %"}:
                            monthly_pivot[column] = monthly_pivot[column].map(fmt_brl_full)
                    if "Variacao %" in monthly_pivot.columns:
                        monthly_pivot["Variacao %"] = monthly_pivot["Variacao %"].map(fmt_pct)
                    st.dataframe(monthly_pivot, hide_index=True, width="stretch")

                    export_monthly = monthly.copy().rename(
                        columns={"ano_label": "Ano", "mes_label": "Mes", "receita": "Vendas"}
                    )
                    out_xlsx = BytesIO()
                    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
                        export_monthly[["Ano", "Mes", "Vendas"]].to_excel(
                            writer, index=False, sheet_name="comparativo_mensal"
                        )
                    st.download_button(
                        "Exportar comparativo mensal",
                        data=out_xlsx.getvalue(),
                        file_name="comparativo_vendas_mensal.xlsx",
                    )
                else:
                    if comp_detail.empty:
                        st.info("Sem base detalhada suficiente para comparar produto ou cliente nesse recorte.")
                    else:
                        dimension_col = "produto" if view_mode == "Produto" else "cliente"
                        compare_base = comp_detail.copy()
                        value_col = "valor_total"
                        if view_mode == "Cliente":
                            client_group_cols = [dimension_col, "ano_label", "nfe_id"]
                            if "empresa" in compare_base.columns:
                                client_group_cols.append("empresa")
                            compare_base = (
                                compare_base.groupby(client_group_cols, dropna=False)["valor_total"]
                                .max()
                                .reset_index()
                            )
                        ranking_base = (
                            compare_base.groupby(dimension_col, dropna=False)[value_col]
                            .sum()
                            .reset_index()
                            .sort_values(value_col, ascending=False)
                            .head(top_n)
                        )
                        selected_items = ranking_base[dimension_col].dropna().astype(str).tolist()
                        detail_view = compare_base[compare_base[dimension_col].isin(selected_items)].copy()
                        grouped = (
                            detail_view.groupby([dimension_col, "ano_label"], dropna=False)[value_col]
                            .sum()
                            .reset_index()
                        )
                        grouped = grouped.rename(columns={dimension_col: "dimensao"})

                        order_dims = (
                            grouped.groupby("dimensao", dropna=False)[value_col]
                            .sum()
                            .sort_values(ascending=False)
                            .index.tolist()
                        )
                        compare_chart = (
                            alt.Chart(grouped)
                            .mark_bar(cornerRadiusEnd=6)
                            .encode(
                                y=alt.Y("dimensao:N", sort=order_dims, title=view_mode),
                                x=alt.X(f"{value_col}:Q", title="Vendas"),
                                color=alt.Color(
                                    "ano_label:N",
                                    title="Ano",
                                    scale=alt.Scale(range=["#1f264d", "#00b6b9", "#5fbd65", "#e07a1f", "#8c5fbf"]),
                                ),
                                xOffset="ano_label:N",
                                tooltip=[
                                    "dimensao:N",
                                    "ano_label:N",
                                    alt.Tooltip(f"{value_col}:Q", format=",.2f", title="Vendas"),
                                ],
                            )
                            .properties(height=max(320, len(selected_items) * 30))
                        )
                        st.markdown(f"#### Comparativo por {view_mode.lower()}")
                        st.altair_chart(compare_chart, width="stretch")

                        pivot = (
                            grouped.pivot_table(index="dimensao", columns="ano_label", values=value_col, aggfunc="sum", fill_value=0)
                            .reset_index()
                        )
                        value_cols = [column for column in pivot.columns if column != "dimensao"]
                        if value_cols:
                            pivot["delta"] = pivot[value_cols[-1]] - pivot[value_cols[0]]
                            base_col = value_cols[0]
                            pivot["delta_pct"] = (
                                pivot["delta"] / pivot[base_col].replace(0, pd.NA) * 100
                            ).fillna(0)
                        for column in value_cols + ["delta"]:
                            if column in pivot.columns:
                                pivot[column] = pivot[column].map(fmt_brl_full)
                        if "delta_pct" in pivot.columns:
                            pivot["delta_pct"] = pivot["delta_pct"].map(fmt_pct)
                        pivot = pivot.rename(columns={"dimensao": view_mode})
                        show_cols = [view_mode] + value_cols + [c for c in ["delta", "delta_pct"] if c in pivot.columns]
                        st.dataframe(pivot[show_cols], hide_index=True, width="stretch")

                        if len(compare_years_sorted) >= 2:
                            latest_year = str(compare_years_sorted[-1])
                            previous_year = str(compare_years_sorted[-2])
                            comparison_wide = (
                                grouped.pivot_table(
                                    index="dimensao",
                                    columns="ano_label",
                                    values=value_col,
                                    aggfunc="sum",
                                    fill_value=0,
                                )
                                .reset_index()
                            )
                            if latest_year in comparison_wide.columns and previous_year in comparison_wide.columns:
                                comparison_wide["delta"] = comparison_wide[latest_year] - comparison_wide[previous_year]
                                comparison_wide["delta_pct"] = (
                                    comparison_wide["delta"] / comparison_wide[previous_year].replace(0, pd.NA) * 100
                                ).fillna(0)

                                top_limit = 5 if view_mode == "Cliente" else 10
                                altas = comparison_wide.sort_values("delta", ascending=False).head(top_limit).copy()
                                quedas = comparison_wide.sort_values("delta", ascending=True).head(top_limit).copy()
                                for frame in [altas, quedas]:
                                    for col in [previous_year, latest_year]:
                                        if col in frame.columns:
                                            frame[col] = frame[col].map(fmt_brl_full)
                                    frame["delta_pct"] = frame["delta_pct"].map(fmt_pct)
                                    frame.drop(columns=["delta"], inplace=True, errors="ignore")

                                st.markdown(f"#### Top Altas {view_mode.lower()}")
                                st.dataframe(
                                    altas.rename(columns={"dimensao": view_mode}),
                                    hide_index=True,
                                    width="stretch",
                                )
                                st.markdown(f"#### Top Quedas {view_mode.lower()}")
                                st.dataframe(
                                    quedas.rename(columns={"dimensao": view_mode}),
                                    hide_index=True,
                                    width="stretch",
                                )

                                export_rank = comparison_wide.rename(columns={"dimensao": view_mode}).copy()
                                out_xlsx = BytesIO()
                                with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
                                    export_rank.to_excel(writer, index=False, sheet_name="comparativo_rank")
                                st.download_button(
                                    f"Exportar comparativo por {view_mode.lower()}",
                                    data=out_xlsx.getvalue(),
                                    file_name=f"comparativo_vendas_{view_mode.lower().replace(' ', '_')}.xlsx",
                                )

# Page B - Pipeline Manager
if page == "Pipeline Manager":
    st.subheader("Pipeline Manager")
    pipeline_view = upper_dashboard_text(apply_acl_codes(load_sales_pipeline_view(), vendor_col="sales_rep_code"))
    pipeline_view = filter_vendor_scope(
        pipeline_view, sel_vendor, ["sales_rep_code", "sales_rep_name"], selected_vendor_candidates
    )
    pipeline_view = filter_company_scope(pipeline_view, sel_company)
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
        pipeline_needed = max(float(compute_kpis(
            sheets,
            year,
            selected_month,
            effective_ytd,
            selected_quarter,
            pipeline_view=df,
        ).gap), 0.0)
        coverage_pct = (weighted_total / pipeline_needed * 100) if pipeline_needed else 0.0

        c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
        c1.metric("Oportunidades", total_opps)
        c2.metric("Pipeline", fmt_brl_abbrev(pipeline_total))
        c3.metric("Pipeline ponderado", fmt_brl_abbrev(weighted_total))
        c4.metric("Sem proximo passo", sem_passo)
        c5.metric("Passos vencidos", overdue)
        c6.metric("Pipeline necessario", fmt_brl_abbrev(pipeline_needed))
        c7.metric("Cobertura do gap", fmt_pct(coverage_pct) if pipeline_needed else "-")

        if "stage" in df.columns and not df.empty:
            stage_df = (
                df.groupby("stage", dropna=False)[["pipeline_value", "weighted_pipeline_value"]]
                .sum()
                .reset_index()
                .fillna(0)
            )
            charts_col, aging_col = st.columns(2)
            with charts_col:
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

            if "last_opportunity_update" in df.columns:
                age_df = df.copy()
                age_df["last_opportunity_update"] = pd.to_datetime(
                    age_df["last_opportunity_update"], errors="coerce", utc=True
                ).dt.tz_localize(None)
                age_df["dias_sem_update"] = (
                    pd.Timestamp.today().normalize() - age_df["last_opportunity_update"].dt.normalize()
                ).dt.days
                age_df["dias_sem_update"] = pd.to_numeric(age_df["dias_sem_update"], errors="coerce").fillna(0)
                aging_stage = (
                    age_df.groupby("stage", dropna=False)
                    .agg(
                        dias_medios_sem_update=("dias_sem_update", "mean"),
                        dias_max_sem_update=("dias_sem_update", "max"),
                        oportunidades=("opportunities_count", "sum"),
                    )
                    .reset_index()
                    .sort_values("dias_medios_sem_update", ascending=False)
                )
                with aging_col:
                    aging_chart = (
                        alt.Chart(aging_stage)
                        .mark_bar(cornerRadiusEnd=6, color="#e07a1f")
                        .encode(
                            y=alt.Y("stage:N", sort="-x", title="Etapa"),
                            x=alt.X("dias_medios_sem_update:Q", title="Dias medios sem update"),
                            tooltip=[
                                "stage",
                                alt.Tooltip("dias_medios_sem_update:Q", format=",.1f", title="Dias medios"),
                                alt.Tooltip("dias_max_sem_update:Q", format=",.0f", title="Dias max"),
                                alt.Tooltip("oportunidades:Q", format=",.0f", title="Oportunidades"),
                            ],
                        )
                    )
                    st.altair_chart(aging_chart, width="stretch")

                risk_stage = aging_stage.copy()
                risk_stage["status"] = "OK"
                risk_stage.loc[risk_stage["dias_medios_sem_update"] >= 14, "status"] = "ATENCAO"
                risk_stage.loc[risk_stage["dias_medios_sem_update"] >= 30, "status"] = "RISCO"
                risk_stage["dias_medios_sem_update"] = risk_stage["dias_medios_sem_update"].round(1)
                risk_stage["dias_max_sem_update"] = risk_stage["dias_max_sem_update"].round(0).astype(int)
                st.markdown("#### Etapas em risco")
                st.dataframe(
                    risk_stage[["stage", "oportunidades", "dias_medios_sem_update", "dias_max_sem_update", "status"]],
                    hide_index=True,
                    width="stretch",
                )

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

        export_view = view.copy()
        for column in export_view.columns:
            if pd.api.types.is_datetime64tz_dtype(export_view[column]):
                export_view[column] = export_view[column].dt.tz_localize(None)

        out = BytesIO()
        export_view.to_excel(out, index=False, sheet_name="pipeline")
        st.download_button("Exportar Pipeline", data=out.getvalue(), file_name="pipeline_manager.xlsx")

        queue_df = upper_dashboard_text(apply_acl_codes(load_crm_priority_queue(), vendor_col="sales_rep_code"))
        queue_df = filter_vendor_scope(
            queue_df, sel_vendor, ["sales_rep_code", "sales_rep_name"], selected_vendor_candidates
        )
        queue_df = filter_company_scope(queue_df, sel_company)
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
    perf_pipeline_view = filter_company_scope(perf_pipeline_view, sel_company)
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
    queue_df = filter_company_scope(queue_df, sel_company)
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
    st.subheader("Auditoria: Dashboard vs Bling (NF-e)")
    st.write("Comparativo mensal entre o realizado exibido no dashboard e o faturamento fiscal do Bling.")

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
    real = filter_period_scope(real, year, selected_month, effective_ytd, selected_quarter)
    real_m = real.groupby(real["data"].dt.to_period("M"))["receita"].sum().reset_index()
    real_m["data"] = real_m["data"].dt.to_timestamp()

    # Bling NF-e fiscal
    nfe_m = build_nfe_monthly_audit_base(
        year,
        sel_company,
        sel_vendor,
        selected_vendor_candidates,
        selected_month,
        effective_ytd,
        selected_quarter,
    )
    if nfe_m.empty:
        if not PUBLIC_REVIEW:
            st.warning("Cache NFe do Bling nao encontrado. Exibindo valores do Bling como zero para revisao.")
        nfe_m = real_m[["data"]].copy() if not real_m.empty else pd.DataFrame(columns=["data"])
        nfe_m["valor"] = 0.0

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
    tabs = st.tabs(["Executive Summary", "Metas", "Cadastro", "Importação", "Transferencia"])
    targets_view_all = apply_acl_codes(metas_source_all, vendor_col="sales_rep_code")
    targets_view_all = filter_vendor_scope(
        targets_view_all, sel_vendor, ["sales_rep_code", "sales_rep_name"], selected_vendor_candidates
    )
    periodo_tipo = "QUARTER" if selected_quarter is not None else "MONTH"
    uf = ""
    status = []
    mes = selected_month if selected_month is not None and not effective_ytd and selected_quarter is None else None
    quarter = selected_quarter
    metas_realizado_base = realizado_source_all.copy()

    with tabs[0]:
        st.write("Resumo executivo das metas no recorte atual do sidebar.")
        st.caption(f"Periodo={period} | Vendedor={sel_vendor} | Empresa={sel_company}")
        colf1, colf2, colf3 = st.columns([2, 2, 1])

        if not targets_view_all.empty:
            all_targets_year = filter_targets_view(
                targets_view_all,
                year,
                periodo_tipo,
                month_num=mes,
                quarter_num=quarter,
                ytd=effective_ytd,
            )
            uf_opts = [""] + sorted([v for v in all_targets_year.get("state", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if v])
        else:
            filtros_metas_base = {"ano": year, "periodo_tipo": periodo_tipo, "mes": mes, "quarter": quarter}
            all_metas = list_metas(filtros_metas_base)
            all_metas = filter_local_targets_scope(all_metas, "TODOS", sel_vendor, selected_vendor_candidates)
            if PROFILE == "gestor" and not all_metas.empty:
                acl = load_acl().get("gestor", {})
                allow = _clean_list(acl.get("allow_vendedores", []))
                block = _clean_list(acl.get("block_vendedores", []))
                if allow:
                    all_metas = all_metas[all_metas["vendedor_id"].isin(allow)]
                elif block:
                    all_metas = all_metas[~all_metas["vendedor_id"].isin(block)]
            uf_opts = [""] + sorted(all_metas["estado"].dropna().unique().tolist()) if not all_metas.empty else [""]

        uf = colf1.selectbox("UF (opcional)", options=uf_opts, key="metas_uf")
        status = colf2.multiselect("Status", ["ATIVO", "PAUSADO", "DESLIGADO", "TRANSFERIDO"], key="metas_status")
        if colf3.button("Criar dados demo", key="metas_seed"):
            seed_demo()
            load_sales_targets_view.clear()
            st.success("Dados demo criados.")

        if not targets_view_all.empty:
            filtered_view = filter_targets_view(
                targets_view_all,
                year,
                periodo_tipo,
                month_num=mes,
                quarter_num=quarter,
                ytd=effective_ytd,
                state=None,
                statuses=status or None,
            )
            filtered_view = overlay_targets_actuals_from_realizado(
                filtered_view,
                metas_realizado_base,
                year_col="target_year",
                period_type_col="period_type",
                month_col="month_num",
                quarter_col="quarter_num",
                state_col=None,
                vendor_col="sales_rep_name",
                company_col=None,
                actual_col="actual_value",
                gap_col="gap_value",
            )
            realized_summary = build_targets_realizado_summary(
                metas_realizado_base,
                target_year=year,
                period_type=periodo_tipo,
                month_num=mes,
                quarter_num=quarter,
                ytd=effective_ytd,
                state=uf or None,
                selected_company="TODOS",
                selected_vendor=sel_vendor,
                selected_vendor_candidates=selected_vendor_candidates,
            )
            res = build_targets_summary(filtered_view, periodo_tipo, realized_summary=realized_summary)
            k = res["kpis"]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Meta", fmt_brl_abbrev(k.get("meta", 0)))
            c2.metric("Realizado", fmt_brl_abbrev(k.get("realizado", 0)))
            c3.metric("Atingimento %", fmt_pct(k.get("atingimento_pct", 0)))
            c4.metric("Delta", fmt_brl_abbrev(k.get("delta", 0)))

            if not res["series"].empty:
                ser = res["series"].rename(columns={"meta_valor": "meta", "realizado_valor": "receita"}).copy()
                if "periodo" not in ser.columns:
                    if periodo_tipo == "QUARTER" and "quarter" in ser.columns:
                        ser["periodo"] = ser["quarter"]
                    elif "mes" in ser.columns:
                        ser["periodo"] = ser["mes"]
                if not realized_summary["series"].empty:
                    ser = ser.drop(columns=["receita"], errors="ignore").merge(
                        realized_summary["series"].rename(columns={"realizado_valor": "receita"}),
                        on="periodo",
                        how="left",
                    )
                    ser["receita"] = pd.to_numeric(ser["receita"], errors="coerce").fillna(0)
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
                if not realized_summary["uf"].empty:
                    uf_df = uf_df.drop(columns=["realizado_valor"], errors="ignore").merge(
                        realized_summary["uf"],
                        on="estado",
                        how="left",
                    )
                    uf_df["realizado_valor"] = pd.to_numeric(uf_df["realizado_valor"], errors="coerce").fillna(0)
                uf_df["ating"] = (uf_df["realizado_valor"] / uf_df["meta_valor"] * 100).fillna(0)
                st.write("Atingimento por UF")
                st.bar_chart(uf_df.set_index("estado")[["meta_valor", "realizado_valor"]])
                uf_df["delta"] = uf_df["realizado_valor"] - uf_df["meta_valor"]
                st.write("Delta por UF")
                st.bar_chart(uf_df.set_index("estado")[["delta"]])

            if not realized_summary["heatmap"].empty:
                heat_src = realized_summary["heatmap"].copy()
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
                "mes": mes,
                "quarter": quarter,
                "estado": uf or None,
                "status": status or None,
            }
            if sel_company != "TODOS":
                filtros["empresa"] = sel_company
            if PROFILE == "gestor":
                acl = load_acl().get("gestor", {})
                allow = _clean_list(acl.get("allow_vendedores", []))
                block = _clean_list(acl.get("block_vendedores", []))
                if allow:
                    filtros["vendedor_id"] = allow
                elif block and not all_metas.empty:
                    filtros["vendedor_id"] = [v for v in all_metas["vendedor_id"].dropna().tolist() if v not in block]
            dfm = list_metas(filtros)
            dfm = filter_local_targets_scope(dfm, "TODOS", sel_vendor, selected_vendor_candidates)
            dfm = overlay_targets_actuals_from_realizado(
                dfm,
                metas_realizado_base,
                year_col="ano",
                period_type_col="periodo_tipo",
                month_col="mes",
                quarter_col="quarter",
                state_col=None,
                vendor_col="vendedor_id",
                company_col=None,
                actual_col="realizado_valor",
                gap_col="gap_valor",
            )
            realized_summary = build_targets_realizado_summary(
                metas_realizado_base,
                target_year=year,
                period_type=periodo_tipo,
                month_num=mes,
                quarter_num=quarter,
                ytd=effective_ytd,
                state=uf or None,
                selected_company="TODOS",
                selected_vendor=sel_vendor,
                selected_vendor_candidates=selected_vendor_candidates,
            )
            res = build_local_targets_summary(dfm, periodo_tipo, realized_summary=realized_summary)
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

            if PROFILE == "gestor" and not dfm.empty:
                acl = load_acl().get("gestor", {})
                allow = _clean_list(acl.get("allow_vendedores", []))
                block = _clean_list(acl.get("block_vendedores", []))
                if allow:
                    dfm = dfm[dfm["vendedor_id"].isin(allow)]
                elif block:
                    dfm = dfm[~dfm["vendedor_id"].isin(block)]
            if not realized_summary["heatmap"].empty:
                heat = realized_summary["heatmap"].copy()
                st.write("Heatmap UF x periodo")
                hm = alt.Chart(heat).mark_rect().encode(
                    x=alt.X("periodo:O", title="Periodo"),
                    y=alt.Y("estado:N", title="UF"),
                    color=alt.Color("realizado:Q", title="Realizado"),
                    tooltip=["estado", "periodo", "realizado"],
                )
                st.altair_chart(hm, width="stretch")

    with tabs[1]:
        st.write("Listagem de metas no recorte atual do sidebar.")
        if not targets_view_all.empty:
            df = filter_targets_view(
                targets_view_all,
                year,
                periodo_tipo,
                month_num=mes,
                quarter_num=quarter,
                ytd=effective_ytd,
                statuses=status or None,
            )
            if not df.empty:
                df = collapse_targets_rows(df, include_state=False)
                df = overlay_targets_actuals_from_realizado(
                    df,
                    metas_realizado_base,
                    year_col="ano",
                    period_type_col="periodo_tipo",
                    month_col="mes",
                    quarter_col="quarter",
                    state_col=None,
                    vendor_col="vendedor_label",
                    company_col="empresa",
                    actual_col="actual_value",
                    gap_col="gap_value",
                )
                df = df.rename(
                    columns={
                        "target_value": "meta_valor",
                        "actual_value": "realizado_valor",
                        "attainment_pct": "atingimento_pct",
                        "gap_value": "gap_valor",
                    }
                )
                df = format_targets_listing(df)
                view_cols = [
                    c
                    for c in [
                        "ano",
                        "mes",
                        "empresa",
                        "vendedor",
                        "meta_valor",
                        "realizado_valor",
                        "atingimento_pct",
                        "gap_valor",
                        "status",
                    ]
                    if c in df.columns
                ]
                st.dataframe(
                    df[view_cols],
                    height=420,
                    width="stretch",
                )
            else:
                st.info("Sem metas para o recorte selecionado.")
        else:
            filtros_lista = {"ano": year, "periodo_tipo": periodo_tipo, "mes": mes, "quarter": quarter, "status": status or None}
            if sel_company != "TODOS":
                filtros_lista["empresa"] = sel_company
            df = list_metas(filtros_lista)
            df = filter_local_targets_scope(df, sel_company, sel_vendor, selected_vendor_candidates)
            df = collapse_targets_rows(df, include_state=False)
            df = overlay_targets_actuals_from_realizado(
                df,
                metas_realizado_base,
                year_col="ano",
                period_type_col="periodo_tipo",
                month_col="mes",
                quarter_col="quarter",
                state_col=None,
                vendor_col="vendedor_label",
                company_col="empresa",
                actual_col="realizado_valor",
                gap_col="gap_valor",
            )
            if PROFILE == "gestor" and not df.empty:
                acl = load_acl().get("gestor", {})
                allow = _clean_list(acl.get("allow_vendedores", []))
                block = _clean_list(acl.get("block_vendedores", []))
                if allow:
                    df = df[df["vendedor_id"].isin(allow)]
                elif block:
                    df = df[~df["vendedor_id"].isin(block)]
            if not df.empty:
                df = format_targets_listing(df)
                view_cols = [
                    c
                    for c in [
                        "ano",
                        "mes",
                        "empresa",
                        "vendedor",
                        "meta_valor",
                        "realizado_valor",
                        "atingimento_pct",
                        "gap_valor",
                        "status",
                    ]
                    if c in df.columns
                ]
                st.dataframe(
                    df[view_cols],
                    height=420,
                    width="stretch",
                )
            else:
                st.info("Sem metas para o recorte selecionado.")

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
            empresa_default = sel_company if sel_company in {"CZ", "CR"} else "CZ"
            mf["empresa"] = st.selectbox("Empresa", ["CZ", "CR"], index=0 if empresa_default == "CZ" else 1, key="meta_empresa")
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
                        "empresa": mf.get("empresa"),
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
        st.write("Importar metas por planilha")
        spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
        sheet_name = os.getenv("GOOGLE_SHEETS_TARGETS_SHEET", "metas").strip()
        sheet_range = os.getenv("GOOGLE_SHEETS_TARGETS_RANGE", "A:O").strip()
        if spreadsheet_id:
            st.success(f"Fonte compartilhada: {sheet_name}!{sheet_range}")
        else:
            st.warning("GOOGLE_SHEETS_SPREADSHEET_ID nao configurado. O sync compartilhado nao vai funcionar ainda.")

        default_company_sync = st.selectbox(
            "Empresa padrao do sync",
            ["AUTO", "CZ", "CR"],
            index=0 if sel_company == "TODOS" else (1 if sel_company == "CZ" else 2),
            key="sync_metas_empresa",
        )
        default_empresa_sync = sel_company if sel_company in {"CZ", "CR"} else "CZ"
        if default_company_sync != "AUTO":
            default_empresa_sync = default_company_sync

        col_sync_1, col_sync_2 = st.columns(2)
        with col_sync_1:
            if st.button("Validar planilha compartilhada", key="validar_sheet_compartilhada"):
                ok, output = run_shared_targets_sync_script(default_empresa_sync, dry_run=True)
                if ok:
                    st.success("Validacao concluida.")
                else:
                    st.error("Validacao com erro.")
                if output:
                    st.code(output, language="text")
        with col_sync_2:
            if st.button("Sincronizar agora", key="sync_sheet_compartilhada"):
                ok, output = run_shared_targets_sync_script(default_empresa_sync, dry_run=False)
                if ok:
                    load_sales_targets_view.clear()
                    try:
                        load_sheets.clear()
                    except Exception:
                        pass
                    st.success("Sincronizacao concluida.")
                    if output:
                        st.code(output, language="text")
                    st.rerun()
                else:
                    st.error("Sincronizacao com erro.")
                    if output:
                        st.code(output, language="text")

        st.caption("Fallback manual: subir arquivo local e usar os botoes para validar ou importar pelo comando.")
        template_df = build_sales_targets_template()
        template_bytes = template_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Baixar template CSV",
            data=template_bytes,
            file_name="template_metas_comerciais.csv",
            mime="text/csv",
        )
        uploaded_targets = st.file_uploader(
            "Carregar planilha de metas",
            type=["csv", "xlsx", "xls", "xlsm"],
            key="upload_metas_comerciais",
        )
        default_company = st.selectbox(
            "Empresa padrao",
            ["AUTO", "CZ", "CR"],
            index=0 if sel_company == "TODOS" else (1 if sel_company == "CZ" else 2),
            key="upload_metas_empresa",
        )
        staging_file = ROOT / "data" / "staging" / "metas_comerciais_import.xlsx"
        if uploaded_targets is not None:
            try:
                uploaded_df = read_uploaded_targets_file(uploaded_targets)
            except Exception as exc:
                st.error(f"Falha ao ler a planilha: {exc}")
            else:
                default_empresa = sel_company if sel_company in {"CZ", "CR"} else "CZ"
                if default_company != "AUTO":
                    default_empresa = default_company
                preview_valid, preview_invalid, preview_warnings = prepare_sales_targets_import(
                    uploaded_df,
                    default_empresa=default_empresa,
                )
                st.write("Prévia da importação")
                if preview_warnings:
                    for warning in preview_warnings:
                        st.info(warning)
                st.caption(f"{len(preview_valid)} linhas válidas | {len(preview_invalid)} linhas inválidas")
                if not preview_valid.empty:
                    st.dataframe(
                        format_targets_listing(preview_valid.head(200)),
                        height=260,
                        width="stretch",
                    )
                if not preview_invalid.empty:
                    st.warning("Algumas linhas foram rejeitadas. Revise antes de importar.")
                    st.dataframe(preview_invalid.head(200), height=220, width="stretch")
                colv1, colv2 = st.columns(2)
                with colv1:
                    if st.button("Validar planilha (comando)", key="validar_metas_planilha"):
                        try:
                            saved_path = save_uploaded_targets_file(uploaded_targets, staging_file)
                            ok, output = run_sales_targets_import_script(saved_path, default_empresa, dry_run=True)
                        except Exception as exc:
                            st.error(f"Falha ao validar metas: {exc}")
                        else:
                            if ok:
                                st.success("Validacao concluida.")
                            else:
                                st.error("Validacao com erro.")
                            if output:
                                st.code(output, language="text")
                with colv2:
                    if st.button("Subir planilha (comando)", key="importar_metas_planilha"):
                        try:
                            saved_path = save_uploaded_targets_file(uploaded_targets, staging_file)
                            ok, output = run_sales_targets_import_script(saved_path, default_empresa, dry_run=False)
                        except Exception as exc:
                            st.error(f"Falha ao importar metas: {exc}")
                        else:
                            if ok:
                                load_sales_targets_view.clear()
                                try:
                                    load_sheets.clear()
                                except Exception:
                                    pass
                                st.success("Importacao concluida.")
                                st.code(output or "OK", language="text")
                                st.rerun()
                            else:
                                st.error("Importacao com erro.")
                                if output:
                                    st.code(output, language="text")

    with tabs[4]:
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
