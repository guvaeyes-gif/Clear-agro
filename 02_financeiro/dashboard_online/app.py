from __future__ import annotations

import base64
import json
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from PIL import Image, ImageDraw, ImageFont


BASE_DIR = Path(__file__).resolve().parent
WORKSPACE_ROOT = BASE_DIR.parents[1]
SNAPSHOT_PATH = BASE_DIR / "data" / "latest_snapshot.json"
DUPLICATAS_GARANTIA_PATH = WORKSPACE_ROOT / "02_financeiro" / "Duplicatas en Garantía.xlsx"
LOGO_CANDIDATES = [
    WORKSPACE_ROOT / "02_financeiro" / "CLEAR logo.png",
    WORKSPACE_ROOT / "data" / "CLEAR.png",
]
MONTH_NAMES = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Marco",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro",
}


st.set_page_config(
    page_title="Clear Agro Financeiro",
    page_icon="C",
    layout="wide",
    initial_sidebar_state="expanded",
)


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --brand-ink: #193326;
            --brand-green: #2f6548;
            --brand-sage: #eef5ef;
            --brand-gold: #b88b2f;
            --brand-line: #d5ddd3;
            --brand-card: rgba(255,255,255,0.8);
        }
        .stApp {
            background:
                radial-gradient(circle at top right, rgba(184,139,47,0.16), transparent 24%),
                linear-gradient(180deg, #f8f4ea 0%, #fbfcf8 20%, #f1f7f0 100%);
        }
        .block-container {
            max-width: 1420px;
            padding-top: 2.4rem;
            padding-bottom: 2rem;
        }
        html, body, [class*="css"] {
            font-family: "Trebuchet MS", "Segoe UI", sans-serif;
        }
        h1, h2, h3 {
            color: var(--brand-ink);
        }
        h1 {
            font-size: 3rem !important;
            line-height: 1 !important;
            letter-spacing: 0.02em;
            margin-bottom: 0.35rem !important;
        }
        h2 {
            font-size: 1.6rem !important;
            margin-top: 1rem !important;
            padding-bottom: 0.2rem;
            border-bottom: 1px solid var(--brand-line);
        }
        h3 {
            font-size: 1.04rem !important;
            text-transform: uppercase;
            letter-spacing: 0.06em;
        }
        .hero-card {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 28px;
            margin: 3.4rem 0 1.8rem 0;
            padding: 2.2rem 2.4rem;
            background: linear-gradient(135deg, rgba(255,255,255,0.9) 0%, rgba(241,247,240,0.96) 100%);
            border: 1px solid rgba(47,101,72,0.18);
            border-radius: 28px;
            box-shadow: 0 22px 44px rgba(25,51,38,0.10);
            position: relative;
            overflow: hidden;
        }
        .hero-card::before {
            content: "";
            position: absolute;
            inset: 0 auto 0 0;
            width: 8px;
            background: linear-gradient(180deg, var(--brand-gold) 0%, var(--brand-green) 100%);
        }
        .hero-title-wrap {
            padding: 0 0 0 1rem;
        }
        .hero-title {
            font-size: 3.4rem;
            line-height: 1;
            font-weight: 700;
            margin: 0;
            color: var(--brand-ink);
            letter-spacing: 0.08em;
        }
        .hero-header {
            display: flex;
            align-items: center;
            justify-content: flex-start;
            gap: 28px;
        }
        .hero-title-box {
            flex: 1 1 auto;
            min-width: 0;
        }
        .hero-logo-box {
            flex: 0 0 32%;
            display: flex;
            justify-content: flex-end;
            align-items: center;
            padding-right: 0.4rem;
        }
        .section-note {
            background: rgba(255,255,255,0.76);
            border: 1px solid var(--brand-line);
            border-left: 6px solid var(--brand-gold);
            border-radius: 14px;
            padding: 0.85rem 1rem;
            margin-bottom: 1rem;
            color: #46584c;
        }
        div[data-testid="stMetric"] {
            background: var(--brand-card);
            border: 1px solid var(--brand-line);
            border-radius: 18px;
            padding: 0.9rem 1rem 1rem 1rem;
            box-shadow: 0 10px 22px rgba(25,51,38,0.05);
        }
        div[data-testid="stMetricLabel"] {
            color: #617166;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-size: 0.72rem;
        }
        div[data-testid="stMetricValue"] {
            color: var(--brand-ink);
            font-size: 1rem;
            line-height: 1.2;
            word-break: break-word;
            white-space: normal;
            overflow-wrap: anywhere;
        }
        .finance-metric-card {
            background: var(--brand-card);
            border: 1px solid var(--brand-line);
            border-radius: 18px;
            padding: 0.9rem 1rem 1rem 1rem;
            box-shadow: 0 10px 22px rgba(25,51,38,0.05);
            text-align: center;
            min-height: 112px;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }
        .finance-metric-card.finance-metric-card-highlight {
            min-height: 132px;
        }
        .finance-metric-label {
            color: #617166;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-size: 0.72rem;
            margin-bottom: 0.45rem;
        }
        .finance-metric-card.finance-metric-card-highlight .finance-metric-label {
            font-size: 0.9rem;
            margin-bottom: 0.6rem;
        }
        .finance-metric-value {
            color: var(--brand-ink);
            font-size: 1.15rem;
            line-height: 1.25;
            font-weight: 700;
            word-break: break-word;
            overflow-wrap: anywhere;
        }
        .finance-metric-card.finance-metric-card-highlight .finance-metric-value {
            font-size: 1.85rem;
            line-height: 1.15;
        }
        .sidebar-logo {
            display: flex;
            justify-content: center;
            margin: -0.2rem 0 1rem 0;
        }
        .sidebar-logo-badge {
            display: inline-flex;
            justify-content: center;
            align-items: center;
            width: 100%;
            padding: 0.85rem 0.7rem;
            background: linear-gradient(135deg, rgba(255,255,255,0.94) 0%, rgba(241,247,240,0.98) 100%);
            border: 1px solid rgba(47,101,72,0.14);
            border-radius: 18px;
            box-shadow: 0 10px 18px rgba(25,51,38,0.06), 0 22px 42px rgba(25,51,38,0.10);
        }
        .sidebar-box {
            background: rgba(255,255,255,0.78);
            border: 1px solid var(--brand-line);
            border-radius: 18px;
            padding: 0.95rem 1rem;
            margin-top: 0.8rem;
        }
        .dre-column {
            background: rgba(255,255,255,0.82);
            border: 1px solid var(--brand-line);
            border-radius: 18px;
            padding: 1rem 1rem 0.9rem 1rem;
            min-height: 560px;
            box-shadow: 0 10px 22px rgba(25,51,38,0.05);
        }
        .dre-column h4 {
            margin: 0 0 0.75rem 0;
            color: var(--brand-ink);
            font-size: 0.98rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
        }
        .dre-total {
            margin: 0.8rem 0 0.55rem 0;
            padding-top: 0.7rem;
            border-top: 1px solid var(--brand-line);
            font-weight: 700;
            color: var(--brand-ink);
        }
        .dre-line {
            display: flex;
            justify-content: space-between;
            gap: 10px;
            padding: 0.22rem 0;
            font-size: 0.92rem;
            color: #31463a;
        }
        .dre-line-label {
            flex: 1 1 auto;
        }
        .dre-line-value {
            flex: 0 0 auto;
            text-align: right;
            white-space: nowrap;
            font-variant-numeric: tabular-nums;
        }
        .dre-note {
            margin-top: 0.55rem;
            font-size: 0.8rem;
            color: #617166;
        }
        .dre-matrix-wrap {
            border: 1px solid var(--brand-line);
            border-radius: 18px;
            background: rgba(255,255,255,0.86);
            box-shadow: 0 10px 22px rgba(25,51,38,0.05);
            overflow: auto;
            max-height: 680px;
        }
        .dre-matrix {
            width: max-content;
            min-width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            font-size: 0.82rem;
            color: #31463a;
            text-transform: uppercase;
        }
        .dre-matrix th,
        .dre-matrix td {
            padding: 0.5rem 0.62rem;
            border-bottom: 1px solid rgba(213,221,211,0.9);
            white-space: nowrap;
        }
        .dre-matrix thead th {
            position: sticky;
            top: 0;
            z-index: 4;
            background: #f4f8f1;
            color: var(--brand-ink);
            letter-spacing: 0.04em;
        }
        .dre-matrix .dre-matrix-account {
            position: sticky;
            left: 0;
            z-index: 3;
            min-width: 260px;
            max-width: 260px;
            white-space: pre-wrap;
            background: #fcfdf9;
        }
        .dre-matrix thead .dre-matrix-account {
            z-index: 5;
            background: #edf4ea;
        }
        .dre-matrix .dre-matrix-value {
            min-width: 116px;
            text-align: right;
            font-variant-numeric: tabular-nums;
        }
        .dre-matrix .dre-matrix-percent {
            min-width: 54px;
            text-align: right;
            font-size: 0.78em;
            color: #7b867f;
        }
        .dre-matrix tbody tr:hover td {
            background: rgba(238,245,239,0.78);
        }
        .dre-matrix tbody tr:hover .dre-matrix-account {
            background: rgba(232,241,232,0.96);
        }
        .dre-matrix .dre-matrix-strong td {
            color: #193326;
            font-weight: 700;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner=False)
def load_snapshot(snapshot_mtime_ns: int) -> dict[str, Any]:
    with SNAPSHOT_PATH.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def find_logo() -> Path | None:
    for path in LOGO_CANDIDATES:
        if path.exists():
            return path
    return None


def render_logo(path: Path, width: int = 220, sidebar: bool = False) -> None:
    suffix = path.suffix.lower()
    if suffix not in {".png", ".jpg", ".jpeg", ".webp"}:
        return
    mime = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
    }[suffix]
    image_b64 = base64.b64encode(path.read_bytes()).decode("ascii")
    style = "width:220px; max-width:94%; height:auto;" if sidebar else f"width:{width}px; max-width:100%; height:auto;"
    wrapper = "sidebar-logo" if sidebar else ""
    content = f'<img src="data:{mime};base64,{image_b64}" alt="Clear Agro" style="{style}">'
    if sidebar:
        content = f'<div class="sidebar-logo-badge">{content}</div>'
    st.markdown(
        f'<div class="{wrapper}">{content}</div>',
        unsafe_allow_html=True,
    )


def brl(value: Any) -> str:
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        return "R$ 0,00"
    formatted = f"{number:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {formatted}"


def bold_headers(frame: pd.DataFrame) -> pd.io.formats.style.Styler:
    return frame.style.set_table_styles(
        [
            {
                "selector": "th",
                "props": [("font-weight", "700")],
            }
        ]
    )


def pct(value: Any) -> str:
    try:
        number = float(value or 0) * 100
    except (TypeError, ValueError):
        return "0,0%"
    return f"{number:.1f}%".replace(".", ",")


def integer(value: Any) -> str:
    try:
        return f"{int(float(value or 0)):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "0"


def as_frame(rows: list[dict[str, Any]] | None) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def load_jsonl_frame(paths: list[Path]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if "empresa" not in obj:
                    obj["empresa"] = "CR" if path.name.endswith("_cr.jsonl") else "CZ"
                rows.append(obj)
    if not rows:
        return pd.DataFrame()
    return pd.json_normalize(rows)


@st.cache_data(show_spinner=False)
def load_effective_sales_frame() -> pd.DataFrame:
    caches = [
        WORKSPACE_ROOT / "bling_api" / "nfe_2026_cache.jsonl",
        WORKSPACE_ROOT / "bling_api" / "nfe_2026_cache_cr.jsonl",
        WORKSPACE_ROOT / "bling_api" / "nfe_2025_cache.jsonl",
        WORKSPACE_ROOT / "bling_api" / "nfe_2025_cache_cr.jsonl",
    ]
    df = load_jsonl_frame(caches)
    if df.empty:
        return df
    if "dataEmissao" in df.columns:
        df["data"] = pd.to_datetime(df["dataEmissao"], errors="coerce")
    elif "dataOperacao" in df.columns:
        df["data"] = pd.to_datetime(df["dataOperacao"], errors="coerce")
    else:
        df["data"] = pd.NaT
    if "valorNota" in df.columns:
        df["receita"] = pd.to_numeric(df["valorNota"], errors="coerce").fillna(0.0)
    else:
        df["receita"] = pd.to_numeric(df.get("valor"), errors="coerce").fillna(0.0)
    if "naturezaOperacao.id" in df.columns:
        df["natureza"] = df["naturezaOperacao.id"].fillna("").astype(str)
    else:
        df["natureza"] = ""
    if "vendedor.id" in df.columns:
        df["vendedor_id"] = df["vendedor.id"].fillna("").astype(str).str.strip()
    elif "vendedor_id" in df.columns:
        df["vendedor_id"] = df["vendedor_id"].fillna("").astype(str).str.strip()
    else:
        df["vendedor_id"] = ""
    item_cfops: list[str] = []
    for items in df.get("itens", pd.Series([[]] * len(df))):
        cfop = ""
        if isinstance(items, list) and items:
            first = items[0]
            if isinstance(first, dict):
                cfop = str(first.get("cfop") or "").strip()
        item_cfops.append(cfop)
    df["cfop"] = item_cfops
    natureza_txt = (df["natureza"] + " " + df["cfop"]).astype(str).str.upper()
    receita = pd.to_numeric(df["receita"], errors="coerce").fillna(0.0)
    vendedor_id = df["vendedor_id"].fillna("").astype(str).str.strip()
    cfop = df["cfop"].fillna("").astype(str).str.strip()
    is_return_cfop = cfop.str.match(r"^[1267]20[1-9]$|^[12]201$|^[56]202$")
    is_non_sale_cfop = cfop.isin({"5910", "6910", "5911", "6911", "5917", "6917", "5949", "6949"})
    is_devolucao = natureza_txt.str.contains("DEVOL|RETORNO|ESTORNO|CANCEL", regex=True) | receita.lt(0) | is_return_cfop
    is_non_sale = natureza_txt.str.contains("REMESSA|CONSIGN|BONIFIC", regex=True) | is_non_sale_cfop
    is_vendor_zero = vendedor_id.eq("0")
    df = df[~is_devolucao & ~is_non_sale & ~is_vendor_zero & receita.gt(0)].copy()
    df = df.dropna(subset=["data"])
    return df


def effective_sales_total(year: int | None, month: int | None) -> float:
    df = load_effective_sales_frame()
    if df.empty:
        return 0.0
    out = df.copy()
    if year is not None:
        out = out[out["data"].dt.year == int(year)]
    if month is not None:
        out = out[out["data"].dt.month == int(month)]
    return float(pd.to_numeric(out["receita"], errors="coerce").fillna(0.0).sum())


def metric_row(metrics: list[tuple[str, str]]) -> None:
    cols = st.columns(len(metrics))
    for col, (label, value) in zip(cols, metrics, strict=False):
        col.metric(label, value)


def metric_grid(metrics: list[tuple[str, str]], columns: int = 2) -> None:
    if not metrics:
        return
    for start in range(0, len(metrics), columns):
        row = metrics[start:start + columns]
        metric_row(row)


def metric_section(title: str, metrics: list[tuple[str, str]], columns: int = 2) -> None:
    st.markdown(f"#### {title}")
    metric_grid(metrics, columns=columns)


def metric_rows(rows: list[list[tuple[str, str] | None]], center_single_item_rows: bool = False) -> None:
    if not rows:
        return
    width = max(len(row) for row in rows)
    for row in rows:
        if center_single_item_rows and len(row) == 1:
            _, metric_col, _ = st.columns([1, 2, 1])
            label, value = row[0]
            metric_col.metric(label, value)
            continue
        cols = st.columns(width)
        for idx in range(width):
            item = row[idx] if idx < len(row) else None
            if item is None:
                cols[idx].markdown("&nbsp;")
            else:
                label, value = item
                cols[idx].metric(label, value)


def render_centered_metric_rows(rows: list[list[tuple[str, str] | None]], highlight_labels: set[str] | None = None) -> None:
    if not rows:
        return

    highlight_labels = {str(label).strip().upper() for label in (highlight_labels or set())}

    def _metric_html(label: str, value: str) -> str:
        safe_label = (
            str(label or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        safe_value = (
            str(value or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        card_class = "finance-metric-card"
        if str(label).strip().upper() in highlight_labels:
            card_class += " finance-metric-card-highlight"
        return (
            f'<div class="{card_class}">'
            f'<div class="finance-metric-label">{safe_label}</div>'
            f'<div class="finance-metric-value">{safe_value}</div>'
            "</div>"
        )

    width = max(len(row) for row in rows)
    for row in rows:
        if len(row) == 1 and row[0] is not None:
            _, metric_col, _ = st.columns([1, 2, 1])
            label, value = row[0]
            metric_col.markdown(_metric_html(label, value), unsafe_allow_html=True)
            continue
        cols = st.columns(width)
        for idx in range(width):
            item = row[idx] if idx < len(row) else None
            if item is None:
                cols[idx].markdown("&nbsp;")
            else:
                label, value = item
                cols[idx].markdown(_metric_html(label, value), unsafe_allow_html=True)


def render_dre_column(title: str, rows: list[tuple[str, str]], total_label: str, total_value: str, note: str = "") -> None:
    def esc(text: Any) -> str:
        return (
            str(text or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    lines = "".join(
        f'<div class="dre-line"><span class="dre-line-label">{esc(label)}</span><span class="dre-line-value">{esc(value)}</span></div>'
        for label, value in rows
    )
    note_html = f'<div class="dre-note">{esc(note)}</div>' if note else ""
    st.markdown(
        f"""
        <div class="dre-column">
            <h4>{esc(title)}</h4>
            {lines}
            <div class="dre-total">{esc(total_label)}: {esc(total_value)}</div>
            {note_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_dre_matrix(frame: pd.DataFrame) -> None:
    if frame.empty:
        return

    def esc(text: Any) -> str:
        return (
            str(text or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    percent_cols = [col for col in frame.columns if col.endswith("%")]

    def row_class(label: Any) -> str:
        text = upper_text(label)
        if "RESULTADO" in text:
            return "dre-matrix-strong"
        if "MARGEM" in text:
            return "dre-matrix-strong"
        if "RECEITA TOTAL" in text or "CUSTOS FIXOS TOTAIS" in text:
            return "dre-matrix-strong"
        if "CMV" in text:
            return "dre-matrix-strong"
        return ""

    header_html = "".join(
        (
            f'<th class="dre-matrix-account">{esc(col)}</th>'
            if col == "CONTA"
            else f'<th class="{"dre-matrix-percent" if col in percent_cols else "dre-matrix-value"}">{esc("%" if col in percent_cols else col)}</th>'
        )
        for col in frame.columns
    )

    body_rows: list[str] = []
    for _, row in frame.iterrows():
        row_html = []
        for col in frame.columns:
            value = esc(row.get(col, ""))
            if col == "CONTA":
                row_html.append(f'<td class="dre-matrix-account">{value}</td>')
            else:
                cell_class = "dre-matrix-percent" if col in percent_cols else "dre-matrix-value"
                row_html.append(f'<td class="{cell_class}">{value}</td>')
        body_rows.append(f'<tr class="{row_class(row.get("CONTA"))}">{"".join(row_html)}</tr>')

    st.markdown(
        f"""
        <div class="dre-matrix-wrap">
            <table class="dre-matrix">
                <thead>
                    <tr>{header_html}</tr>
                </thead>
                <tbody>
                    {"".join(body_rows)}
                </tbody>
            </table>
        </div>
        """,
        unsafe_allow_html=True,
    )


def upper_text(value: Any) -> str:
    return str(value or "").strip().upper()


def _pdf_font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    candidates = [
        "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/calibrib.ttf" if bold else "C:/Windows/Fonts/calibri.ttf",
        "C:/Windows/Fonts/segoeuib.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf",
    ]
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def _text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    return int(draw.textbbox((0, 0), text, font=font)[2])


def _fit_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> str:
    value = str(text or "")
    if _text_width(draw, value, font) <= max_width:
        return value
    shortened = value
    while len(shortened) > 1 and _text_width(draw, f"{shortened}...", font) > max_width:
        shortened = shortened[:-1]
    return f"{shortened}..."


def _load_pdf_logo(logo_path: Path | None, max_width: int, max_height: int) -> Image.Image | None:
    if logo_path is None or not logo_path.exists():
        return None
    try:
        logo = Image.open(logo_path).convert("RGBA")
    except Exception:
        return None
    logo.thumbnail((max_width, max_height))
    return logo


def _dre_row_is_strong(label: Any) -> bool:
    text = upper_text(label)
    if "RESULTADO" in text or "MARGEM" in text or "CMV" in text:
        return True
    return "RECEITA TOTAL" in text or "DESPESAS OPERACIONAIS" in text or "CAPITAL ATUAL INVESTIDO" in text


def build_dre_analytic_pdf(
    matrix_df: pd.DataFrame,
    analysis_year: int,
    period_label_value: str,
    source_label: str,
    logo_path: Path | None,
) -> bytes:
    if matrix_df.empty:
        return b""

    page_width = 1654
    page_height = 1169
    margin_x = 56
    margin_y = 46
    header_h = 170
    table_top = margin_y + header_h + 24
    table_bottom = page_height - 52
    row_h = 28
    header_row_h = 34
    usable_table_h = table_bottom - table_top - header_row_h
    rows_per_page = max(8, usable_table_h // row_h)

    account_col = "CONTA"
    trailing_cols = [col for col in ["TOTAL", "TOTAL %"] if col in matrix_df.columns]
    detail_cols = [col for col in matrix_df.columns if col not in {account_col, *trailing_cols}]
    chunk_size = 6
    detail_chunks = [detail_cols[i:i + chunk_size] for i in range(0, len(detail_cols), chunk_size)] or [[]]

    title_font = _pdf_font(34, bold=True)
    meta_font = _pdf_font(16, bold=False)
    small_font = _pdf_font(13, bold=False)
    small_bold = _pdf_font(13, bold=True)
    body_font = _pdf_font(12, bold=False)
    body_bold = _pdf_font(12, bold=True)

    pages: list[Image.Image] = []
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")
    total_page_count = sum(max(1, (len(matrix_df) + rows_per_page - 1) // rows_per_page) for _ in detail_chunks)
    current_page = 1

    for chunk_index, chunk in enumerate(detail_chunks, start=1):
        selected_cols = [account_col, *chunk, *trailing_cols]
        chunk_df = matrix_df[selected_cols].copy()

        if len(chunk) == 0:
            chunk_label = "TOTAL"
        else:
            visible_months = [str(col).replace(" %", "") for col in chunk if not str(col).endswith("%")]
            chunk_label = f"{visible_months[0]} A {visible_months[-1]}"

        for start in range(0, len(chunk_df), rows_per_page):
            page = Image.new("RGB", (page_width, page_height), "white")
            draw = ImageDraw.Draw(page)

            draw.rounded_rectangle(
                (margin_x, margin_y, page_width - margin_x, margin_y + header_h),
                radius=28,
                fill=(245, 249, 245),
                outline=(214, 224, 214),
                width=2,
            )
            draw.rounded_rectangle(
                (margin_x, margin_y, margin_x + 10, margin_y + header_h),
                radius=12,
                fill=(184, 139, 47),
            )
            draw.rectangle((margin_x + 5, margin_y, margin_x + 10, margin_y + header_h), fill=(47, 101, 72))

            draw.text((margin_x + 34, margin_y + 26), "DRE ANALITICO", fill=(25, 51, 38), font=title_font)
            draw.text(
                (margin_x + 36, margin_y + 80),
                f"Ano {analysis_year} | Periodo {period_label_value} | Fonte {source_label}",
                fill=(70, 88, 76),
                font=meta_font,
            )
            draw.text(
                (margin_x + 36, margin_y + 112),
                f"Faixa exportada: {chunk_label} | Gerado em {timestamp}",
                fill=(97, 113, 102),
                font=meta_font,
            )

            logo = _load_pdf_logo(logo_path, max_width=340, max_height=118)
            if logo is not None:
                logo_x = page_width - margin_x - logo.width - 20
                logo_y = margin_y + (header_h - logo.height) // 2
                page.paste(logo, (logo_x, logo_y), logo)

            current_top = table_top
            page_rows = chunk_df.iloc[start:start + rows_per_page]

            account_width = 430
            trailing_widths = []
            for col in trailing_cols:
                trailing_widths.append(150 if str(col) == "TOTAL" else 94)
            remaining_width = page_width - (2 * margin_x) - account_width - sum(trailing_widths)
            dynamic_count = max(1, len(chunk))
            dynamic_width = max(74, remaining_width // dynamic_count)
            col_widths: list[int] = [account_width]
            col_widths.extend([dynamic_width for _ in chunk])
            col_widths.extend(trailing_widths)

            header_x = margin_x
            for col, width in zip(selected_cols, col_widths, strict=False):
                draw.rectangle(
                    (header_x, current_top, header_x + width, current_top + header_row_h),
                    fill=(232, 239, 232),
                    outline=(205, 214, 204),
                    width=1,
                )
                label = "%" if str(col).endswith("%") else str(col)
                label = _fit_text(draw, label, small_bold, width - 14)
                draw.text((header_x + 8, current_top + 9), label, fill=(25, 51, 38), font=small_bold)
                header_x += width

            current_top += header_row_h

            for _, row in page_rows.iterrows():
                row_label = str(row.get(account_col, ""))
                strong_row = _dre_row_is_strong(row_label)
                row_fill = (248, 251, 248) if strong_row else (255, 255, 255)
                text_fill = (25, 51, 38)
                row_x = margin_x
                for col, width in zip(selected_cols, col_widths, strict=False):
                    draw.rectangle(
                        (row_x, current_top, row_x + width, current_top + row_h),
                        fill=row_fill,
                        outline=(223, 229, 222),
                        width=1,
                    )
                    raw_value = str(row.get(col, "") or "")
                    font = body_bold if strong_row else body_font
                    if col == account_col:
                        value = _fit_text(draw, raw_value, font, width - 12)
                        draw.text((row_x + 8, current_top + 7), value, fill=text_fill, font=font)
                    else:
                        value = _fit_text(draw, raw_value, font, width - 10)
                        value_fill = (198, 40, 40) if raw_value.startswith("R$ -") else text_fill
                        value_width = _text_width(draw, value, font)
                        draw.text((row_x + width - value_width - 8, current_top + 7), value, fill=value_fill, font=font)
                    row_x += width
                current_top += row_h

            footer = f"Pagina {current_page} de {total_page_count}"
            footer_width = _text_width(draw, footer, small_font)
            draw.text((page_width - margin_x - footer_width, page_height - 34), footer, fill=(97, 113, 102), font=small_font)
            pages.append(page)
            current_page += 1

    pdf_buffer = BytesIO()
    pages[0].save(pdf_buffer, format="PDF", save_all=True, append_images=pages[1:] if len(pages) > 1 else [])
    return pdf_buffer.getvalue()


def monthly_frame(snapshot: dict[str, Any], key: str = "monthly") -> pd.DataFrame:
    frame = as_frame(snapshot.get(key))
    if frame.empty:
        return frame
    frame["mes"] = frame["mes"].astype(str)
    frame["ano"] = frame["mes"].str[:4].astype(int)
    frame["mes_num"] = frame["mes_num"].astype(int)
    frame["mes_nome"] = frame["mes_num"].map(MONTH_NAMES)
    frame["periodo"] = frame["mes_nome"] + "/" + frame["ano"].astype(str)
    for col in [
        "receita_liquida",
        "custos_variaveis_total",
        "cmv_proxy",
        "cmv_sales_cost",
        "cmv_purchase_fallback",
        "custo_fixo_base",
        "despesas_ap_proxy",
        "margem_contribuicao",
        "ebitda",
    ]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)
    return frame.sort_values(["ano", "mes_num"]).reset_index(drop=True)


def cash_days_frame(snapshot: dict[str, Any]) -> pd.DataFrame:
    frame = as_frame(snapshot.get("cash_projection", {}).get("days"))
    if frame.empty:
        return frame
    frame["data"] = pd.to_datetime(frame["data"], errors="coerce")
    frame = frame.dropna(subset=["data"]).copy()
    frame["ano"] = frame["data"].dt.year.astype(int)
    frame["mes_num"] = frame["data"].dt.month.astype(int)
    frame["data_label"] = frame["data"].dt.strftime("%d/%m/%Y")
    for col in ["inflow", "outflow", "net", "cumulative_net"]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)
    return frame


def bank_balance_frame(snapshot: dict[str, Any]) -> pd.DataFrame:
    frame = as_frame((snapshot.get("bank_balances") or {}).get("balances"))
    if frame.empty:
        return frame
    if "balance" in frame.columns:
        frame["balance"] = pd.to_numeric(frame["balance"], errors="coerce").fillna(0.0)
    if "balance_status" not in frame.columns:
        frame["balance_status"] = "API"
    if "as_of" in frame.columns:
        frame["as_of_dt"] = pd.to_datetime(frame["as_of"], errors="coerce")
        frame["as_of_label"] = frame["as_of_dt"].dt.strftime("%d/%m/%Y")
    return frame


def future_flow_monthly(frame: pd.DataFrame, value_col: str = "valor") -> pd.DataFrame:
    if frame.empty or "data_vencimento" not in frame.columns:
        return pd.DataFrame(columns=["ano", "mes_num", "periodo", "valor"])
    work = frame.copy()
    work["data_vencimento"] = pd.to_datetime(work["data_vencimento"], errors="coerce")
    work = work.dropna(subset=["data_vencimento"]).copy()
    today = pd.Timestamp(datetime.now().date())
    work = work[work["data_vencimento"] >= today].copy()
    if work.empty:
        return pd.DataFrame(columns=["ano", "mes_num", "periodo", "valor"])
    work["ano"] = work["data_vencimento"].dt.year.astype(int)
    work["mes_num"] = work["data_vencimento"].dt.month.astype(int)
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce").fillna(0.0)
    out = work.groupby(["ano", "mes_num"], as_index=False)[value_col].sum()
    out["periodo"] = out["mes_num"].map(MONTH_NAMES).fillna(out["mes_num"].astype(str)) + "/" + out["ano"].astype(str)
    out = out.rename(columns={value_col: "valor"})
    return out.sort_values(["ano", "mes_num"]).reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_duplicatas_garantia() -> pd.DataFrame:
    if not DUPLICATAS_GARANTIA_PATH.exists():
        return pd.DataFrame(columns=["company", "banco", "pagador", "valor", "vencimento", "data_vencimento", "ano", "mes_num", "data_label"])
    try:
        frame = pd.read_excel(DUPLICATAS_GARANTIA_PATH, sheet_name="Base_CZ")
    except Exception:
        return pd.DataFrame(columns=["company", "banco", "pagador", "valor", "vencimento", "data_vencimento", "ano", "mes_num", "data_label"])
    if frame.empty:
        return pd.DataFrame(columns=["company", "banco", "pagador", "valor", "vencimento", "data_vencimento", "ano", "mes_num", "data_label"])
    out = pd.DataFrame()
    out["company"] = frame.get("Empresa", "CZ").fillna("CZ").astype(str).str.upper()
    out["banco"] = frame.get("Banco", "").fillna("").astype(str).str.upper()
    out["pagador"] = frame.get("Pagador", "").fillna("").astype(str).str.upper()
    out["valor"] = pd.to_numeric(frame.get("Valor (R$)"), errors="coerce").fillna(0.0)
    out["data_vencimento"] = pd.to_datetime(frame.get("Vencimento"), errors="coerce")
    out = out.dropna(subset=["data_vencimento"]).copy()
    today = pd.Timestamp(datetime.now().date())
    out.loc[out["data_vencimento"] < today, "data_vencimento"] = today
    out["vencimento"] = out["data_vencimento"].dt.strftime("%Y-%m-%d")
    out["ano"] = out["data_vencimento"].dt.year.astype(int)
    out["mes_num"] = out["data_vencimento"].dt.month.astype(int)
    out["data_label"] = out["data_vencimento"].dt.strftime("%d/%m/%Y")
    return out.sort_values("data_vencimento").reset_index(drop=True)


def top_flow_frame(rows: list[dict[str, Any]] | None) -> pd.DataFrame:
    frame = as_frame(rows)
    if frame.empty:
        return frame
    frame["data"] = pd.to_datetime(frame["data"], errors="coerce")
    frame = frame.dropna(subset=["data"]).copy()
    frame["ano"] = frame["data"].dt.year.astype(int)
    frame["mes_num"] = frame["data"].dt.month.astype(int)
    frame["data_label"] = frame["data"].dt.strftime("%d/%m/%Y")
    frame["valor"] = pd.to_numeric(frame["valor"], errors="coerce").fillna(0.0)
    return frame.sort_values("valor", ascending=False).reset_index(drop=True)


def account_detail_frame(snapshot: dict[str, Any], key: str, fallback_rows: list[dict[str, Any]] | None, tipo: str) -> pd.DataFrame:
    frame = as_frame(snapshot.get(key))
    if frame.empty:
        frame = top_flow_frame(fallback_rows).copy()
        if frame.empty:
            return frame
        frame["cliente_fornecedor"] = frame.get("contato", "N/D")
        frame["fornecedor"] = frame["cliente_fornecedor"] if tipo == "pagar" else ""
        frame["cliente"] = frame["cliente_fornecedor"] if tipo == "receber" else ""
        frame["data_emissao"] = frame["data"].dt.strftime("%Y-%m-%d")
        frame["vencimento"] = frame["data"].dt.strftime("%Y-%m-%d")
        frame["situacao"] = "A vencer"
        frame["company"] = ""
        frame["documento"] = ""
        frame["cultura"] = ""
        frame["zafra"] = ""
        frame["juros"] = 0.0
        frame["dias_atraso"] = 0
        frame["vencido"] = False
        frame["data_vencimento"] = pd.to_datetime(frame["vencimento"], errors="coerce")
    else:
        if "valor" in frame.columns:
            frame["valor"] = pd.to_numeric(frame["valor"], errors="coerce").fillna(0.0)
        if "juros" not in frame.columns:
            frame["juros"] = 0.0
        frame["juros"] = pd.to_numeric(frame["juros"], errors="coerce").fillna(0.0)
        if "dias_atraso" not in frame.columns:
            frame["dias_atraso"] = 0
        frame["dias_atraso"] = pd.to_numeric(frame["dias_atraso"], errors="coerce").fillna(0).astype(int)
        if "vencimento" in frame.columns:
            frame["data_vencimento"] = pd.to_datetime(frame["vencimento"], errors="coerce")
        else:
            frame["data_vencimento"] = pd.NaT
        if "data_emissao" in frame.columns:
            frame["data_emissao_dt"] = pd.to_datetime(frame["data_emissao"], errors="coerce")
        elif "data" in frame.columns:
            frame["data_emissao_dt"] = pd.to_datetime(frame["data"], errors="coerce")
        else:
            frame["data_emissao_dt"] = pd.NaT
    if "data_vencimento" in frame.columns:
        frame["ano"] = pd.to_datetime(frame["data_vencimento"], errors="coerce").dt.year
        frame["mes_num"] = pd.to_datetime(frame["data_vencimento"], errors="coerce").dt.month
    elif "data_emissao_dt" in frame.columns:
        frame["ano"] = pd.to_datetime(frame["data_emissao_dt"], errors="coerce").dt.year
        frame["mes_num"] = pd.to_datetime(frame["data_emissao_dt"], errors="coerce").dt.month
    if "data_emissao_dt" in frame.columns:
        frame["data_emissao_label"] = pd.to_datetime(frame["data_emissao_dt"], errors="coerce").dt.strftime("%d/%m/%Y")
    if "data_vencimento" in frame.columns:
        frame["data_label"] = pd.to_datetime(frame["data_vencimento"], errors="coerce").dt.strftime("%d/%m/%Y")
    elif "vencimento" in frame.columns:
        frame["data_label"] = pd.to_datetime(frame["vencimento"], errors="coerce").dt.strftime("%d/%m/%Y")
    for col in ["cliente_fornecedor", "fornecedor", "cliente", "cultura", "zafra", "situacao", "documento", "company"]:
        if col not in frame.columns:
            frame[col] = ""
        frame[col] = frame[col].fillna("").astype(str).str.strip()
    for col in ["cliente_fornecedor", "fornecedor", "cliente"]:
        if col in frame.columns:
            frame[col] = frame[col].str.upper()
    return frame


def period_options(monthly: pd.DataFrame) -> tuple[list[int], dict[str, int | None]]:
    years = sorted(monthly["ano"].unique().tolist()) if not monthly.empty else []
    month_map: dict[str, int | None] = {"Todos": None}
    if not monthly.empty:
        for mes_num in sorted(monthly["mes_num"].unique().tolist()):
            month_map[MONTH_NAMES.get(mes_num, str(mes_num))] = mes_num
    return years, month_map


def period_options_from_frames(frames: list[pd.DataFrame]) -> tuple[list[int], dict[str, int | None]]:
    years_set: set[int] = set()
    months_set: set[int] = set()
    for frame in frames:
        if frame.empty:
            continue
        if "ano" in frame.columns:
            years_set.update(
                {
                    int(value)
                    for value in pd.to_numeric(frame["ano"], errors="coerce").dropna().tolist()
                }
            )
        if "mes_num" in frame.columns:
            months_set.update(
                {
                    int(value)
                    for value in pd.to_numeric(frame["mes_num"], errors="coerce").dropna().tolist()
                }
            )
    years = sorted(years_set)
    month_map: dict[str, int | None] = {"Todos": None}
    for mes_num in sorted(months_set):
        month_map[MONTH_NAMES.get(mes_num, str(mes_num))] = mes_num
    return years, month_map


def filter_monthly(frame: pd.DataFrame, year: int | None, month: int | None) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    if year is not None:
        out = out[out["ano"] == int(year)]
    if month is not None:
        out = out[out["mes_num"] == int(month)]
    return out


def filter_dated(frame: pd.DataFrame, year: int | None, month: int | None) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    if year is not None and "ano" in out.columns:
        out = out[out["ano"] == int(year)]
    if month is not None and "mes_num" in out.columns:
        out = out[out["mes_num"] == int(month)]
    return out


def filter_company(frame: pd.DataFrame, company: str | None) -> pd.DataFrame:
    if frame.empty or not company or company == "Todas" or "company" not in frame.columns:
        return frame
    out = frame.copy()
    return out[out["company"].fillna("").astype(str).str.upper() == str(company).upper()]


def normalize_account_company(company: str | None) -> str:
    if not company:
        return "Todas"
    company_text = str(company).strip().upper()
    if company_text in {"CZ", "CR", "TODAS"}:
        return "Todas" if company_text == "TODAS" else company_text
    return "Todas"


def aging_bucket(days_overdue: Any) -> str:
    try:
        days = int(days_overdue or 0)
    except (TypeError, ValueError):
        days = 0
    if days <= 0:
        return "A vencer"
    if days <= 30:
        return "1-30"
    if days <= 60:
        return "31-60"
    if days <= 90:
        return "61-90"
    if days <= 180:
        return "91-180"
    return ">180"


def status_label(raw_status: Any, days_overdue: Any, value: Any) -> str:
    text = str(raw_status or "").strip().upper()
    if "CANCEL" in text:
        return "Cancelado"
    try:
        status_code = int(float(raw_status))
    except (TypeError, ValueError):
        status_code = None
    try:
        amount = float(value or 0)
    except (TypeError, ValueError):
        amount = 0.0
    try:
        days = int(days_overdue or 0)
    except (TypeError, ValueError):
        days = 0
    if status_code == 3:
        return "Parcial em atraso" if days > 0 else "Parcial em dia"
    if amount <= 0:
        return "Sem saldo"
    if days > 0:
        return "Vencido"
    return "A vencer"


def period_summary(monthly: pd.DataFrame) -> dict[str, float]:
    if monthly.empty:
        return {
            "receita_liquida": 0.0,
            "cmv_proxy": 0.0,
            "cmv_pct": 0.0,
            "custos_variaveis_total": 0.0,
            "custo_fixo_base": 0.0,
            "margem_contribuicao": 0.0,
            "ebitda": 0.0,
            "margem_ebitda_pct": 0.0,
            "meses": 0.0,
        }
    receita = float(monthly["receita_liquida"].sum())
    custos_variaveis = float(monthly["custos_variaveis_total"].sum())
    margem_contribuicao = float(monthly["margem_contribuicao"].sum())
    if "cmv_proxy" in monthly.columns:
        cmv_proxy = float(monthly["cmv_proxy"].sum())
    else:
        cmv_proxy = float(receita - margem_contribuicao - custos_variaveis)
    ebitda = float(monthly["ebitda"].sum())
    return {
        "receita_liquida": receita,
        "cmv_proxy": cmv_proxy,
        "cmv_pct": (cmv_proxy / receita) if receita else 0.0,
        "custos_variaveis_total": custos_variaveis,
        "custo_fixo_base": float(monthly["custo_fixo_base"].sum()),
        "margem_contribuicao": margem_contribuicao,
        "ebitda": ebitda,
        "margem_ebitda_pct": (ebitda / receita) if receita else 0.0,
        "meses": float(len(monthly)),
    }


def has_proxy_rows(monthly: pd.DataFrame) -> bool:
    return (
        not monthly.empty
        and "dre_model" in monthly.columns
        and monthly["dre_model"].astype(str).isin(["bling_proxy", "bling_erp"]).any()
    )


def period_label(year: int | None, month: int | None) -> str:
    if year is None:
        return "Base completa"
    if month is None:
        return f"Ano {year}"
    return f"{MONTH_NAMES.get(month, str(month))}/{year}"


def compute_bling_management_summary(ap_frame: pd.DataFrame, ar_frame: pd.DataFrame, cash_frame: pd.DataFrame) -> dict[str, float]:
    ap = ap_frame.copy() if not ap_frame.empty else pd.DataFrame()
    ar = ar_frame.copy() if not ar_frame.empty else pd.DataFrame()
    cash = cash_frame.copy() if not cash_frame.empty else pd.DataFrame()

    for frame in [ap, ar]:
        if not frame.empty:
            frame["valor"] = pd.to_numeric(frame["valor"], errors="coerce").fillna(0.0)
            frame["dias_atraso"] = pd.to_numeric(frame["dias_atraso"], errors="coerce").fillna(0).astype(int)

    if not cash.empty:
        for col in ["inflow", "outflow", "net", "cumulative_net"]:
            cash[col] = pd.to_numeric(cash[col], errors="coerce").fillna(0.0)

    ap_total = float(ap["valor"].sum()) if not ap.empty else 0.0
    ar_total = float(ar["valor"].sum()) if not ar.empty else 0.0
    ap_vencido = float(ap.loc[ap["dias_atraso"] > 0, "valor"].sum()) if not ap.empty else 0.0
    ar_vencido = float(ar.loc[ar["dias_atraso"] > 0, "valor"].sum()) if not ar.empty else 0.0
    ap_a_vencer = float(ap.loc[ap["dias_atraso"] <= 0, "valor"].sum()) if not ap.empty else 0.0
    ar_a_vencer = float(ar.loc[ar["dias_atraso"] <= 0, "valor"].sum()) if not ar.empty else 0.0

    ap_top = 0.0
    if not ap.empty and "fornecedor" in ap.columns:
        ap_top = float(ap.groupby("fornecedor", dropna=False)["valor"].sum().max())
    ar_top = 0.0
    if not ar.empty and "cliente" in ar.columns:
        ar_top = float(ar.groupby("cliente", dropna=False)["valor"].sum().max())

    return {
        "ap_total": ap_total,
        "ar_total": ar_total,
        "ap_vencido": ap_vencido,
        "ar_vencido": ar_vencido,
        "ap_a_vencer": ap_a_vencer,
        "ar_a_vencer": ar_a_vencer,
        "saldo_aberto": ar_total - ap_total,
        "saldo_vencido": ar_vencido - ap_vencido,
        "fluxo_30d": float(cash["net"].sum()) if not cash.empty else 0.0,
        "entrada_30d": float(cash["inflow"].sum()) if not cash.empty else 0.0,
        "saida_30d": float(cash["outflow"].sum()) if not cash.empty else 0.0,
        "menor_caixa_30d": float(cash["cumulative_net"].min()) if not cash.empty else 0.0,
        "titulos_ap": float(len(ap)),
        "titulos_ar": float(len(ar)),
        "ap_maior_fornecedor": ap_top,
        "ar_maior_cliente": ar_top,
        "inadimplencia_ar_pct": (ar_vencido / ar_total) if ar_total else 0.0,
        "pressao_ap_pct": (ap_vencido / ap_total) if ap_total else 0.0,
    }


def render_hero(snapshot: dict[str, Any], title: str, subtitle: str, logo_path: Path | None) -> None:
    logo_html = ""
    if logo_path is not None and logo_path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}:
        mime = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".webp": "image/webp",
        }[logo_path.suffix.lower()]
        image_b64 = base64.b64encode(logo_path.read_bytes()).decode("ascii")
        logo_html = (
            f'<div class="hero-logo-box">'
            f'<img src="data:{mime};base64,{image_b64}" alt="Clear Agro" '
            f'style="width:100%; max-width:760px; height:auto; display:block;">'
            f"</div>"
        )
    st.markdown(
        f"""
        <div class="hero-card">
            <div class="hero-header">
                <div class="hero-title-box">
                    <div class="hero-title-wrap" style="text-align:left;">
                        <div class="hero-title">{title}</div>
                    </div>
                </div>
                {logo_html}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_note(text: str) -> None:
    st.markdown(f'<div class="section-note">{text}</div>', unsafe_allow_html=True)


def render_overview(
    snapshot: dict[str, Any],
    monthly_period: pd.DataFrame,
    monthly_all: pd.DataFrame,
    ap_period: pd.DataFrame,
    ar_period: pd.DataFrame,
    cash_period: pd.DataFrame,
    year: int | None,
    month: int | None,
    label: str,
) -> None:
    quality = snapshot["quality_reconciliation"]
    governance = snapshot["governance"]
    classic = snapshot["classic_kpis"]
    
    # Compute summary from dataframes, with fallback to classic_kpis
    summary = compute_bling_management_summary(ap_period, ar_period, cash_period)
    
    # Fallback to classic_kpis when dataframes are empty (e.g., on Render)
    if summary["ar_total"] == 0 and summary["ap_total"] == 0:
        classic_kpis = snapshot.get("classic_kpis", {})
        summary["ar_total"] = classic_kpis.get("ar_aberto", 0)
        summary["ap_total"] = classic_kpis.get("ap_aberto", 0)
        summary["ar_vencido"] = classic_kpis.get("ar_vencido", 0)
        summary["ap_vencido"] = classic_kpis.get("ap_vencido", 0)
        summary["ar_a_vencer"] = summary["ar_total"] - summary["ar_vencido"]
        summary["ap_a_vencer"] = summary["ap_total"] - summary["ap_vencido"]
        summary["saldo_aberto"] = summary["ar_total"] - summary["ap_total"]
        summary["fluxo_30d"] = classic_kpis.get("fluxo_liquido_previsto_30d", 0)
        summary["inadimplencia_ar_pct"] = (summary["ar_vencido"] / summary["ar_total"]) if summary["ar_total"] else 0
        summary["pressao_ap_pct"] = (summary["ap_vencido"] / summary["ap_total"]) if summary["ap_total"] else 0
    
    dre_summary = period_summary(monthly_period)
    proxy_period = has_proxy_rows(monthly_period)

    st.header("Resumo Executivo")
    metric_row(
        [
            ("AR Total", brl(summary["ar_total"])),
            ("AP Total", brl(summary["ap_total"])),
            ("Saldo em Aberto", brl(summary["saldo_aberto"])),
            ("Qualidade", classic["quality_status"]),
        ]
    )
    metric_row(
        [
            ("AR A Vencer", brl(summary["ar_a_vencer"])),
            ("AP A Vencer", brl(summary["ap_a_vencer"])),
            ("Fluxo Liquido 30d", brl(summary["fluxo_30d"])),
            ("Checks com Falha", integer(quality["quality_check_fail"])),
        ]
    )

    if proxy_period:
        commercial_sales_total = effective_sales_total(year, month)
        cmv_sales_total = (
            float(pd.to_numeric(monthly_period.get("cmv_sales_cost"), errors="coerce").fillna(0.0).sum())
            if "cmv_sales_cost" in monthly_period.columns
            else 0.0
        )
        cmv_sales_pct = (cmv_sales_total / commercial_sales_total) if commercial_sales_total else 0.0
        st.subheader("Resultado Gerencial do Periodo")
        metric_rows(
            [
                [
                    ("Faturamento NF-e", brl(dre_summary["receita_liquida"])),
                    ("Vendas efetivas", brl(commercial_sales_total)),
                ],
                [
                    ("Custo total das vendas efetivas", brl(cmv_sales_total)),
                    ("CMV sobre vendas", pct(cmv_sales_pct)),
                ],
                [
                    ("Despesa AP Proxy", brl(dre_summary["custo_fixo_base"])),
                    ("EBITDA Proxy", brl(dre_summary["ebitda"])),
                ],
            ]
        )

    left, right = st.columns([1.5, 1.0])
    with left:
        st.subheader("Abertos por Origem")
        base = pd.DataFrame(
            [
                {"indicador": "AR total", "valor": summary["ar_total"]},
                {"indicador": "AR vencido", "valor": summary["ar_vencido"]},
                {"indicador": "AP total", "valor": summary["ap_total"]},
                {"indicador": "AP vencido", "valor": summary["ap_vencido"]},
            ]
        )
        st.bar_chart(base.set_index("indicador")["valor"], use_container_width=True)
    with right:
        st.subheader("Semaforo Gerencial")
        status = pd.DataFrame(
            [
                {"tema": "Dashboard pronto", "status": "Sim" if snapshot["health"]["ready"] else "Nao"},
                {"tema": "Gate de qualidade", "status": classic["quality_status"]},
                {"tema": "Inadimplencia AR", "status": pct(summary["inadimplencia_ar_pct"])},
                {"tema": "Pressao AP vencido", "status": pct(summary["pressao_ap_pct"])},
                {"tema": "Fornecedores em revisao", "status": integer(governance["review_count"])},
                {"tema": "Pendencias de nome", "status": integer(governance["pending_count"])},
            ]
        )
        st.dataframe(status, use_container_width=True, hide_index=True)

    st.subheader("Concentracao da Carteira")
    concentration = pd.DataFrame(
        [
            {"indicador": "Maior cliente em AR", "valor": brl(summary["ar_maior_cliente"])},
            {"indicador": "Maior fornecedor em AP", "valor": brl(summary["ap_maior_fornecedor"])},
            {"indicador": "Titulos AR", "valor": integer(summary["titulos_ar"])},
            {"indicador": "Titulos AP", "valor": integer(summary["titulos_ap"])},
        ]
    )
    st.dataframe(concentration, use_container_width=True, hide_index=True)


def render_executive(
    snapshot: dict[str, Any],
    monthly_period: pd.DataFrame,
    ap_period: pd.DataFrame,
    ar_period: pd.DataFrame,
    cash_period: pd.DataFrame,
    label: str,
) -> None:
    classic = snapshot["classic_kpis"]
    
    # Compute summary from dataframes, with fallback to classic_kpis
    summary = compute_bling_management_summary(ap_period, ar_period, cash_period)
    
    # Fallback to classic_kpis when dataframes are empty (e.g., on Render)
    if summary["ar_total"] == 0 and summary["ap_total"] == 0:
        classic_kpis = snapshot.get("classic_kpis", {})
        summary["ar_total"] = classic_kpis.get("ar_aberto", 0)
        summary["ap_total"] = classic_kpis.get("ap_aberto", 0)
        summary["ar_vencido"] = classic_kpis.get("ar_vencido", 0)
        summary["ap_vencido"] = classic_kpis.get("ap_vencido", 0)
        summary["ar_a_vencer"] = summary["ar_total"] - summary["ar_vencido"]
        summary["ap_a_vencer"] = summary["ap_total"] - summary["ap_vencido"]
        summary["saldo_aberto"] = summary["ar_total"] - summary["ap_total"]
        summary["fluxo_30d"] = classic_kpis.get("fluxo_liquido_previsto_30d", 0)
    
    dre_summary = period_summary(monthly_period)
    proxy_period = has_proxy_rows(monthly_period)

    st.header("Painel Executivo Financeiro")
    st.subheader("Exposicao Consolidada")
    metric_row(
        [
            ("AR Total", brl(summary["ar_total"])),
            ("AP Total", brl(summary["ap_total"])),
            ("Saldo em Aberto", brl(summary["saldo_aberto"])),
            ("Fluxo Liquido 30d", brl(summary["fluxo_30d"])),
        ]
    )

    if proxy_period:
        st.subheader("DRE Proxy do Periodo")
        metric_row(
            [
                ("Receita NF-e", brl(dre_summary["receita_liquida"])),
                ("CMV 2026", brl(dre_summary["cmv_proxy"])),
                ("Despesa AP Proxy", brl(dre_summary["custo_fixo_base"])),
                ("EBITDA Proxy", brl(dre_summary["ebitda"])),
            ]
        )

    st.subheader("A Vencer vs Vencido")
    metric_row(
        [
            ("AR A Vencer", brl(summary["ar_a_vencer"])),
            ("AR Vencido", brl(summary["ar_vencido"])),
            ("AP A Vencer", brl(summary["ap_a_vencer"])),
            ("AP Vencido", brl(summary["ap_vencido"])),
        ]
    )

    aging_ap = as_frame(classic["aging_ap"])
    aging_ar = as_frame(classic["aging_ar"])
    left, right = st.columns(2)
    with left:
        st.subheader("Aging de Contas a Pagar")
        if not aging_ap.empty:
            st.bar_chart(aging_ap.set_index("bucket")["valor"], use_container_width=True)
            show = aging_ap.rename(columns={"bucket": "faixa", "valor": "valor"})
            show["valor"] = show["valor"].map(brl)
            st.dataframe(show, use_container_width=True, hide_index=True)
    with right:
        st.subheader("Aging de Contas a Receber")
        if not aging_ar.empty:
            st.bar_chart(aging_ar.set_index("bucket")["valor"], use_container_width=True)
            show = aging_ar.rename(columns={"bucket": "faixa", "valor": "valor"})
            show["valor"] = show["valor"].map(brl)
            st.dataframe(show, use_container_width=True, hide_index=True)

    st.subheader("Indicadores de Risco")
    risk = pd.DataFrame(
        [
            {"indicador": "Inadimplencia AR", "valor": pct(summary["inadimplencia_ar_pct"])},
            {"indicador": "Pressao AP vencido", "valor": pct(summary["pressao_ap_pct"])},
            {"indicador": "Menor caixa 30d", "valor": brl(summary["menor_caixa_30d"])},
            {"indicador": "Maior cliente em AR", "valor": brl(summary["ar_maior_cliente"])},
            {"indicador": "Maior fornecedor em AP", "valor": brl(summary["ap_maior_fornecedor"])},
        ]
    )
    st.dataframe(risk, use_container_width=True, hide_index=True)


def render_dre(
    snapshot: dict[str, Any],
    monthly_legacy_period: pd.DataFrame,
    monthly_legacy_all: pd.DataFrame,
    monthly_bling_period: pd.DataFrame,
    monthly_bling_all: pd.DataFrame,
    label: str,
    year: int | None,
    month: int | None,
) -> None:
    ap_details = account_detail_frame(
        snapshot,
        "ap_details",
        snapshot.get("cash_projection", {}).get("top_outflows"),
        "pagar",
    )
    ap_period = filter_dated(ap_details, year, month)

    st.header("DRE e EBITDA")
    source_options = []
    if not monthly_bling_all.empty:
        source_options.append("ERP Bling")
    if not monthly_legacy_all.empty:
        source_options.append("Finance Recon Hub")
    if not source_options:
        source_options = ["ERP Bling"]
    selected_source = st.radio("Fonte do DRE", options=source_options, horizontal=True)
    if selected_source == "ERP Bling":
        monthly_period = monthly_bling_period
        monthly_all = monthly_bling_all
        source_label = "ERP Bling"
    else:
        monthly_period = monthly_legacy_period
        monthly_all = monthly_legacy_all
        source_label = "Finance Recon Hub"

    summary = period_summary(monthly_period)
    proxy_period = has_proxy_rows(monthly_period)
    tab_resumo, tab_analitico, tab_cmv = st.tabs(["Resumo", "Analitico", "Detalhe do CMV"])

    with tab_resumo:
        if proxy_period:
            commercial_sales_total = effective_sales_total(year, month)
            cmv_sales_total = (
                float(pd.to_numeric(monthly_period.get("cmv_sales_cost"), errors="coerce").fillna(0.0).sum())
                if "cmv_sales_cost" in monthly_period.columns
                else 0.0
            )
            cmv_sales_pct = (cmv_sales_total / commercial_sales_total) if commercial_sales_total else 0.0
            metric_rows(
                [
                    [
                        ("Faturamento NF-e", brl(summary["receita_liquida"])),
                        ("Vendas efetivas", brl(commercial_sales_total)),
                    ],
                    [
                        ("Custo total das vendas efetivas", brl(cmv_sales_total)),
                        ("CMV sobre vendas", pct(cmv_sales_pct)),
                    ],
                    [
                        (f"Despesa AP {source_label}", brl(summary["custo_fixo_base"])),
                        (f"EBITDA {source_label}", brl(summary["ebitda"])),
                    ],
                ]
            )
            st.caption(
                "Base ERP Bling: receita por NF-e emitida, CMV por custo dos itens vendidos e despesas por contas a pagar totais sem cancelados."
            )
        else:
            metric_grid(
                [
                    ("Receita Liquida", brl(summary["receita_liquida"])),
                    ("CMV %", pct(summary["cmv_pct"])),
                    ("Custos Variaveis", brl(summary["custos_variaveis_total"])),
                    ("Custo Fixo Base", brl(summary["custo_fixo_base"])),
                    ("EBITDA", brl(summary["ebitda"])),
                ],
                columns=2,
            )

        if not monthly_all.empty:
            st.subheader("Historico Mensal")
            st.line_chart(
                monthly_all.set_index("periodo")[["receita_liquida", "margem_contribuicao", "ebitda"]],
                use_container_width=True,
            )

        if not monthly_period.empty:
            show = monthly_period.copy()
            for col in [
                "receita_liquida",
                "cmv_proxy",
                "custos_variaveis_total",
                "custo_fixo_base",
                "margem_contribuicao",
                "ebitda",
            ]:
                show[col] = show[col].map(brl)
            if "despesas_ap_proxy" in show.columns:
                show["despesas_ap_proxy"] = show["despesas_ap_proxy"].map(brl)
            if proxy_period and "cmv_proxy" in monthly_period.columns:
                cmv_series = pd.to_numeric(monthly_period["cmv_proxy"], errors="coerce").fillna(0.0)
                receita_series = pd.to_numeric(monthly_period["receita_liquida"], errors="coerce").fillna(0.0)
                show["cmv_pct"] = [pct((cmv / receita) if receita else 0.0) for cmv, receita in zip(cmv_series, receita_series, strict=False)]
            elif {"receita_liquida", "margem_contribuicao", "custos_variaveis_total"} <= set(monthly_period.columns):
                cmv_series = (
                    pd.to_numeric(monthly_period["receita_liquida"], errors="coerce").fillna(0.0)
                    - pd.to_numeric(monthly_period["margem_contribuicao"], errors="coerce").fillna(0.0)
                    - pd.to_numeric(monthly_period["custos_variaveis_total"], errors="coerce").fillna(0.0)
                )
                receita_series = pd.to_numeric(monthly_period["receita_liquida"], errors="coerce").fillna(0.0)
                show["cmv_pct"] = [pct((cmv / receita) if receita else 0.0) for cmv, receita in zip(cmv_series, receita_series, strict=False)]
            st.subheader("Tabela do Periodo")
            if proxy_period:
                cols = ["periodo", "receita_liquida", "cmv_proxy", "cmv_pct", "despesas_ap_proxy", "ebitda"]
                labels = {
                    "periodo": "periodo",
                    "receita_liquida": "receita_nfe",
                    "cmv_proxy": "cmv_erp",
                    "cmv_pct": "cmv_pct",
                    "despesas_ap_proxy": "despesa_ap_erp",
                    "ebitda": "ebitda_erp",
                }
            else:
                cols = [
                    "periodo",
                    "receita_liquida",
                    "cmv_pct",
                    "custos_variaveis_total",
                    "custo_fixo_base",
                    "margem_contribuicao",
                    "ebitda",
                ]
                labels = None
            st.dataframe(show[cols].rename(columns=labels) if labels else show[cols], use_container_width=True, hide_index=True)

    with tab_analitico:
        commercial_sales_total = effective_sales_total(year, month)
        capital_atual = 0.0
        cmv_sales_total = (
            float(pd.to_numeric(monthly_period.get("cmv_sales_cost"), errors="coerce").fillna(0.0).sum())
            if "cmv_sales_cost" in monthly_period.columns
            else 0.0
        )
        receita_total = float(summary["receita_liquida"])
        custos_variaveis_total = float(summary["cmv_proxy"] + summary["custos_variaveis_total"])
        margem_contribuicao = float(summary["margem_contribuicao"])
        custo_fixo_total = float(summary["custo_fixo_base"])
        ebitda = float(summary["ebitda"])
        margem_contribuicao_pct = (margem_contribuicao / receita_total) if receita_total else 0.0
        rentabilidade_capital = (ebitda / capital_atual) if capital_atual else 0.0

        receita_items = pd.DataFrame(
            [
                {"item": "Faturamento NF-e", "valor": receita_total},
                {"item": "Vendas efetivas", "valor": commercial_sales_total},
            ]
        )
        receita_items["valor_fmt"] = receita_items["valor"].map(brl)

        custos_variaveis_items = pd.DataFrame(
            [
                {
                    "item": "CMV Proxy",
                    "valor": float(summary["cmv_proxy"]),
                    "pct": (float(summary["cmv_proxy"]) / receita_total) if receita_total else 0.0,
                },
                {
                    "item": "Custos variaveis adicionais",
                    "valor": float(summary["custos_variaveis_total"]),
                    "pct": (float(summary["custos_variaveis_total"]) / receita_total) if receita_total else 0.0,
                },
                {
                    "item": "Custo das vendas efetivas",
                    "valor": cmv_sales_total,
                    "pct": (cmv_sales_total / commercial_sales_total) if commercial_sales_total else 0.0,
                },
            ]
        )
        custos_variaveis_items["valor_fmt"] = custos_variaveis_items["valor"].map(brl)
        custos_variaveis_items["pct_fmt"] = custos_variaveis_items["pct"].map(pct)

        resultado_items = pd.DataFrame(
            [
                {"item": "Margem de contribuicao", "valor": margem_contribuicao, "aux": pct(margem_contribuicao_pct)},
                {"item": "Custos fixos mensais", "valor": custo_fixo_total, "aux": pct((custo_fixo_total / receita_total) if receita_total else 0.0)},
                {"item": "Resultado operacional (EBITDA)", "valor": ebitda, "aux": pct(summary["margem_ebitda_pct"])},
                {"item": "Rentabilidade sobre capital atual", "valor": capital_atual, "aux": pct(rentabilidade_capital)},
            ]
        )
        resultado_items["valor_fmt"] = resultado_items["valor"].map(brl)

        analysis_year = year if year is not None else datetime.now().year
        dre_monthly = monthly_all[monthly_all["ano"] == int(analysis_year)].copy() if not monthly_all.empty else pd.DataFrame()
        month_cols = []
        month_labels: dict[int, str] = {}
        if not dre_monthly.empty:
            month_cols = sorted(dre_monthly["mes_num"].dropna().astype(int).unique().tolist())
            month_labels = {mes: MONTH_NAMES.get(mes, str(mes))[:3] for mes in month_cols}

        classification_rows = (snapshot.get("ap_classification") or {}).get("category_monthly") or []
        classification_df = pd.DataFrame(classification_rows)
        if not classification_df.empty and {"mes", "label", "valor"} <= set(classification_df.columns):
            classification_df["valor"] = pd.to_numeric(classification_df["valor"], errors="coerce").fillna(0.0)
            classification_df["ano"] = classification_df["mes"].astype(str).str[:4].astype(int)
            classification_df["mes_num"] = classification_df["mes"].astype(str).str[-2:].astype(int)
            classification_df = classification_df[classification_df["ano"] == int(analysis_year)].copy()
            classification_df["grupo_dre"] = classification_df["label"].apply(
                lambda value: "Custos Variaveis"
                if str(value).strip().upper().startswith("VARIAVEL")
                else "Despesas Operacionais"
            )
            variable_category_items = (
                classification_df[classification_df["grupo_dre"] == "Custos Variaveis"]
                .groupby("label", as_index=False)["valor"]
                .sum()
                .sort_values("valor", ascending=False)
                .head(8)
                .copy()
            )
            fixed_cost_items = (
                classification_df[classification_df["grupo_dre"] == "Despesas Operacionais"]
                .groupby("label", as_index=False)["valor"]
                .sum()
                .sort_values("valor", ascending=False)
                .head(12)
                .copy()
            )
            variable_month_map = {
                label: {
                    int(row["mes_num"]): float(row["valor"])
                    for _, row in group.iterrows()
                }
                for label, group in classification_df[classification_df["grupo_dre"] == "Custos Variaveis"].groupby("label")
            }
            fixed_month_map = {
                label: {
                    int(row["mes_num"]): float(row["valor"])
                    for _, row in group.iterrows()
                }
                for label, group in classification_df[classification_df["grupo_dre"] == "Despesas Operacionais"].groupby("label")
            }
        else:
            variable_category_items = pd.DataFrame(columns=["label", "valor"])
            fixed_cost_items = pd.DataFrame(columns=["label", "valor"])
            variable_month_map = {}
            fixed_month_map = {}

        if fixed_cost_items.empty and not ap_period.empty:
            fixed_cost_items = (
                ap_period.groupby("fornecedor", dropna=False, as_index=False)["valor"]
                .sum()
                .sort_values("valor", ascending=False)
                .head(12)
                .copy()
                .rename(columns={"fornecedor": "label"})
            )
            fixed_cost_items["label"] = fixed_cost_items["label"].replace("", "N/D")
            fixed_month_map = {str(row["label"]): {} for _, row in fixed_cost_items.iterrows()}

        nfe_sales_by_month = {}
        sales_df = load_effective_sales_frame()
        if not sales_df.empty:
            sales_work = sales_df[sales_df["data"].dt.year == int(analysis_year)].copy()
            if not sales_work.empty:
                nfe_sales_by_month = (
                    sales_work.groupby(sales_work["data"].dt.month)["receita"].sum().to_dict()
                )

        month_rows: list[tuple[str, dict[int, float]]] = [
            ("CAPITAL ATUAL INVESTIDO", {mes: capital_atual for mes in month_cols}),
            ("RECEITA TOTAL", {mes: float(dre_monthly.loc[dre_monthly["mes_num"] == mes, "receita_liquida"].sum()) for mes in month_cols}),
            ("VENDA EFETIVA", {mes: float(nfe_sales_by_month.get(mes, 0.0)) for mes in month_cols}),
            ("CMV / CUSTOS VARIAVEIS", {mes: float(dre_monthly.loc[dre_monthly["mes_num"] == mes, "cmv_proxy"].sum()) for mes in month_cols}),
            ("MARGEM DE CONTRIBUICAO", {mes: float(dre_monthly.loc[dre_monthly["mes_num"] == mes, "margem_contribuicao"].sum()) for mes in month_cols}),
            ("DESPESAS OPERACIONAIS", {mes: float(dre_monthly.loc[dre_monthly["mes_num"] == mes, "custo_fixo_base"].sum()) for mes in month_cols}),
            ("RESULTADO OPERACIONAL", {mes: float(dre_monthly.loc[dre_monthly["mes_num"] == mes, "ebitda"].sum()) for mes in month_cols}),
        ]

        pct_labels = {
            "CMV / CUSTOS VARIAVEIS",
            "MARGEM DE CONTRIBUICAO",
            "DESPESAS OPERACIONAIS",
            "RESULTADO OPERACIONAL",
        }

        matrix_rows: list[dict[str, str]] = []
        variable_detail_rows = []
        if not variable_category_items.empty:
            for row_item in variable_category_items.to_dict("records"):
                label_name = str(row_item["label"])
                variable_detail_rows.append(
                    (
                        f"   {upper_text(label_name)}",
                        variable_month_map.get(label_name, {}),
                        float(row_item["valor"]),
                    )
                )
        fixed_detail_rows = []
        if not fixed_cost_items.empty:
            for row_item in fixed_cost_items.to_dict("records"):
                label_name = str(row_item["label"])
                fixed_detail_rows.append(
                    (
                        f"   {upper_text(label_name)}",
                        fixed_month_map.get(label_name, {}),
                        float(row_item["valor"]),
                    )
                )

        for label, values_by_month in month_rows:
            label_upper = upper_text(label)
            row = {"CONTA": label_upper}
            total_row = 0.0
            for mes in month_cols:
                value = float(values_by_month.get(mes, 0.0))
                receita_mes = float(dre_monthly.loc[dre_monthly["mes_num"] == mes, "receita_liquida"].sum()) if not dre_monthly.empty else 0.0
                total_row += value
                row[upper_text(month_labels[mes])] = brl(value)
                row[f"{upper_text(month_labels[mes])} %"] = pct((value / receita_mes) if receita_mes else 0.0) if label_upper in pct_labels else ""
            row["TOTAL"] = brl(total_row)
            total_receita = float(dre_monthly["receita_liquida"].sum()) if not dre_monthly.empty else 0.0
            row["TOTAL %"] = pct((total_row / total_receita) if total_receita else 0.0) if label_upper in pct_labels else ""
            matrix_rows.append(row)
            if label_upper == "CMV / CUSTOS VARIAVEIS" and variable_detail_rows:
                for detail_label, detail_values_by_month, detail_total in variable_detail_rows:
                    detail_row = {"CONTA": detail_label}
                    for mes in month_cols:
                        month_value = float(detail_values_by_month.get(mes, 0.0))
                        detail_row[upper_text(month_labels[mes])] = brl(month_value) if month_value else ""
                        detail_row[f"{upper_text(month_labels[mes])} %"] = ""
                    detail_row["TOTAL"] = brl(detail_total)
                    detail_row["TOTAL %"] = pct((detail_total / total_receita) if total_receita else 0.0)
                    matrix_rows.append(detail_row)
            if label_upper == "DESPESAS OPERACIONAIS" and fixed_detail_rows:
                for detail_label, detail_values_by_month, detail_total in fixed_detail_rows:
                    detail_row = {"CONTA": detail_label}
                    for mes in month_cols:
                        month_value = float(detail_values_by_month.get(mes, 0.0))
                        detail_row[upper_text(month_labels[mes])] = brl(month_value) if month_value else ""
                        detail_row[f"{upper_text(month_labels[mes])} %"] = ""
                    detail_row["TOTAL"] = brl(detail_total)
                    detail_row["TOTAL %"] = pct((detail_total / total_receita) if total_receita else 0.0)
                    matrix_rows.append(detail_row)

        st.subheader(f"DRE ANALITICA EM COLUNAS - {analysis_year}")
        if matrix_rows:
            matrix_df = pd.DataFrame(matrix_rows)
            pdf_bytes = build_dre_analytic_pdf(
                matrix_df=matrix_df,
                analysis_year=int(analysis_year),
                period_label_value=label,
                source_label=source_label,
                logo_path=find_logo(),
            )
            st.download_button(
                "Exportar Analitico em PDF",
                data=pdf_bytes,
                file_name=f"dre_analitico_{analysis_year}_{source_label.lower().replace(' ', '_')}.pdf",
                mime="application/pdf",
                use_container_width=False,
            )
            render_dre_matrix(matrix_df)
        else:
            st.info("NAO HA BASE MENSAL SUFICIENTE PARA MONTAR A DRE ANALITICA EM COLUNAS PARA O ANO SELECIONADO.")

    with tab_cmv:
        proxy_info = snapshot.get("dre_bling_info") or {}
        if not proxy_period:
            st.info("O detalhamento de CMV fica disponível quando o snapshot traz a visão proxy de DRE.")
        else:
            cmv_sales_total = (
                float(pd.to_numeric(monthly_period.get("cmv_sales_cost"), errors="coerce").fillna(0.0).sum())
                if "cmv_sales_cost" in monthly_period.columns
                else 0.0
            )
            cmv_purchase_total = (
                float(pd.to_numeric(monthly_period.get("cmv_purchase_fallback"), errors="coerce").fillna(0.0).sum())
                if "cmv_purchase_fallback" in monthly_period.columns
                else 0.0
            )
            faturamento_nfe = summary["receita_liquida"]
            commercial_sales_total = effective_sales_total(year, month)
            cmv_sales_rate = (cmv_sales_total / faturamento_nfe) if faturamento_nfe else 0.0
            cmv_cover_ratio = (commercial_sales_total / summary["cmv_proxy"]) if summary["cmv_proxy"] else 0.0
            cmv_pct = (summary["cmv_proxy"] / commercial_sales_total) if commercial_sales_total else 0.0

            metric_grid(
                [
                    ("Faturamento NF-e", brl(faturamento_nfe)),
                    ("Vendas efetivas", brl(commercial_sales_total)),
                    ("Custo total das vendas efetivas", brl(cmv_sales_total)),
                    ("CMV ERP", brl(summary["cmv_proxy"])),
                    ("Vendas / CMV", f"{cmv_cover_ratio:.2f}x" if cmv_cover_ratio else "0,00x"),
                    ("CMV % sobre vendas", pct(cmv_pct)),
                    ("CMV itens vendidos %", pct(cmv_sales_rate)),
                    ("EBITDA ERP", brl(summary["ebitda"])),
                ],
                columns=2,
            )

            cmv_detail = pd.DataFrame(
                [
                    {"indicador": "Faturamento NF-e", "valor": brl(faturamento_nfe)},
                    {"indicador": "Vendas efetivas", "valor": brl(commercial_sales_total)},
                    {"indicador": "Custo total das vendas efetivas", "valor": brl(cmv_sales_total)},
                    {"indicador": "CMV ERP", "valor": brl(summary["cmv_proxy"])},
                    {"indicador": "CMV fallback compras", "valor": brl(cmv_purchase_total)},
                    {"indicador": "Relacao vendas / CMV", "valor": f"{cmv_cover_ratio:.2f}x" if cmv_cover_ratio else "0,00x"},
                    {"indicador": "CMV % sobre vendas", "valor": pct(cmv_pct)},
                    {"indicador": "CMV itens vendidos %", "valor": pct(cmv_sales_rate)},
                    {"indicador": "Formula", "valor": "Vendas efetivas / custo total das mercadorias vendidas"},
                    {"indicador": "Itens de CMV conciliados", "valor": integer(proxy_info.get("cmv_item_matched") or 0)},
                    {"indicador": "Itens de CMV sem correspondencia", "valor": integer(proxy_info.get("cmv_item_missing") or 0)},
                    {"indicador": "Taxa de conciliacao CMV", "valor": pct(float(proxy_info.get("cmv_match_rate") or 0.0))},
                ]
            )
            st.dataframe(cmv_detail, use_container_width=True, hide_index=True)


def render_cash(snapshot: dict[str, Any], cash_period: pd.DataFrame, year: int | None, month: int | None, label: str) -> None:
    cash = snapshot["cash_projection"]
    inflows = filter_dated(top_flow_frame(cash["top_inflows"]), year, month)
    outflows = filter_dated(top_flow_frame(cash["top_outflows"]), year, month)

    st.header("Caixa e Projecao")
    metric_row(
        [
            ("Entradas do Horizonte", brl(cash_period["inflow"].sum() if not cash_period.empty else cash["inflow_30d"])),
            ("Saidas do Horizonte", brl(cash_period["outflow"].sum() if not cash_period.empty else cash["outflow_30d"])),
            ("Liquido do Horizonte", brl(cash_period["net"].sum() if not cash_period.empty else cash["net_30d"])),
            ("Pior Acumulado", brl(cash_period["cumulative_net"].min() if not cash_period.empty else cash["min_cumulative_30d"])),
        ]
    )

    if not cash_period.empty:
        st.subheader("Fluxo Diario")
        st.line_chart(cash_period.set_index("data_label")[["inflow", "outflow", "net"]], use_container_width=True)
        st.subheader("Acumulado Liquido")
        st.line_chart(cash_period.set_index("data_label")[["cumulative_net"]], use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.subheader("Principais Entradas")
        if not inflows.empty:
            show = inflows.copy()
            show["valor"] = show["valor"].map(brl)
            st.dataframe(show[["data_label", "contato", "valor"]], use_container_width=True, hide_index=True)
    with right:
        st.subheader("Principais Saidas")
        if not outflows.empty:
            show = outflows.copy()
            show["valor"] = show["valor"].map(brl)
            st.dataframe(show[["data_label", "contato", "valor"]], use_container_width=True, hide_index=True)


def render_cash_management(
    snapshot: dict[str, Any],
    bank_balances: pd.DataFrame,
    cash_all: pd.DataFrame,
    ap_all: pd.DataFrame,
    ar_all: pd.DataFrame,
    selected_company: str,
    selected_year: int | None,
    selected_month: int | None,
    selected_weeks: int,
) -> None:
    work_banks = bank_balances.copy() if not bank_balances.empty else pd.DataFrame()
    if not work_banks.empty and selected_company != "Todas" and "company" in work_banks.columns:
        work_banks = work_banks[
            work_banks["company"].fillna("").astype(str).str.upper() == str(selected_company).upper()
        ].copy()

    total_balance = float(work_banks["balance"].sum()) if not work_banks.empty else 0.0
    horizon = cash_all.copy() if not cash_all.empty else pd.DataFrame()
    ap_future = ap_all.copy() if not ap_all.empty else pd.DataFrame()
    ar_future = ar_all.copy() if not ar_all.empty else pd.DataFrame()
    dup_future = load_duplicatas_garantia()
    if selected_company != "Todas":
        if not ap_future.empty and "company" in ap_future.columns:
            ap_future = ap_future[ap_future["company"].fillna("").astype(str).str.upper() == str(selected_company).upper()].copy()
        if not ar_future.empty and "company" in ar_future.columns:
            ar_future = ar_future[ar_future["company"].fillna("").astype(str).str.upper() == str(selected_company).upper()].copy()
        if not dup_future.empty and "company" in dup_future.columns:
            dup_future = dup_future[dup_future["company"].fillna("").astype(str).str.upper() == str(selected_company).upper()].copy()
    if not horizon.empty:
        horizon = horizon.sort_values("data").reset_index(drop=True)
        horizon["projected_balance"] = total_balance + pd.to_numeric(
            horizon["cumulative_net"], errors="coerce"
        ).fillna(0.0)
    ap_monthly = future_flow_monthly(ap_future)
    ar_monthly = future_flow_monthly(ar_future)
    dup_monthly = future_flow_monthly(dup_future)

    def _month_label(ts: pd.Timestamp) -> str:
        return f"{MONTH_NAMES.get(int(ts.month), str(ts.month))}/{int(ts.year)}"

    def _sum_between_dates(frame: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> float:
        if frame.empty or "data_vencimento" not in frame.columns:
            return 0.0
        work = frame.copy()
        work["data_vencimento"] = pd.to_datetime(work["data_vencimento"], errors="coerce")
        work["valor"] = pd.to_numeric(work["valor"], errors="coerce").fillna(0.0)
        mask = (work["data_vencimento"] >= start_date) & (work["data_vencimento"] <= end_date)
        return float(work.loc[mask, "valor"].sum())

    horizon_projection_rows: list[dict[str, Any]] = []
    today = pd.Timestamp(datetime.now().date())
    if selected_year is not None and selected_month is not None:
        base_month = pd.Timestamp(year=int(selected_year), month=int(selected_month), day=1)
    else:
        base_month = today.replace(day=1)
    month_starts = [base_month + pd.DateOffset(months=offset) for offset in range(6)]
    running_month_balance = total_balance
    for month_start in month_starts:
        month_end = month_start + pd.offsets.MonthEnd(0)
        period_start = max(month_start, today) if selected_month is None else month_start
        ar_h = _sum_between_dates(ar_future, period_start, month_end)
        ap_h = _sum_between_dates(ap_future, period_start, month_end)
        dup_h = _sum_between_dates(dup_future, period_start, month_end)
        caixa_h = ar_h - ap_h - dup_h
        opening_balance = running_month_balance
        closing_balance = opening_balance + caixa_h
        horizon_projection_rows.append(
            {
                "janela": _month_label(month_start),
                "banco_inicial": opening_balance,
                "a_receber": ar_h,
                "a_pagar": ap_h,
                "duplicatas_garantia": dup_h,
                "caixa": caixa_h,
                "banco_projetado": closing_balance,
            }
        )
        running_month_balance = closing_balance
    horizon_projection = pd.DataFrame(horizon_projection_rows)

    current_month_label = _month_label(month_starts[0]) if month_starts else "Atual"
    third_month_label = _month_label(month_starts[2]) if len(month_starts) >= 3 else current_month_label
    sixth_month_label = _month_label(month_starts[5]) if len(month_starts) >= 6 else current_month_label
    proj_current = total_balance
    proj_3m = total_balance
    proj_6m = total_balance
    min_balance = total_balance
    risk_days = 0
    ar_total_future = float(ar_monthly["valor"].sum()) if not ar_monthly.empty else 0.0
    ap_total_future = float(ap_monthly["valor"].sum()) if not ap_monthly.empty else 0.0
    dup_total_future = float(dup_monthly["valor"].sum()) if not dup_monthly.empty else 0.0
    ap_total_future_combined = ap_total_future + dup_total_future
    if not horizon_projection.empty:
        proj_current = float(horizon_projection.iloc[0]["banco_projetado"])
        proj_3m = float(horizon_projection.iloc[min(2, len(horizon_projection) - 1)]["banco_projetado"])
        proj_6m = float(horizon_projection.iloc[min(5, len(horizon_projection) - 1)]["banco_projetado"])
    if not horizon.empty:
        min_balance = float(horizon["projected_balance"].min())
        risk_days = int((horizon["projected_balance"] < 0).sum())

    st.markdown('<div style="height: 2.4rem;"></div>', unsafe_allow_html=True)

    render_centered_metric_rows(
        [
            [("Saldo Bancario Atual", brl(total_balance))],
            [
                ("A Receber Futuro", brl(ar_total_future)),
                ("A Pagar Futuro", brl(ap_total_future_combined)),
            ],
            [
                (f"Saldo Projetado {current_month_label}", brl(proj_current)),
                (f"Saldo Projetado {third_month_label}", brl(proj_3m)),
            ],
            [
                (f"Saldo Projetado {sixth_month_label}", brl(proj_6m)),
                ("Menor Saldo Projetado", brl(min_balance)),
            ],
        ],
        highlight_labels={"Saldo Bancario Atual"},
    )

    if not horizon_projection.empty:
        st.subheader("Fluxo Projetado por Horizonte")
        horizon_map = {
            str(row["janela"]): {
                "banco_inicial": float(row.get("banco_inicial", total_balance)),
                "a_receber": float(row["a_receber"]),
                "a_pagar": float(row["a_pagar"]),
                "duplicatas_garantia": float(row.get("duplicatas_garantia", 0.0)),
                "caixa": float(row["caixa"]),
                "banco_projetado": float(row["banco_projetado"]),
            }
            for _, row in horizon_projection.iterrows()
        }
        horizon_cols = [str(row["janela"]) for _, row in horizon_projection.iterrows()]
        rows: list[dict[str, str]] = []
        total_bancos_row = {"valor": "TOTAL BANCOS"}
        ar_total_row = {"valor": "TOTAL A RECEBER"}
        ap_total_row = {"valor": "TOTAL A PAGAR"}
        saldo_row = {"valor": "SALDO LIQUIDO"}
        for col in horizon_cols:
            values = horizon_map.get(
                col,
                {
                    "banco_inicial": total_balance,
                    "a_receber": 0.0,
                    "a_pagar": 0.0,
                    "duplicatas_garantia": 0.0,
                    "caixa": 0.0,
                    "banco_projetado": total_balance,
                },
            )
            total_bancos_row[col] = brl(values["banco_inicial"])
            ar_total_row[col] = brl(values["a_receber"])
            ap_total_row[col] = brl(-abs(values["a_pagar"] + values["duplicatas_garantia"]))
            saldo_row[col] = brl(values["banco_projetado"])

        rows.extend(
            [
                total_bancos_row,
                ar_total_row,
                ap_total_row,
                saldo_row,
            ]
        )
        show_horizon = pd.DataFrame(rows)
        show_horizon.columns = [str(col).upper() for col in show_horizon.columns]

        def _style_horizon_row(row: pd.Series) -> list[str]:
            row_label = str(row.get("VALOR", ""))
            is_liquid = row_label.upper() == "SALDO LIQUIDO"
            styles: list[str] = []
            for _, cell in row.items():
                cell_text = str(cell or "").strip()
                cell_styles: list[str] = []
                if is_liquid:
                    cell_styles.append("font-weight: 700")
                if cell_text.startswith("R$ -"):
                    cell_styles.append("color: #c62828")
                styles.append("; ".join(cell_styles))
            return styles

        styled_horizon = bold_headers(show_horizon).apply(_style_horizon_row, axis=1)
        st.dataframe(styled_horizon, use_container_width=True, hide_index=True)


    if not horizon.empty:
        st.subheader("Fluxo Projetado por Dia")
        horizon_daily = horizon.copy()
        horizon_daily["data"] = pd.to_datetime(horizon_daily["data"], errors="coerce")
        horizon_daily = horizon_daily.dropna(subset=["data"]).sort_values("data").reset_index(drop=True)
        if selected_year is not None:
            horizon_daily = horizon_daily[horizon_daily["data"].dt.year == int(selected_year)].copy()
        if selected_month is not None:
            horizon_daily = horizon_daily[horizon_daily["data"].dt.month == int(selected_month)].copy()
            daily_start = pd.Timestamp(year=int(selected_year), month=int(selected_month), day=1)
        else:
            horizon_daily = horizon_daily.head(31).copy()
            daily_start = today

        max_days = max(int(selected_weeks or 1), 1) * 7
        horizon_daily = horizon_daily.head(max_days).copy()

        if horizon_daily.empty:
            st.info("Nao ha projecao diaria disponivel para o filtro selecionado.")
            return

        def _sum_on_day(frame: pd.DataFrame, target_day: pd.Timestamp) -> float:
            if frame.empty or "data_vencimento" not in frame.columns:
                return 0.0
            work = frame.copy()
            work["data_vencimento"] = pd.to_datetime(work["data_vencimento"], errors="coerce")
            work["valor"] = pd.to_numeric(work["valor"], errors="coerce").fillna(0.0)
            mask = work["data_vencimento"].dt.normalize() == target_day.normalize()
            return float(work.loc[mask, "valor"].sum())

        day_cols = [str(value) for value in horizon_daily["data_label"].tolist()]
        total_bancos_day_row = {"VALOR": "TOTAL BANCOS"}
        ar_day_row = {"VALOR": "TOTAL A RECEBER"}
        ap_day_row = {"VALOR": "TOTAL A PAGAR"}
        saldo_day_row = {"VALOR": "SALDO LIQUIDO"}
        running_balance = total_balance
        for _, day_row in horizon_daily.iterrows():
            day_ts = pd.Timestamp(day_row["data"])
            day_label = str(day_row["data_label"])
            opening_balance = running_balance
            ar_day = _sum_on_day(ar_future, day_ts)
            ap_day = _sum_on_day(ap_future, day_ts)
            dup_day = _sum_on_day(dup_future, day_ts)
            running_balance = running_balance + ar_day - ap_day - dup_day
            total_bancos_day_row[day_label] = brl(opening_balance)
            ar_day_row[day_label] = brl(ar_day)
            ap_day_row[day_label] = brl(-abs(ap_day + dup_day))
            saldo_day_row[day_label] = brl(running_balance)

        show_daily = pd.DataFrame(
            [
                total_bancos_day_row,
                ar_day_row,
                ap_day_row,
                saldo_day_row,
            ]
        )
        show_daily.columns = [str(col).upper() for col in show_daily.columns]

        def _style_daily_row(row: pd.Series) -> list[str]:
            row_label = str(row.get("VALOR", ""))
            is_liquid = row_label.upper() == "SALDO LIQUIDO"
            styles: list[str] = []
            for _, cell in row.items():
                cell_text = str(cell or "").strip()
                cell_styles: list[str] = []
                if is_liquid:
                    cell_styles.append("font-weight: 700")
                if cell_text.startswith("R$ -"):
                    cell_styles.append("color: #c62828")
                styles.append("; ".join(cell_styles))
            return styles

        styled_daily = bold_headers(show_daily).apply(_style_daily_row, axis=1)
        st.dataframe(styled_daily, use_container_width=True, hide_index=True)

    if work_banks.empty:
        st.info(
            "Nao ha saldos bancarios sincronizados no snapshot atual. A tela ja usa a projecao de AP/AR, "
            "mas precisa de uma base de bancos para calcular o saldo inicial consolidado."
        )
    else:
        show_banks = work_banks.copy()
        show_banks["balance"] = pd.to_numeric(show_banks["balance"], errors="coerce").fillna(0.0)
        show_banks = show_banks.sort_values("balance", ascending=False).reset_index(drop=True)
        show_banks["balance"] = show_banks["balance"].map(brl)
        display_cols = [c for c in ["company", "bank_name", "balance"] if c in show_banks.columns]
        rename_map = {
            "company": "EMPRESA",
            "bank_name": "BANCO",
            "balance": "SALDO_ATUAL",
        }
        st.subheader("Saldos por Banco")
        show_banks_display = show_banks[display_cols].rename(columns=rename_map)

        def _style_bank_balance(row: pd.Series) -> list[str]:
            styles: list[str] = []
            for col, cell in row.items():
                cell_text = str(cell or "").strip()
                if str(col).upper() == "SALDO_ATUAL" and cell_text.startswith("R$ -"):
                    styles.append("color: #c62828; font-weight: 700")
                else:
                    styles.append("")
            return styles

        styled_banks = bold_headers(show_banks_display).apply(_style_bank_balance, axis=1)
        st.dataframe(styled_banks, use_container_width=True, hide_index=True)

def render_accounts_page(
    frame: pd.DataFrame,
    page_title: str,
    entity_col: str,
    label: str,
    selected_year: int | None,
    selected_month: int | None,
    selected_company: str,
) -> None:
    st.header(page_title)
    if frame.empty:
        st.info("Nao ha detalhes disponiveis para este recorte no snapshot atual.")
        return

    work = frame.copy()
    if "dias_atraso" not in work.columns:
        work["dias_atraso"] = 0
    if "juros" not in work.columns:
        work["juros"] = 0.0
    work["dias_atraso"] = pd.to_numeric(work["dias_atraso"], errors="coerce").fillna(0).astype(int)
    work["juros"] = pd.to_numeric(work["juros"], errors="coerce").fillna(0.0)
    work["valor"] = pd.to_numeric(work["valor"], errors="coerce").fillna(0.0)
    work["status_titulo"] = work["dias_atraso"].apply(lambda x: "Vencido" if x > 0 else "A vencer")
    work["faixa_atraso"] = work["dias_atraso"].apply(aging_bucket)
    work["situacao_legivel"] = [
        status_label(raw, days, amount)
        for raw, days, amount in zip(work.get("situacao", ""), work["dias_atraso"], work["valor"], strict=False)
    ]

    total_valor = float(work["valor"].sum())
    vencidos = work[work["status_titulo"] == "Vencido"].copy()
    a_vencer = work[work["status_titulo"] == "A vencer"].copy()
    total_juros = float(work["juros"].sum())

    metric_grid(
        [
            ("Valor Total", brl(total_valor)),
            ("Valor A Vencer", brl(a_vencer["valor"].sum() if not a_vencer.empty else 0)),
            ("Valor Vencido", brl(vencidos["valor"].sum() if not vencidos.empty else 0)),
            ("Juros Identificados", brl(total_juros)),
        ],
        columns=2,
    )
    metric_row(
        [
            ("Titulos A Vencer", integer(len(a_vencer))),
            ("Titulos Vencidos", integer(len(vencidos))),
            ("Maior Atraso", integer(work["dias_atraso"].max())),
            ("Empresas no Recorte", integer(work["company"].replace("", pd.NA).dropna().nunique() if "company" in work.columns else 0)),
        ]
    )

    search_col, doc_col = st.columns(2)
    with search_col:
        search_name = st.text_input("Buscar por nome", key=f"nome_{page_title.lower().replace(' ', '_')}")
    with doc_col:
        search_doc = st.text_input("Buscar por documento", key=f"doc_{page_title.lower().replace(' ', '_')}")

    if search_name.strip():
        work = work[work[entity_col].fillna("").astype(str).str.contains(search_name.strip(), case=False, na=False)].copy()
    if search_doc.strip():
        work = work[work["documento"].fillna("").astype(str).str.contains(search_doc.strip(), case=False, na=False)].copy()

    top_entities = (
        work.groupby(entity_col, dropna=False)["valor"]
        .sum()
        .reset_index()
        .sort_values("valor", ascending=False)
        .head(15)
    )
    top_five = (
        work.groupby(entity_col, dropna=False)
        .agg(
            valor=("valor", "sum"),
            titulos=("valor", "size"),
            vencido=("valor", lambda s: float(work.loc[s.index].loc[work.loc[s.index, "status_titulo"] == "Vencido", "valor"].sum())),
            a_vencer=("valor", lambda s: float(work.loc[s.index].loc[work.loc[s.index, "status_titulo"] == "A vencer", "valor"].sum())),
        )
        .reset_index()
        .sort_values("valor", ascending=False)
        .head(5)
    )
    status_value = (
        work.groupby("status_titulo", dropna=False)["valor"]
        .sum()
        .reset_index()
        .sort_values("valor", ascending=False)
    )
    left, right = st.columns(2)
    with left:
        st.subheader(f"Maiores {entity_col.replace('_', ' ').title()}")
        if not top_entities.empty:
            st.bar_chart(top_entities.set_index(entity_col)["valor"], use_container_width=True)
    with right:
        st.subheader("Status dos Titulos")
        if not status_value.empty:
            st.bar_chart(status_value.set_index("status_titulo")["valor"], use_container_width=True)

    st.subheader(f"Top 5 {'Fornecedores' if entity_col == 'fornecedor' else 'Clientes'} em Aberto")
    if not top_five.empty:
        show_top = top_five.rename(columns={entity_col: "nome"}).copy()
        show_top["titulos"] = show_top["titulos"].map(integer)
        for col in ["valor", "vencido", "a_vencer"]:
            show_top[col] = show_top[col].map(brl)
        st.dataframe(show_top, use_container_width=True, hide_index=True)

    st.subheader("Resumo Detalhado")
    resumo_detalhado = pd.DataFrame(
        [
            {
                "indicador": "Valor Total",
                "status": "Todos",
                "titulos": len(work),
                "valor": total_valor,
            },
            {
                "indicador": "Valor A Vencer",
                "status": "A vencer",
                "titulos": len(a_vencer),
                "valor": float(a_vencer["valor"].sum()) if not a_vencer.empty else 0.0,
            },
            {
                "indicador": "Valor Vencido",
                "status": "Vencido",
                "titulos": len(vencidos),
                "valor": float(vencidos["valor"].sum()) if not vencidos.empty else 0.0,
            },
            {
                "indicador": "Juros Identificados",
                "status": "Todos",
                "titulos": len(work),
                "valor": total_juros,
            },
        ]
    )
    resumo_detalhado["titulos"] = resumo_detalhado["titulos"].map(integer)
    resumo_detalhado["valor"] = resumo_detalhado["valor"].map(brl)
    st.dataframe(resumo_detalhado, use_container_width=True, hide_index=True)

    show = work.copy()
    for col in ["valor", "juros"]:
        if col in show.columns:
            show[col] = show[col].map(brl)
    show = show.sort_values(["dias_atraso", "valor"], ascending=[False, False])
    display_cols = [
        c
        for c in [
            entity_col,
            "company",
            "documento",
            "valor",
            "data_emissao_label",
            "data_label",
            "status_titulo",
            "faixa_atraso",
            "situacao_legivel",
            "dias_atraso",
            "juros",
        ]
        if c in show.columns
    ]
    rename_map = {
        "fornecedor": "fornecedor",
        "cliente": "cliente",
        "company": "empresa",
        "documento": "documento",
        "valor": "valor",
        "data_emissao_label": "data_nota",
        "data_label": "vencimento",
        "status_titulo": "status",
        "faixa_atraso": "faixa_atraso",
        "situacao_legivel": "situacao_operacional",
        "dias_atraso": "dias_atraso",
        "juros": "juros",
    }
    st.subheader("Detalhamento")
    st.dataframe(show[display_cols].rename(columns=rename_map), use_container_width=True, hide_index=True)

    if page_title == "Contas a Pagar":
        dup_detail = load_duplicatas_garantia()
        if not dup_detail.empty:
            if selected_company != "Todas" and "company" in dup_detail.columns:
                dup_detail = dup_detail[
                    dup_detail["company"].fillna("").astype(str).str.upper() == str(selected_company).upper()
                ].copy()
            if selected_year is not None:
                dup_detail = dup_detail[dup_detail["data_vencimento"].dt.year == int(selected_year)].copy()
            if selected_month is not None:
                dup_detail = dup_detail[dup_detail["data_vencimento"].dt.month == int(selected_month)].copy()

        if not dup_detail.empty:
            st.subheader("Duplicatas en Garantia")
            dup_detail = dup_detail.sort_values("data_vencimento").copy()
            dup_detail["CLIENTE"] = dup_detail["pagador"]
            dup_detail["VALOR"] = dup_detail["valor"].map(brl)
            dup_detail["VENCIMENTO"] = dup_detail["data_vencimento"].dt.strftime("%d/%m/%Y")
            dup_detail["BANCO"] = dup_detail["banco"]
            show_dup_detail = dup_detail[["CLIENTE", "VALOR", "VENCIMENTO", "BANCO"]].copy()
            st.dataframe(bold_headers(show_dup_detail), use_container_width=True, hide_index=True)


def render_ap_governance(snapshot: dict[str, Any]) -> None:
    gov = snapshot["ap_governance"]
    governance = snapshot["governance"]

    st.header("Governanca de AP")
    metric_row(
        [
            ("Valor AP Classificado", brl(gov["ap_total_value"])),
            ("Lancamentos AP", integer(gov["ap_total_rows"])),
            ("Em Revisao", integer(governance["review_count"])),
            ("Pendentes de Nome", integer(governance["pending_count"])),
        ]
    )

    left, right = st.columns(2)
    with left:
        st.subheader("Mapeamento por Valor")
        mapped = as_frame(gov["mapped_status_value"])
        if not mapped.empty:
            st.bar_chart(mapped.set_index("label")["valor"], use_container_width=True)
    with right:
        st.subheader("Categorias com Maior Exposicao")
        category = as_frame(gov["category_value"]).head(10)
        if not category.empty:
            st.bar_chart(category.set_index("label")["valor"], use_container_width=True)

    low_left, low_right = st.columns(2)
    with low_left:
        st.subheader("Top Fornecedores")
        suppliers = as_frame(gov["top_suppliers"])
        if not suppliers.empty:
            suppliers["valor"] = suppliers["valor"].map(brl)
            st.dataframe(suppliers.rename(columns={"label": "fornecedor"}), use_container_width=True, hide_index=True)
    with low_right:
        st.subheader("Subcategorias Criticas")
        sub = as_frame(gov["subcategory_value"]).head(12)
        if not sub.empty:
            sub["valor"] = sub["valor"].map(brl)
            st.dataframe(sub.rename(columns={"label": "subcategoria"}), use_container_width=True, hide_index=True)


def render_quality(snapshot: dict[str, Any]) -> None:
    quality = snapshot["quality_reconciliation"]

    st.header("Qualidade e Reconciliacao")
    metric_row(
        [
            ("Checks OK", f"{quality['quality_check_ok']}/{quality['quality_check_total']}"),
            ("Health Ready", "Sim" if quality["health_ready"] else "Nao"),
            ("Falhas de Check", integer(quality["quality_check_fail"])),
            ("Gate Atual", quality["gate_detail"] or "N/D"),
        ]
    )

    rows = []
    for label, item in [
        ("Reconciliation geral", quality["latest_reconciliation"]),
        ("Reconciliation CZ", quality["latest_reconciliation_cz"]),
        ("Reconciliation CR", quality["latest_reconciliation_cr"]),
        ("Finance ingest hub", quality["latest_ingest"]),
        ("Import generator", quality["latest_import_generator"]),
        ("Cutover health", quality["latest_cutover_health"]),
    ]:
        rows.append(
            {
                "processo": label,
                "status": item.get("status", ""),
                "run_id": item.get("run_id", ""),
                "gerado_em": item.get("generated_at", ""),
                "arquivo": item.get("name", ""),
            }
        )
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def render_qa(snapshot: dict[str, Any]) -> None:
    qa = snapshot["qa"]
    governance = snapshot["governance"]

    st.header("QA e Governanca")
    metric_row(
        [
            ("Warn QA", integer(qa["warn_count"])),
            ("Fail QA", integer(qa["fail_count"])),
            ("Meses EBITDA Negativo", integer(qa["negative_ebitda_months"])),
            ("Fornecedores em Revisao", integer(governance["review_count"])),
        ]
    )

    left, right = st.columns(2)
    with left:
        st.subheader("Checks de QA")
        checks = as_frame(qa["checks"])
        if not checks.empty:
            st.dataframe(checks, use_container_width=True, hide_index=True)
    with right:
        st.subheader("Top Fornecedores em Revisao")
        review = as_frame(governance["top_review"])
        if not review.empty:
            review["valor_total"] = review["valor_total"].map(brl)
            st.dataframe(review, use_container_width=True, hide_index=True)

    pending = as_frame(governance["top_pending"])
    if not pending.empty:
        pending["valor_total"] = pending["valor_total"].map(brl)
        st.subheader("Pendencias de Nome")
        st.dataframe(pending, use_container_width=True, hide_index=True)


def main() -> None:
    inject_styles()
    snapshot_mtime_ns = SNAPSHOT_PATH.stat().st_mtime_ns if SNAPSHOT_PATH.exists() else 0
    snapshot = load_snapshot(snapshot_mtime_ns)
    logo_path = find_logo()
    monthly_all = monthly_frame(snapshot)
    monthly_bling_all = monthly_frame(snapshot, "monthly_bling")
    bank_balances_all = bank_balance_frame(snapshot)
    ap_details_all = account_detail_frame(snapshot, "ap_details", snapshot.get("cash_projection", {}).get("top_outflows"), "pagar")
    ar_details_all = account_detail_frame(snapshot, "ar_details", snapshot.get("cash_projection", {}).get("top_inflows"), "receber")
    cash_all = cash_days_frame(snapshot)
    years, month_map = period_options_from_frames([monthly_all, monthly_bling_all, cash_all, ap_details_all, ar_details_all])
    current_year = datetime.now().year
    selected_year = current_year if current_year in years else (years[-1] if years else None)
    selected_month = None
    selected_company = "Todas"
    selected_weeks = 1
    selected_counterparty = "Todos"
    page_groups = {
        "Executivo": [
            "Resumo Executivo",
            "Painel Executivo Financeiro",
            "DRE e EBITDA",
        ],
        "Caixa": [
            "Gestao de Caixa",
            "Caixa e Projecao",
        ],
        "Contas": [
            "Contas a Pagar",
            "Contas a Receber",
        ],
        "Governanca": [
            "Governanca de AP",
            "Qualidade e Reconciliacao",
            "QA e Governanca",
        ],
    }

    with st.sidebar:
        if logo_path is not None:
            render_logo(logo_path, sidebar=True)
        st.markdown("### **Secao**")
        selected_section = st.selectbox(
            "Secao",
            options=list(page_groups.keys()),
            index=0,
            label_visibility="collapsed",
        )
        st.markdown("### **Pagina**")
        page = st.selectbox(
            "Pagina",
            options=page_groups[selected_section],
            index=0,
            label_visibility="collapsed",
        )
        company_options = ["Todas"]
        company_values = sorted(
            {
                value
                for frame in [ap_details_all, ar_details_all, cash_all, bank_balances_all]
                if not frame.empty and "company" in frame.columns
                for value in frame["company"].fillna("").astype(str).str.strip().tolist()
                if value
            }
        )
        company_options.extend(company_values)
        st.markdown('<div class="sidebar-box">', unsafe_allow_html=True)
        st.markdown("### Empresa")
        selected_company = st.selectbox("Empresa", options=company_options, index=0)
        selected_account_company = normalize_account_company(selected_company)
        if page == "Contas a Pagar":
            ap_filter_frame = filter_company(filter_dated(ap_details_all, selected_year, selected_month), selected_account_company)
            supplier_options = ["Todos"]
            if not ap_filter_frame.empty and "fornecedor" in ap_filter_frame.columns:
                supplier_values = sorted(
                    {
                        str(value).strip()
                        for value in ap_filter_frame["fornecedor"].fillna("").astype(str).tolist()
                        if str(value).strip()
                    }
                )
                supplier_options.extend(supplier_values)
            selected_counterparty = st.selectbox("Fornecedores", options=supplier_options, index=0)
        elif page == "Contas a Receber":
            ar_filter_frame = filter_company(filter_dated(ar_details_all, selected_year, selected_month), selected_account_company)
            customer_options = ["Todos"]
            if not ar_filter_frame.empty and "cliente" in ar_filter_frame.columns:
                customer_values = sorted(
                    {
                        str(value).strip()
                        for value in ar_filter_frame["cliente"].fillna("").astype(str).tolist()
                        if str(value).strip()
                    }
                )
                customer_options.extend(customer_values)
            selected_counterparty = st.selectbox("Clientes", options=customer_options, index=0)
        st.markdown("</div>", unsafe_allow_html=True)
        year_filter_label = "Ano/Vencimento" if page in {"Contas a Pagar", "Contas a Receber"} else "Ano"
        st.markdown('<div class="sidebar-box">', unsafe_allow_html=True)
        st.markdown("### Filtros de Periodo")
        if years:
            default_year = current_year if current_year in years else years[-1]
            selected_year = st.selectbox(year_filter_label, options=years, index=years.index(default_year))
        month_label_selected = st.selectbox("Mes", options=list(month_map.keys()), index=0)
        selected_month = month_map[month_label_selected]
        if page == "Gestao de Caixa":
            selected_weeks = st.selectbox("Semanas", options=[1, 2, 3], index=0)
        st.markdown("</div>", unsafe_allow_html=True)

    current_label = period_label(selected_year, selected_month)
    monthly_period = filter_monthly(monthly_all, selected_year, selected_month)
    monthly_bling_period = filter_monthly(monthly_bling_all, selected_year, selected_month)
    cash_period = filter_company(filter_dated(cash_all, selected_year, selected_month), selected_account_company)
    ap_details_period = filter_company(filter_dated(ap_details_all, selected_year, selected_month), selected_account_company)
    ar_details_period = filter_company(filter_dated(ar_details_all, selected_year, selected_month), selected_account_company)
    if page == "Contas a Pagar" and selected_counterparty != "Todos" and "fornecedor" in ap_details_period.columns:
        ap_details_period = ap_details_period[
            ap_details_period["fornecedor"].fillna("").astype(str).str.strip() == str(selected_counterparty).strip()
        ].copy()
    if page == "Contas a Receber" and selected_counterparty != "Todos" and "cliente" in ar_details_period.columns:
        ar_details_period = ar_details_period[
            ar_details_period["cliente"].fillna("").astype(str).str.strip() == str(selected_counterparty).strip()
        ].copy()

    render_hero(
        snapshot,
        "FINANCEIRO",
        "",
        logo_path,
    )

    if page == "Resumo Executivo":
        render_overview(
            snapshot,
            monthly_period,
            monthly_all,
            ap_details_period,
            ar_details_period,
            cash_period,
            selected_year,
            selected_month,
            current_label,
        )
    elif page == "Painel Executivo Financeiro":
        render_executive(snapshot, monthly_period, ap_details_period, ar_details_period, cash_period, current_label)
    elif page == "DRE e EBITDA":
        render_dre(
            snapshot,
            monthly_period,
            monthly_all,
            monthly_bling_period,
            monthly_bling_all,
            current_label,
            selected_year,
            selected_month,
        )
    elif page == "Gestao de Caixa":
        render_cash_management(
            snapshot,
            bank_balances_all,
            cash_all,
            ap_details_all,
            ar_details_all,
            selected_account_company,
            selected_year,
            selected_month,
            selected_weeks,
        )
    elif page == "Caixa e Projecao":
        render_cash(snapshot, cash_period, selected_year, selected_month, current_label)
    elif page == "Contas a Pagar":
        render_accounts_page(
            ap_details_period,
            "Contas a Pagar",
            "fornecedor",
            current_label,
            selected_year,
            selected_month,
            selected_account_company,
        )
    elif page == "Contas a Receber":
        render_accounts_page(
            ar_details_period,
            "Contas a Receber",
            "cliente",
            current_label,
            selected_year,
            selected_month,
            selected_account_company,
        )
    elif page == "Governanca de AP":
        render_ap_governance(snapshot)
    elif page == "Qualidade e Reconciliacao":
        render_quality(snapshot)
    else:
        render_qa(snapshot)

if __name__ == "__main__":
    main()
