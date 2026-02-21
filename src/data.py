from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "out" / "base_unificada.xlsx"
BLING_VENDAS = ROOT / "bling_api" / "vendas_2026_cache.jsonl"
BLING_VENDAS_FALLBACK = ROOT / "bling_api" / "vendas_2025_cache.jsonl"
BLING_VENDEDORES = ROOT / "bling_api" / "vendedores_map.csv"
BLING_NFE_2026 = ROOT / "bling_api" / "nfe_2026_cache.jsonl"
BLING_NFE_2025 = ROOT / "bling_api" / "nfe_2025_cache.jsonl"


def _norm(col: str) -> str:
    return str(col).strip().lower().replace(" ", "_")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm(c) for c in df.columns]
    return df


def _standardize_opps(df: pd.DataFrame) -> pd.DataFrame:
    df = _normalize_columns(df)
    rename = {}
    if "volume_potencial" not in df.columns and "potencial_de_venda" in df.columns:
        rename["potencial_de_venda"] = "volume_potencial"
    if rename:
        df = df.rename(columns=rename)
    if "volume_potencial" in df.columns:
        df["volume_potencial"] = pd.to_numeric(df["volume_potencial"], errors="coerce")
    return df


def _standardize_realizado(df: pd.DataFrame) -> pd.DataFrame:
    df = _normalize_columns(df)
    if "receita" in df.columns:
        df["receita"] = pd.to_numeric(df["receita"], errors="coerce")
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
    return df


def _standardize_metas(df: pd.DataFrame) -> pd.DataFrame:
    df = _normalize_columns(df)
    if "meta" in df.columns:
        df["meta"] = pd.to_numeric(df["meta"], errors="coerce")
    if "realizado" in df.columns:
        df["realizado"] = pd.to_numeric(df["realizado"], errors="coerce")
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
    return df


@st.cache_data(show_spinner=False)
def load_bling_realizado() -> pd.DataFrame:
    cache = BLING_VENDAS if BLING_VENDAS.exists() else BLING_VENDAS_FALLBACK
    if not cache.exists():
        return pd.DataFrame()
    rows = []
    import json
    with cache.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = pd.json_normalize(json.loads(line))
            rows.append(obj)
    if not rows:
        return pd.DataFrame()
    df = pd.concat(rows, ignore_index=True)
    df = df.rename(columns={"total": "receita", "data": "data", "vendedor_id": "vendedor_id"})
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
    if "receita" in df.columns:
        df["receita"] = pd.to_numeric(df["receita"], errors="coerce")
    df["origem"] = "bling"

    if BLING_VENDEDORES.exists() and "vendedor_id" in df.columns:
        vmap = pd.read_csv(BLING_VENDEDORES)
        vmap.columns = [_norm(c) for c in vmap.columns]
        if "vendedor_id" in vmap.columns and "vendedor" in vmap.columns:
            df = df.merge(vmap[["vendedor_id", "vendedor"]], on="vendedor_id", how="left")
    return df


@st.cache_data(show_spinner=False)
def load_bling_nfe(year: int) -> pd.DataFrame:
    if year == 2026 and BLING_NFE_2026.exists():
        cache = BLING_NFE_2026
    elif year == 2025 and BLING_NFE_2025.exists():
        cache = BLING_NFE_2025
    elif BLING_NFE_2026.exists():
        cache = BLING_NFE_2026
    elif BLING_NFE_2025.exists():
        cache = BLING_NFE_2025
    else:
        return pd.DataFrame()
    rows = []
    import json
    with cache.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            rows.append(obj)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # expected fields: dataEmissao, valorNota
    if "dataEmissao" in df.columns:
        df["data"] = pd.to_datetime(df["dataEmissao"], errors="coerce")
    elif "dataOperacao" in df.columns:
        df["data"] = pd.to_datetime(df["dataOperacao"], errors="coerce")
    if "valorNota" in df.columns:
        df["valor"] = pd.to_numeric(df["valorNota"], errors="coerce")
    elif "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    return df[["data", "valor"]].dropna()


@st.cache_data(show_spinner=False)
def load_sheets(cache_buster: float | None = None) -> Dict[str, pd.DataFrame]:
    if not BASE.exists():
        return {}
    xls = pd.ExcelFile(BASE)
    sheets: Dict[str, pd.DataFrame] = {}
    for s in xls.sheet_names:
        df = pd.read_excel(BASE, sheet_name=s)
        if s == "oportunidades":
            df = _standardize_opps(df)
        elif s == "realizado":
            df = _standardize_realizado(df)
        elif s == "metas":
            df = _standardize_metas(df)
        else:
            df = _normalize_columns(df)
        sheets[s] = df
    return sheets
