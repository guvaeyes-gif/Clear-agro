from __future__ import annotations

from pathlib import Path
import os
from typing import Dict
import unicodedata

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
_PROFILE = os.getenv("CRM_PROFILE", "director").strip().lower()
if _PROFILE == "gestor" and (ROOT / "out" / "base_unificada_gestor.xlsx").exists():
    BASE = ROOT / "out" / "base_unificada_gestor.xlsx"
else:
    BASE = ROOT / "out" / "base_unificada.xlsx"
BLING_VENDAS = ROOT / "bling_api" / "vendas_2026_cache.jsonl"
BLING_VENDAS_FALLBACK = ROOT / "bling_api" / "vendas_2025_cache.jsonl"
BLING_VENDEDORES = ROOT / "bling_api" / "vendedores_map.csv"
BLING_NFE_2026 = ROOT / "bling_api" / "nfe_2026_cache.jsonl"
BLING_NFE_2025 = ROOT / "bling_api" / "nfe_2025_cache.jsonl"
BLING_CONTAS_RECEBER = ROOT / "bling_api" / "contas_receber_cache.jsonl"
BLING_CONTAS_PAGAR = ROOT / "bling_api" / "contas_pagar_cache.jsonl"
BLING_ESTOQUE = ROOT / "bling_api" / "estoque_cache.jsonl"


def _norm(col: str) -> str:
    return str(col).strip().lower().replace(" ", "_")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm(c) for c in df.columns]
    return df


def _normalize_text(value: object) -> str:
    txt = str(value or "").strip().upper()
    if not txt:
        return ""
    txt = "".join(ch for ch in unicodedata.normalize("NFKD", txt) if not unicodedata.combining(ch))
    return " ".join(txt.split())


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
    if "contato.nome" in df.columns and "cliente" not in df.columns:
        df["cliente"] = df["contato.nome"].astype(str)
    if "vendedor" not in df.columns:
        for c in ["vendedor.nome", "vendedorResponsavel.nome", "responsavel.nome", "representante.nome"]:
            if c in df.columns:
                df["vendedor"] = df[c]
                break
    if "vendedor_id" not in df.columns:
        for c in ["vendedor.id", "vendedorResponsavel.id", "responsavel.id", "representante.id"]:
            if c in df.columns:
                df["vendedor_id"] = df[c]
                break
    df["origem"] = "bling"

    if BLING_VENDEDORES.exists() and "vendedor_id" in df.columns:
        try:
            vmap = pd.read_csv(BLING_VENDEDORES, encoding="utf-8-sig")
            vmap.columns = [_norm(c) for c in vmap.columns]
            # Accept common header variants from manual CSV exports.
            if "vendedor_id" not in vmap.columns:
                for alt in ["id", "vendedorid", "vendedor_id_bling"]:
                    if alt in vmap.columns:
                        vmap = vmap.rename(columns={alt: "vendedor_id"})
                        break
            if "vendedor" not in vmap.columns:
                for alt in ["nome", "name", "vendedor_nome"]:
                    if alt in vmap.columns:
                        vmap = vmap.rename(columns={alt: "vendedor"})
                        break

            if "vendedor_id" in vmap.columns and "vendedor" in vmap.columns:
                df["vendedor_id"] = df["vendedor_id"].astype(str)
                vmap["vendedor_id"] = vmap["vendedor_id"].astype(str)
                vmap["vendedor"] = vmap["vendedor"].astype(str)
                vmap = vmap[["vendedor_id", "vendedor"]].drop_duplicates(subset=["vendedor_id"])
                df = df.merge(
                    vmap.rename(columns={"vendedor": "vendedor_map"}),
                    on="vendedor_id",
                    how="left",
                )
                if "vendedor" not in df.columns:
                    df["vendedor"] = df["vendedor_map"]
                else:
                    missing = df["vendedor"].isna() | (df["vendedor"].astype(str).str.strip() == "")
                    df.loc[missing, "vendedor"] = df.loc[missing, "vendedor_map"]
                df = df.drop(columns=["vendedor_map"], errors="ignore")
        except Exception:
            # Keep app running even if mapping file is malformed.
            pass
    if "vendedor" not in df.columns:
        df["vendedor"] = pd.NA

    # Fallback: derive seller by matching client name with local "realizado" history.
    # Keeps monthly granularity and fills only missing sellers.
    missing_vendor = df["vendedor"].isna() | (df["vendedor"].astype(str).str.strip() == "")
    if missing_vendor.any() and BASE.exists():
        try:
            local_real = pd.read_excel(BASE, sheet_name="realizado")
            local_real = _normalize_columns(local_real)
            if {"cliente", "vendedor"}.issubset(local_real.columns):
                map_df = local_real[["cliente", "vendedor"]].dropna().copy()
                map_df["cliente_key"] = map_df["cliente"].map(_normalize_text)
                map_df = map_df[map_df["cliente_key"] != ""]
                if not map_df.empty:
                    pref = (
                        map_df.groupby(["cliente_key", "vendedor"])
                        .size()
                        .reset_index(name="cnt")
                        .sort_values(["cliente_key", "cnt"], ascending=[True, False])
                        .drop_duplicates("cliente_key")
                        .set_index("cliente_key")["vendedor"]
                    )
                    df["cliente_key"] = df.get("cliente", pd.Series("", index=df.index)).map(_normalize_text)
                    mapped = df["cliente_key"].map(pref)
                    df.loc[missing_vendor, "vendedor"] = mapped.loc[missing_vendor]
                    df = df.drop(columns=["cliente_key"], errors="ignore")
        except Exception:
            pass

    df["vendedor"] = df["vendedor"].fillna("SEM_VENDEDOR")
    keep_cols = [c for c in ["data", "receita", "cliente", "vendedor", "origem"] if c in df.columns]
    df = df[keep_cols].copy()
    df = df.dropna(subset=["data", "receita"])
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


def _load_jsonl(cache: Path) -> pd.DataFrame:
    if not cache.exists():
        return pd.DataFrame()
    rows = []
    import json
    with cache.open("r", encoding="utf-8") as f:
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


def _pick_first_column(df: pd.DataFrame, options: list[str]) -> str | None:
    for c in options:
        if c in df.columns:
            return c
    return None


@st.cache_data(show_spinner=False)
def load_bling_contas(tipo: str = "receber") -> pd.DataFrame:
    cache = BLING_CONTAS_RECEBER if tipo == "receber" else BLING_CONTAS_PAGAR
    df = _load_jsonl(cache)
    if df.empty:
        return df

    val_col = _pick_first_column(df, ["valor", "valorDocumento", "valorOriginal", "total", "titulo.valor"])
    venc_col = _pick_first_column(df, ["dataVencimento", "vencimento", "dataEmissao", "competencia"])
    sit_col = _pick_first_column(df, ["situacao.descricao", "situacao.valor", "situacao", "status"])
    contato_col = _pick_first_column(df, ["contato.nome", "contato", "fornecedor.nome", "cliente.nome"])

    out = pd.DataFrame()
    out["id"] = df["id"] if "id" in df.columns else range(1, len(df) + 1)
    out["tipo"] = tipo
    out["valor"] = pd.to_numeric(df[val_col], errors="coerce") if val_col else 0.0
    out["vencimento"] = pd.to_datetime(df[venc_col], errors="coerce") if venc_col else pd.NaT
    out["situacao"] = df[sit_col].astype(str) if sit_col else "N/D"
    out["contato"] = df[contato_col].astype(str) if contato_col else "N/D"
    return out


@st.cache_data(show_spinner=False)
def load_bling_estoque() -> pd.DataFrame:
    df = _load_jsonl(BLING_ESTOQUE)
    if df.empty:
        return df

    produto_col = _pick_first_column(df, ["produto.nome", "nome", "descricao", "produto.descricao"])
    qtd_col = _pick_first_column(df, ["saldoFisicoTotal", "saldoVirtualTotal", "saldo", "estoque", "quantidade"])
    cod_col = _pick_first_column(df, ["produto.codigo", "codigo", "sku"])

    out = pd.DataFrame()
    out["id"] = df["id"] if "id" in df.columns else range(1, len(df) + 1)
    out["produto"] = df[produto_col].astype(str) if produto_col else "N/D"
    out["codigo"] = df[cod_col].astype(str) if cod_col else ""
    out["saldo"] = pd.to_numeric(df[qtd_col], errors="coerce").fillna(0) if qtd_col else 0
    return out


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
