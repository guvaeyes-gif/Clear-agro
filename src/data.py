from __future__ import annotations

from pathlib import Path
import os
from typing import Any, Dict
import unicodedata

import pandas as pd
import streamlit as st

from integrations.shared.bling_paths import resolve_bling_file
from src import metas_db

_CRM_VIEW_ERRORS: dict[str, str] = {}

ROOT = Path(__file__).resolve().parents[1]
_PROFILE = os.getenv("CRM_PROFILE", "director").strip().lower()
if _PROFILE == "gestor" and (ROOT / "out" / "base_unificada_gestor.xlsx").exists():
    BASE = ROOT / "out" / "base_unificada_gestor.xlsx"
else:
    BASE = ROOT / "out" / "base_unificada.xlsx"
BLING_VENDAS = resolve_bling_file("vendas_2026_cache.jsonl", mode="app")
BLING_VENDAS_FALLBACK = resolve_bling_file("vendas_2025_cache.jsonl", mode="app")
BLING_VENDAS_CR = resolve_bling_file("vendas_2026_cache_cr.jsonl", mode="app")
BLING_VENDAS_CR_FALLBACK = resolve_bling_file("vendas_2025_cache_cr.jsonl", mode="app")
BLING_VENDEDORES = resolve_bling_file("vendedores_map.csv", mode="app")
BLING_VENDEDORES_CR = resolve_bling_file("vendedores_map_cr.csv", mode="app")
BLING_VENDEDORES_MANUAL = resolve_bling_file("vendedores_map_manual.csv", mode="app")
BLING_VENDEDORES_CR_MANUAL = resolve_bling_file("vendedores_map_cr_manual.csv", mode="app")
BLING_PRODUTOS = resolve_bling_file("produtos_cache.jsonl", mode="app")
BLING_PRODUTOS_CR = resolve_bling_file("produtos_cache_cr.jsonl", mode="app")
BLING_NFE_2026 = resolve_bling_file("nfe_2026_cache.jsonl", mode="app")
BLING_NFE_2025 = resolve_bling_file("nfe_2025_cache.jsonl", mode="app")
BLING_NFE_2026_CR = resolve_bling_file("nfe_2026_cache_cr.jsonl", mode="app")
BLING_NFE_2025_CR = resolve_bling_file("nfe_2025_cache_cr.jsonl", mode="app")
BLING_CONTAS_RECEBER = resolve_bling_file("contas_receber_cache.jsonl", mode="app")
BLING_CONTAS_PAGAR = resolve_bling_file("contas_pagar_cache.jsonl", mode="app")
BLING_ESTOQUE = resolve_bling_file("estoque_cache.jsonl", mode="app")
NATURE_MAP_PATH = ROOT / "data" / "natureza_operacao_map.csv"
VENDOR_LINKS_PATH = ROOT / "data" / "vendor_links.csv"
CONSIGNACAO_LOTES_PATH = ROOT / "data" / "consignacao_lotes.csv"


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _crm_read_source() -> str:
    source = os.getenv("CRM_READ_SOURCE", "").strip().lower()
    if source in {"legacy", "supabase", "auto"}:
        return source
    if _env_flag("USE_SUPABASE_CRM_READ"):
        return "supabase"
    return "auto"


def _crm_read_enabled() -> bool:
    source = _crm_read_source()
    if source == "legacy":
        return False
    if source == "supabase":
        return True
    return metas_db._backend_mode() in {"postgres", "supabase-rest"}


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


def _coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def _coerce_datetime(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column in out.columns:
            out[column] = pd.to_datetime(out[column], errors="coerce")
    return out


def _fetch_crm_view(view_name: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    if not _crm_read_enabled():
        _CRM_VIEW_ERRORS[view_name] = (
            "Leitura CRM via Supabase desativada por configuracao "
            "(CRM_READ_SOURCE=legacy ou backend indisponivel)."
        )
        return pd.DataFrame()
    mode = metas_db._backend_mode()
    try:
        if mode == "postgres":
            with metas_db._connect_pg() as conn:
                with conn.cursor() as cur:
                    df = metas_db._fetch_dataframe(cur, f"select * from public.{view_name}", [])
                    _CRM_VIEW_ERRORS[view_name] = ""
                    return df
        if mode == "supabase-rest":
            query = {"select": "*", "limit": "5000"}
            if params:
                query.update({k: str(v) for k, v in params.items() if v not in (None, "")})
            rows = metas_db._rest_request("GET", view_name, params=query) or []
            _CRM_VIEW_ERRORS[view_name] = ""
            return pd.DataFrame(rows)
        _CRM_VIEW_ERRORS[view_name] = f"CRM backend nao suportado: {mode or 'indefinido'}"
    except Exception as exc:
        _CRM_VIEW_ERRORS[view_name] = f"{type(exc).__name__}: {exc}"
        return pd.DataFrame()
    return pd.DataFrame()


def get_crm_view_error(view_name: str) -> str:
    return _CRM_VIEW_ERRORS.get(view_name, "")


def _load_bling_nfe_rows(years: list[int] | None = None) -> pd.DataFrame:
    years = years or [2026, 2025]
    cache_map = {
        2026: [BLING_NFE_2026, BLING_NFE_2026_CR],
        2025: [BLING_NFE_2025, BLING_NFE_2025_CR],
    }
    caches: list[Path] = []
    for year in years:
        for path in cache_map.get(year, []):
            if path.exists():
                caches.append(path)
    if not caches:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    import json
    for cache in caches:
        with cache.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if "empresa" not in obj:
                    obj["empresa"] = "CR" if cache.name.endswith("_cr.jsonl") else "CZ"
                rows.append(obj)
    if not rows:
        return pd.DataFrame()
    return pd.json_normalize(rows)


def _load_remote_bling_nfe_rows(years: list[int] | None = None) -> pd.DataFrame:
    years = years or [2026, 2025]
    df = _fetch_crm_view("bling_nfe_documents", params={"select": "*", "limit": "5000"})
    if df.empty:
        return pd.DataFrame()
    df = _normalize_columns(df)
    rows: list[dict[str, Any]] = []
    import json
    for _, row in df.iterrows():
        payload = row.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        issue_value = payload.get("dataEmissao") or row.get("issue_datetime")
        issue_dt = pd.to_datetime(issue_value, errors="coerce")
        if pd.isna(issue_dt) or int(issue_dt.year) not in years:
            continue
        obj = dict(payload)
        obj.setdefault("id", row.get("bling_nfe_id"))
        obj.setdefault("empresa", row.get("company"))
        obj.setdefault("dataEmissao", str(issue_dt))
        obj.setdefault("numero", row.get("invoice_number"))
        obj.setdefault("chaveAcesso", row.get("access_key"))
        obj.setdefault("serie", row.get("series"))
        obj.setdefault("valorNota", row.get("total_amount"))
        obj.setdefault("valorFrete", row.get("freight_amount"))
        if not obj.get("contato"):
            obj["contato"] = {
                "nome": row.get("customer_name"),
                "numeroDocumento": row.get("customer_tax_id"),
                "endereco": {"uf": row.get("customer_state")},
            }
        if not obj.get("vendedor"):
            obj["vendedor"] = {"id": row.get("salesperson_bling_id")}
        rows.append(obj)
    if not rows:
        return pd.DataFrame()
    return pd.json_normalize(rows)


def _map_vendedor_from_local_history(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "cliente" not in df.columns:
        out = df.copy()
        if "vendedor" not in out.columns:
            out["vendedor"] = pd.NA
        if "vendedor_id" in out.columns:
            vendedor_id_txt = out["vendedor_id"].fillna("").astype(str).str.strip()
            vendedor_txt = out["vendedor"].fillna("").astype(str).str.strip()
            out["vendedor"] = vendedor_txt.mask(vendedor_txt.eq(""), vendedor_id_txt)
        out["vendedor"] = out["vendedor"].replace("", "SEM_VENDEDOR").fillna("SEM_VENDEDOR")
        return out
    out = df.copy()
    if "vendedor" not in out.columns:
        out["vendedor"] = pd.NA
    missing_vendor = out["vendedor"].isna() | (out["vendedor"].astype(str).str.strip() == "")
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
                    out["cliente_key"] = out["cliente"].map(_normalize_text)
                    mapped = out["cliente_key"].map(pref)
                    out.loc[missing_vendor, "vendedor"] = mapped.loc[missing_vendor]
                    out = out.drop(columns=["cliente_key"], errors="ignore")
        except Exception:
            pass
    if "vendedor_id" in out.columns:
        vendedor_id_txt = out["vendedor_id"].fillna("").astype(str).str.strip()
        vendedor_txt = out["vendedor"].fillna("").astype(str).str.strip()
        out["vendedor"] = vendedor_txt.mask(vendedor_txt.eq(""), vendedor_id_txt)
    out["vendedor"] = out["vendedor"].replace("", "SEM_VENDEDOR").fillna("SEM_VENDEDOR")
    return out


@st.cache_data(show_spinner=False)
def load_vendor_links() -> pd.DataFrame:
    if not VENDOR_LINKS_PATH.exists():
        return pd.DataFrame(columns=["vendedor_id", "nome_meta", "nome_exibicao", "empresa", "status"])
    try:
        df = pd.read_csv(VENDOR_LINKS_PATH, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame(columns=["vendedor_id", "nome_meta", "nome_exibicao", "empresa", "status"])
    if df.empty:
        return pd.DataFrame(columns=["vendedor_id", "nome_meta", "nome_exibicao", "empresa", "status"])
    df.columns = [_norm(c) for c in df.columns]
    rename_map = {}
    if "bling_vendedor_id" in df.columns and "vendedor_id" not in df.columns:
        rename_map["bling_vendedor_id"] = "vendedor_id"
    if "nome_meta" not in df.columns and "vendedor" in df.columns:
        rename_map["vendedor"] = "nome_meta"
    if rename_map:
        df = df.rename(columns=rename_map)
    for column in ["vendedor_id", "nome_meta", "nome_exibicao", "empresa", "status"]:
        if column not in df.columns:
            df[column] = ""
        df[column] = df[column].fillna("").astype(str).str.strip().replace("nan", "")
    df = df[df["vendedor_id"] != ""].copy()
    return df[["vendedor_id", "nome_meta", "nome_exibicao", "empresa", "status"]].drop_duplicates(
        subset=["vendedor_id"], keep="first"
    )


@st.cache_data(show_spinner=False)
def load_consignacao_lotes() -> pd.DataFrame:
    if not CONSIGNACAO_LOTES_PATH.exists():
        return pd.DataFrame(
            columns=[
                "data_remessa",
                "empresa",
                "numero_nf",
                "vendedor",
                "cliente",
                "produto_codigo",
                "produto",
                "lote",
                "vencimento_lote",
                "quantidade_remetida",
                "quantidade_vendida",
                "quantidade_devolvida",
                "status",
                "observacao",
            ]
        )
    try:
        df = pd.read_csv(CONSIGNACAO_LOTES_PATH, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    df = _normalize_columns(df)
    for col in [
        "empresa",
        "numero_nf",
        "vendedor",
        "cliente",
        "produto_codigo",
        "produto",
        "lote",
        "status",
        "observacao",
    ]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()
    if "data_remessa" not in df.columns:
        df["data_remessa"] = pd.NaT
    df["data_remessa"] = pd.to_datetime(df["data_remessa"], errors="coerce")
    if "vencimento_lote" not in df.columns:
        df["vencimento_lote"] = pd.NaT
    df["vencimento_lote"] = pd.to_datetime(df["vencimento_lote"], errors="coerce")
    for col in ["quantidade_remetida", "quantidade_vendida", "quantidade_devolvida"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["saldo_lote"] = df["quantidade_remetida"] - df["quantidade_vendida"] - df["quantidade_devolvida"]
    return df


@st.cache_data(show_spinner=False)
def load_bling_vendor_map() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for map_path in [BLING_VENDEDORES, BLING_VENDEDORES_CR, BLING_VENDEDORES_MANUAL, BLING_VENDEDORES_CR_MANUAL]:
        if not map_path.exists():
            continue
        vm = pd.DataFrame()
        for encoding in ["utf-8-sig", "latin1", "cp1252"]:
            try:
                import csv

                with map_path.open("r", encoding=encoding, newline="") as f:
                    reader = csv.reader(f)
                    header = next(reader, None)
                    if not header:
                        continue
                    norm_header = [_norm(col) for col in header]
                    rows: list[dict[str, str]] = []
                    for row in reader:
                        if not row:
                            continue
                        item = {norm_header[idx]: row[idx] if idx < len(row) else "" for idx in range(len(norm_header))}
                        rows.append(item)
                vm = pd.DataFrame(rows)
                break
            except Exception:
                vm = pd.DataFrame()
        if vm.empty:
            continue
        if "vendedor_id" not in vm.columns:
            for alt in ["id", "vendedorid", "vendedor_id_bling"]:
                if alt in vm.columns:
                    vm = vm.rename(columns={alt: "vendedor_id"})
                    break
        if "vendedor" not in vm.columns:
            for alt in ["nome", "name", "vendedor_nome"]:
                if alt in vm.columns:
                    vm = vm.rename(columns={alt: "vendedor"})
                    break
        if "vendedor_id" not in vm.columns:
            continue
        out = pd.DataFrame()
        out["vendedor_id"] = vm["vendedor_id"].fillna("").astype(str).str.strip()
        if "vendedor" in vm.columns:
            out["vendedor"] = vm["vendedor"].fillna("").astype(str).str.strip()
        else:
            out["vendedor"] = ""
        if "empresa" in vm.columns:
            out["empresa"] = vm["empresa"].fillna("").astype(str).str.strip()
        else:
            out["empresa"] = ""
        out = out[out["vendedor_id"] != ""]
        frames.append(out)
    if not frames:
        merged = pd.DataFrame(columns=["vendedor_id", "vendedor", "empresa"])
    else:
        merged = pd.concat(frames, ignore_index=True)
        merged["vendor_key"] = merged["vendedor_id"].map(_normalize_text)
        merged["name_key"] = merged["vendedor"].map(_normalize_text)
        merged["manual_priority"] = merged["vendedor"].astype(str).str.strip().ne("").astype(int)
        merged = merged.sort_values(["vendor_key", "manual_priority", "name_key", "empresa"], ascending=[True, False, False, True])
        merged = merged.drop_duplicates(subset=["vendor_key"], keep="first")
        merged = merged.drop(columns=["vendor_key", "name_key", "manual_priority"], errors="ignore").reset_index(drop=True)

    links = load_vendor_links()
    if not links.empty:
        link_map = pd.DataFrame()
        link_map["vendedor_id"] = links["vendedor_id"]
        link_map["vendedor"] = links["nome_exibicao"].mask(
            links["nome_exibicao"].eq(""),
            links["nome_meta"],
        )
        link_map["empresa"] = links["empresa"]
        if merged.empty:
            merged = link_map.copy()
        else:
            merged = merged.merge(
                link_map.rename(columns={"vendedor": "__link_vendedor", "empresa": "__link_empresa"}),
                on="vendedor_id",
                how="outer",
            )
            if "vendedor" not in merged.columns:
                merged["vendedor"] = ""
            merged["vendedor"] = merged["vendedor"].fillna("").astype(str).str.strip()
            merged["__link_vendedor"] = merged["__link_vendedor"].fillna("").astype(str).str.strip()
            merged["empresa"] = merged.get("empresa", "").fillna("").astype(str).str.strip()
            merged["__link_empresa"] = merged["__link_empresa"].fillna("").astype(str).str.strip()
            missing_name = merged["vendedor"].eq("")
            merged.loc[missing_name, "vendedor"] = merged.loc[missing_name, "__link_vendedor"]
            missing_company = merged["empresa"].eq("")
            merged.loc[missing_company, "empresa"] = merged.loc[missing_company, "__link_empresa"]
            merged = merged.drop(columns=["__link_vendedor", "__link_empresa"], errors="ignore")
    if merged.empty:
        return pd.DataFrame(columns=["vendedor_id", "vendedor", "empresa"])
    merged["vendedor_id"] = merged["vendedor_id"].fillna("").astype(str).str.strip()
    merged["vendedor"] = merged["vendedor"].fillna("").astype(str).str.strip()
    merged["empresa"] = merged["empresa"].fillna("").astype(str).str.strip()
    merged["vendor_key"] = merged["vendedor_id"].map(_normalize_text)
    merged["name_key"] = merged["vendedor"].map(_normalize_text)
    merged["manual_priority"] = merged["vendedor"].astype(str).str.strip().ne("").astype(int)
    merged = merged.sort_values(["vendor_key", "manual_priority", "name_key", "empresa"], ascending=[True, False, False, True])
    merged = merged.drop_duplicates(subset=["vendor_key"], keep="first")
    return merged.drop(columns=["vendor_key", "name_key", "manual_priority"], errors="ignore").reset_index(drop=True)


def _apply_vendor_map(df: pd.DataFrame, vendor_id_col: str = "vendedor_id", vendor_col: str = "vendedor") -> pd.DataFrame:
    if df.empty or vendor_id_col not in df.columns:
        return df
    vmap = load_bling_vendor_map()
    if vmap.empty or "vendedor_id" not in vmap.columns:
        return df
    out = df.copy()
    out[vendor_id_col] = out[vendor_id_col].fillna("").astype(str).str.strip()
    if vendor_col not in out.columns:
        out[vendor_col] = pd.NA
    out[vendor_col] = out[vendor_col].fillna("").astype(str).str.strip()
    merged = out.merge(
        vmap[["vendedor_id", "vendedor"]].rename(columns={"vendedor": "__vendedor_map"}),
        left_on=vendor_id_col,
        right_on="vendedor_id",
        how="left",
    )
    missing = merged[vendor_col].eq("")
    merged.loc[missing, vendor_col] = merged.loc[missing, "__vendedor_map"].fillna("")
    merged = merged.drop(columns=["__vendedor_map"], errors="ignore")
    if f"{vendor_id_col}_y" in merged.columns:
        merged = merged.drop(columns=[f"{vendor_id_col}_y"], errors="ignore")
    if f"{vendor_id_col}_x" in merged.columns:
        merged = merged.rename(columns={f"{vendor_id_col}_x": vendor_id_col})
    return merged


def _load_nature_map() -> dict[str, str]:
    if not NATURE_MAP_PATH.exists():
        return {}
    try:
        df = pd.read_csv(NATURE_MAP_PATH, encoding="utf-8-sig")
    except Exception:
        return {}
    if df.empty or "natureza" not in df.columns:
        return {}
    name_col = "nome" if "nome" in df.columns else None
    if not name_col:
        return {}
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        natureza = str(row.get("natureza") or "").strip()
        nome = str(row.get(name_col) or "").strip()
        if natureza and nome:
            out[natureza] = nome
    return out


CFOP_LABELS = {
    "5101": "Venda de producao do estabelecimento",
    "5102": "Venda de mercadoria adquirida de terceiros",
    "5405": "Venda com ST",
    "6101": "Venda de producao fora do estado",
    "6102": "Venda de mercadoria de terceiros fora do estado",
    "6405": "Venda interestadual com ST",
    "6917": "Remessa de mercadoria em consignacao/sem faturamento",
}


def _first_cfop_from_items(value: object) -> str:
    if not isinstance(value, list):
        return ""
    for item in value:
        if not isinstance(item, dict):
            continue
        cfop = str(item.get("cfop") or "").strip()
        if cfop:
            return cfop
    return ""


def _append_nature_labels(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "natureza" not in out.columns:
        return out
    if "itens" in out.columns:
        out["cfop"] = out["itens"].apply(_first_cfop_from_items)
    elif "cfop" not in out.columns:
        out["cfop"] = ""
    out["natureza"] = out["natureza"].astype(str)
    out["cfop"] = out["cfop"].fillna("").astype(str)
    mapped_names = _load_nature_map()
    out["natureza_nome"] = out["natureza"].map(mapped_names)
    out["natureza_nome"] = out["natureza_nome"].fillna(
        out["cfop"].map(lambda cfop: CFOP_LABELS.get(cfop, "Natureza fiscal"))
    )
    out["natureza_label"] = out.apply(
        lambda row: f"{row['natureza']} - {row['natureza_nome']}",
        axis=1,
    )
    return out


@st.cache_data(show_spinner=False)
def load_sales_targets_view() -> pd.DataFrame:
    df = _fetch_crm_view("vw_sales_targets_summary")
    if df.empty:
        return df
    df = _normalize_columns(df)
    if "status" in df.columns:
        df["status"] = df["status"].apply(metas_db._map_status_from_db)
    if "period_type" in df.columns:
        df["period_type"] = df["period_type"].apply(metas_db._map_period_from_db)
    df = _coerce_numeric(
        df,
        [
            "target_year",
            "month_num",
            "quarter_num",
            "target_value",
            "actual_value",
            "gap_value",
            "attainment_pct",
            "target_volume",
            "actual_volume",
        ],
    )
    df = _coerce_datetime(df, ["updated_at"])
    return df


@st.cache_data(show_spinner=False)
def load_sales_pipeline_view() -> pd.DataFrame:
    df = _fetch_crm_view("vw_sales_pipeline_summary")
    if df.empty:
        return df
    df = _normalize_columns(df)
    df = _coerce_numeric(
        df,
        [
            "opportunities_count",
            "pipeline_value",
            "weighted_pipeline_value",
            "avg_probability",
            "opportunities_without_next_step",
            "opportunities_with_overdue_step",
        ],
    )
    df = _coerce_datetime(df, ["expected_close_month", "last_opportunity_update"])
    return df


@st.cache_data(show_spinner=False)
def load_bling_sales_realized_view() -> pd.DataFrame:
    df = _fetch_crm_view("vw_bling_sales_realized")
    if df.empty:
        return df
    df = _normalize_columns(df)
    if "transaction_date" in df.columns and "data" not in df.columns:
        df = df.rename(columns={"transaction_date": "data"})
    if "revenue_amount" in df.columns and "receita" not in df.columns:
        df = df.rename(columns={"revenue_amount": "receita"})
    if "customer_name" in df.columns and "cliente" not in df.columns:
        df = df.rename(columns={"customer_name": "cliente"})
    if "sales_rep_name" in df.columns and "vendedor" not in df.columns:
        df = df.rename(columns={"sales_rep_name": "vendedor"})
    if "sales_rep_code" in df.columns and "vendedor_id" not in df.columns:
        df = df.rename(columns={"sales_rep_code": "vendedor_id"})
    if "company" in df.columns and "empresa" not in df.columns:
        df = df.rename(columns={"company": "empresa"})
    df = _coerce_numeric(df, ["receita"])
    df = _coerce_datetime(df, ["data", "updated_at"])
    for column in ["cliente", "vendedor", "vendedor_id", "empresa", "customer_state", "invoice_number", "natureza", "natureza_label", "cfop"]:
        if column in df.columns:
            df[column] = df[column].fillna("").astype(str).str.strip()
    return _apply_vendor_map(df, vendor_id_col="vendedor_id", vendor_col="vendedor")


@st.cache_data(show_spinner=False)
def load_sales_realized_view() -> pd.DataFrame:
    df = _fetch_crm_view("vw_sales_realized_summary")
    if df.empty:
        return df
    df = _normalize_columns(df)
    if "transaction_date" in df.columns and "data" not in df.columns:
        df = df.rename(columns={"transaction_date": "data"})
    if "revenue_amount" in df.columns and "receita" not in df.columns:
        df = df.rename(columns={"revenue_amount": "receita"})
    if "customer_name" in df.columns and "cliente" not in df.columns:
        df = df.rename(columns={"customer_name": "cliente"})
    if "sales_rep_name" in df.columns and "vendedor" not in df.columns:
        df = df.rename(columns={"sales_rep_name": "vendedor"})
    if "sales_rep_code" in df.columns and "vendedor_id" not in df.columns:
        df = df.rename(columns={"sales_rep_code": "vendedor_id"})
    if "company" in df.columns and "empresa" not in df.columns:
        df = df.rename(columns={"company": "empresa"})
    df = _coerce_numeric(df, ["receita"])
    df = _coerce_datetime(df, ["data", "due_date"])
    for column in ["cliente", "vendedor", "vendedor_id", "empresa", "customer_state", "invoice_number"]:
        if column in df.columns:
            df[column] = df[column].fillna("").astype(str).str.strip()
    return df


@st.cache_data(show_spinner=False)
def load_crm_priority_queue() -> pd.DataFrame:
    df = _fetch_crm_view("vw_crm_agent_priority_queue")
    if df.empty:
        return df
    df = _normalize_columns(df)
    df = _coerce_numeric(df, ["priority_score"])
    df = _coerce_datetime(df, ["due_at", "completed_at", "created_at", "updated_at"])
    return df


@st.cache_data(show_spinner=False)
def load_bling_realizado() -> pd.DataFrame:
    nfe_df = _load_bling_nfe_rows([2026, 2025])
    if not nfe_df.empty:
        df = nfe_df.copy()
        if "dataEmissao" in df.columns:
            df["data"] = pd.to_datetime(df["dataEmissao"], errors="coerce")
        elif "dataOperacao" in df.columns:
            df["data"] = pd.to_datetime(df["dataOperacao"], errors="coerce")
        elif "data" not in df.columns:
            df["data"] = pd.NaT
        if "valorNota" in df.columns:
            df["receita"] = pd.to_numeric(df["valorNota"], errors="coerce")
        elif "valor" in df.columns:
            df["receita"] = pd.to_numeric(df["valor"], errors="coerce")
        elif "receita" not in df.columns:
            df["receita"] = pd.NA
        customer_col = _pick_first_column(
            df,
            ["contato.nome", "cliente.nome", "contato", "cliente", "destinatario.nome", "nomeContato"],
        )
        if customer_col:
            df["cliente"] = df[customer_col].astype(str)
        elif "cliente" not in df.columns:
            df["cliente"] = ""
        doc_col = _pick_first_column(
            df,
            [
                "contato.numeroDocumento",
                "cliente.numeroDocumento",
                "numeroDocumento",
                "destinatario.numeroDocumento",
            ],
        )
        if doc_col:
            df["numero_documento"] = df[doc_col].astype(str)
        natureza_col = _pick_first_column(df, ["naturezaOperacao.id", "naturezaOperacao", "natureza_operacao_id"])
        if natureza_col:
            df["natureza"] = df[natureza_col].astype(str)
        vendor_id_col = _pick_first_column(df, ["vendedor_id", "vendedor.id", "vendedorResponsavel.id", "responsavel.id", "representante.id"])
        if vendor_id_col:
            df["vendedor_id"] = df[vendor_id_col].astype(str)
        vendor_name_col = _pick_first_column(df, ["vendedor", "vendedor.nome", "vendedorResponsavel.nome", "responsavel.nome", "representante.nome"])
        if vendor_name_col:
            df["vendedor"] = df[vendor_name_col].astype(str)
        df = _apply_vendor_map(df)
        df = _append_nature_labels(df)
        df["origem"] = "bling_nfe"
        df = _map_vendedor_from_local_history(df)
        keep_cols = [
            c
            for c in ["data", "receita", "cliente", "vendedor", "vendedor_id", "origem", "empresa", "numero_documento", "natureza", "natureza_label"]
            if c in df.columns
        ]
        df = df[keep_cols].copy()
        return df.dropna(subset=["data", "receita"])

    caches: list[Path] = []
    if BLING_VENDAS.exists():
        caches.append(BLING_VENDAS)
    elif BLING_VENDAS_FALLBACK.exists():
        caches.append(BLING_VENDAS_FALLBACK)
    if BLING_VENDAS_CR.exists():
        caches.append(BLING_VENDAS_CR)
    elif BLING_VENDAS_CR_FALLBACK.exists():
        caches.append(BLING_VENDAS_CR_FALLBACK)

    if not caches:
        return pd.DataFrame()
    rows = []
    import json
    for cache in caches:
        with cache.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = pd.json_normalize(json.loads(line))
                # If source file has no explicit company flag, infer from filename.
                if "empresa" not in obj.columns:
                    obj["empresa"] = "CR" if cache.name.endswith("_cr.jsonl") else "CZ"
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

    if "vendedor_id" in df.columns:
        try:
            df = _apply_vendor_map(df)
        except Exception:
            # Keep app running even if mapping file is malformed.
            pass
    if "vendedor" not in df.columns:
        df["vendedor"] = pd.NA

    df = _map_vendedor_from_local_history(df)
    keep_cols = [c for c in ["data", "receita", "cliente", "vendedor", "vendedor_id", "origem", "empresa"] if c in df.columns]
    df = df[keep_cols].copy()
    df = df.dropna(subset=["data", "receita"])
    return df


@st.cache_data(show_spinner=False)
def load_bling_nfe(year: int) -> pd.DataFrame:
    df = _load_bling_nfe_rows([year])
    if df.empty:
        df = _load_remote_bling_nfe_rows([year])
    if df.empty:
        return pd.DataFrame()
    if "dataEmissao" in df.columns:
        df["data"] = pd.to_datetime(df["dataEmissao"], errors="coerce")
    elif "dataOperacao" in df.columns:
        df["data"] = pd.to_datetime(df["dataOperacao"], errors="coerce")
    elif "data" not in df.columns:
        df["data"] = pd.NaT
    if "valorNota" in df.columns:
        df["valor"] = pd.to_numeric(df["valorNota"], errors="coerce")
    elif "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    elif "valor" not in df.columns:
        df["valor"] = pd.NA
    customer_col = _pick_first_column(
        df,
        ["contato.nome", "cliente.nome", "contato", "cliente", "destinatario.nome", "nomeContato"],
    )
    if customer_col:
        df["cliente"] = df[customer_col].astype(str)
    doc_col = _pick_first_column(
        df,
        ["contato.numeroDocumento", "cliente.numeroDocumento", "numeroDocumento", "destinatario.numeroDocumento"],
    )
    if doc_col:
        df["numero_documento"] = df[doc_col].astype(str)
    natureza_col = _pick_first_column(df, ["naturezaOperacao.id", "naturezaOperacao", "natureza_operacao_id"])
    if natureza_col:
        df["natureza"] = df[natureza_col].astype(str)
    vendor_id_col = _pick_first_column(
        df,
        ["vendedor_id", "vendedor.id", "vendedorResponsavel.id", "responsavel.id", "representante.id"],
    )
    if vendor_id_col:
        df["vendedor_id"] = df[vendor_id_col].fillna("").astype(str).str.strip()
    vendor_name_col = _pick_first_column(
        df,
        ["vendedor", "vendedor.nome", "vendedorResponsavel.nome", "responsavel.nome", "representante.nome"],
    )
    if vendor_name_col:
        df["vendedor"] = df[vendor_name_col].fillna("").astype(str).str.strip()
    if "vendedor" not in df.columns:
        df["vendedor"] = pd.NA
    if "vendedor_id" in df.columns:
        df = _apply_vendor_map(df)
    df = _map_vendedor_from_local_history(df)
    df = _append_nature_labels(df)
    keep_cols = [
        c
        for c in [
            "data",
            "valor",
            "cliente",
            "vendedor",
            "vendedor_id",
            "empresa",
            "numero_documento",
            "natureza",
            "natureza_label",
        ]
        if c in df.columns
    ]
    return df[keep_cols].dropna(subset=["data", "valor"])


def _load_bling_nfe_detail_years(years: tuple[int, ...]) -> pd.DataFrame:
    years = tuple(sorted({int(year) for year in years if str(year).strip()}))
    if not years:
        years = (2026, 2025)
    df = _load_bling_nfe_rows(list(years))
    if df.empty:
        df = _load_remote_bling_nfe_rows(list(years))
    if df.empty:
        return pd.DataFrame()

    if "dataEmissao" in df.columns:
        df["data"] = pd.to_datetime(df["dataEmissao"], errors="coerce")
    elif "dataOperacao" in df.columns:
        df["data"] = pd.to_datetime(df["dataOperacao"], errors="coerce")
    else:
        df["data"] = pd.NaT

    natureza_col = _pick_first_column(df, ["naturezaOperacao.id", "naturezaOperacao", "natureza_operacao_id"])
    if natureza_col:
        df["natureza"] = df[natureza_col].astype(str)
    else:
        df["natureza"] = ""

    customer_col = _pick_first_column(
        df,
        ["contato.nome", "cliente.nome", "contato", "cliente", "destinatario.nome", "nomeContato"],
    )
    if customer_col:
        df["cliente"] = df[customer_col].astype(str)
    else:
        df["cliente"] = "SEM_CLIENTE"

    doc_col = _pick_first_column(
        df,
        ["contato.numeroDocumento", "cliente.numeroDocumento", "numeroDocumento", "destinatario.numeroDocumento"],
    )
    if doc_col:
        df["numero_documento"] = df[doc_col].astype(str)
    else:
        df["numero_documento"] = ""

    vendor_id_col = _pick_first_column(
        df,
        ["vendedor_id", "vendedor.id", "vendedorResponsavel.id", "responsavel.id", "representante.id"],
    )
    if vendor_id_col:
        df["vendedor_id"] = df[vendor_id_col].fillna("").astype(str).str.strip()
    else:
        df["vendedor_id"] = ""

    vendor_name_col = _pick_first_column(
        df,
        ["vendedor", "vendedor.nome", "vendedorResponsavel.nome", "responsavel.nome", "representante.nome"],
    )
    if vendor_name_col:
        df["vendedor"] = df[vendor_name_col].fillna("").astype(str).str.strip()
    else:
        df["vendedor"] = ""

    if "vendedor_id" in df.columns:
        df = _apply_vendor_map(df)
    df = _map_vendedor_from_local_history(df)
    df = _append_nature_labels(df)

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        base = {
            "data": row.get("data"),
            "empresa": row.get("empresa", ""),
            "nfe_id": row.get("id", ""),
            "numero_nf": row.get("numero", ""),
            "serie": row.get("serie", ""),
            "chave_acesso": row.get("chaveAcesso", ""),
            "cliente": row.get("cliente", "SEM_CLIENTE"),
            "numero_documento": row.get("numero_documento", ""),
            "vendedor": row.get("vendedor", "SEM_VENDEDOR"),
            "vendedor_id": row.get("vendedor_id", ""),
            "natureza": row.get("natureza", ""),
            "natureza_label": row.get("natureza_label", ""),
            "valor_nf": pd.to_numeric(row.get("valorNota"), errors="coerce"),
        }
        itens = row.get("itens")
        if not isinstance(itens, list) or not itens:
            rows.append(
                {
                    **base,
                    "produto": "SEM_ITEM_DETALHADO",
                    "produto_codigo": "",
                    "quantidade": 0.0,
                    "valor_unitario": pd.NA,
                    "valor_total": pd.to_numeric(row.get("valorNota"), errors="coerce"),
                    "cfop": "",
                    "lote": pd.NA,
                    "vencimento_lote": pd.NaT,
                }
            )
            continue

        for item in itens:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    **base,
                    "produto": item.get("descricao") or item.get("nome") or "N/D",
                    "produto_codigo": item.get("codigo") or "",
                    "quantidade": pd.to_numeric(item.get("quantidade"), errors="coerce"),
                    "valor_unitario": pd.to_numeric(item.get("valor"), errors="coerce"),
                    "valor_total": pd.to_numeric(item.get("valorTotal"), errors="coerce"),
                    "cfop": str(item.get("cfop") or "").strip(),
                    "lote": item.get("lote"),
                    "vencimento_lote": pd.to_datetime(item.get("vencimento") or item.get("validade"), errors="coerce"),
                }
            )

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out["data"] = pd.to_datetime(out["data"], errors="coerce")
    out["quantidade"] = pd.to_numeric(out["quantidade"], errors="coerce").fillna(0)
    out["valor_unitario"] = pd.to_numeric(out["valor_unitario"], errors="coerce")
    out["valor_total"] = pd.to_numeric(out["valor_total"], errors="coerce")
    out["valor_nf"] = pd.to_numeric(out["valor_nf"], errors="coerce")
    out["produto"] = out["produto"].fillna("N/D").astype(str)
    out["produto_codigo"] = out["produto_codigo"].fillna("").astype(str)
    out["cliente"] = out["cliente"].fillna("SEM_CLIENTE").astype(str)
    out["vendedor"] = out["vendedor"].fillna("SEM_VENDEDOR").astype(str)
    out["vendedor_id"] = out["vendedor_id"].fillna("").astype(str)
    out["natureza"] = out["natureza"].fillna("").astype(str)
    out["natureza_label"] = out["natureza_label"].fillna("").astype(str)
    out["cfop"] = out["cfop"].fillna("").astype(str)
    out["numero_nf"] = out["numero_nf"].fillna("").astype(str)
    out["dias_em_aberto"] = (pd.Timestamp.today().normalize() - out["data"].dt.normalize()).dt.days
    out["month_start"] = out["data"].dt.to_period("M").dt.to_timestamp()
    return out.dropna(subset=["data"])


@st.cache_data(show_spinner=False)
def load_bling_nfe_detail(year: int = 2026) -> pd.DataFrame:
    years = (year,) if year in {2025, 2026} else (2026, 2025)
    return _load_bling_nfe_detail_years(years)


@st.cache_data(show_spinner=False)
def load_bling_nfe_detail_years(years: tuple[int, ...] | None = None) -> pd.DataFrame:
    years = years or (2026, 2025)
    return _load_bling_nfe_detail_years(years)


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


def _pick_first_value(payload: dict[str, Any], options: list[str]) -> Any:
    for key in options:
        if key in payload and payload.get(key) not in (None, ""):
            return payload.get(key)
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
def load_bling_produtos() -> pd.DataFrame:
    caches = [path for path in [BLING_PRODUTOS, BLING_PRODUTOS_CR] if path.exists()]
    if not caches:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for cache in caches:
        df = _load_jsonl(cache)
        if df.empty:
            continue
        df = _normalize_columns(df)
        if "empresa" not in df.columns:
            df["empresa"] = "CR" if cache.name.endswith("_cr.jsonl") else "CZ"
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    id_col = _pick_first_column(df, ["id", "produto.id"])
    code_col = _pick_first_column(df, ["codigo", "produto.codigo", "sku"])
    name_col = _pick_first_column(df, ["nome", "descricao", "produto.nome", "produto.descricao"])
    type_col = _pick_first_column(df, ["tipo", "produto.tipo", "categoria", "produto.categoria"])

    out = pd.DataFrame()
    out["produto_id"] = df[id_col].astype(str) if id_col else ""
    out["produto_codigo"] = df[code_col].astype(str) if code_col else ""
    out["produto"] = df[name_col].astype(str) if name_col else "N/D"
    out["tipo_produto"] = df[type_col].astype(str) if type_col else "N/D"
    out["empresa"] = df["empresa"].astype(str) if "empresa" in df.columns else "N/D"
    out["produto_key"] = out["produto"].map(_normalize_text)
    out["produto_codigo"] = out["produto_codigo"].fillna("").astype(str)
    out["produto_id"] = out["produto_id"].fillna("").astype(str)
    out["tipo_produto"] = out["tipo_produto"].replace({"": "N/D"}).fillna("N/D")
    out = out.drop_duplicates(subset=["empresa", "produto_id", "produto_codigo", "produto_key"])
    return out


@st.cache_data(show_spinner=False)
def load_bling_sales_detail(year: int = 2026) -> pd.DataFrame:
    if year == 2026:
        candidates = [BLING_VENDAS, BLING_VENDAS_CR]
    elif year == 2025:
        candidates = [BLING_VENDAS_FALLBACK, BLING_VENDAS_CR_FALLBACK]
    else:
        candidates = [BLING_VENDAS, BLING_VENDAS_CR, BLING_VENDAS_FALLBACK, BLING_VENDAS_CR_FALLBACK]

    caches = [path for path in candidates if path.exists()]
    if not caches:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    import json

    for cache in caches:
        company = "CR" if cache.name.endswith("_cr.jsonl") else "CZ"
        with cache.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    order = json.loads(line)
                except Exception:
                    continue

                order_date = (
                    _pick_first_value(order, ["data", "dataEmissao", "dataOperacao", "dataSaida"])
                )
                seller = _pick_first_value(
                    order,
                    [
                        "vendedor",
                        "vendedor.nome",
                        "vendedorResponsavel.nome",
                        "responsavel.nome",
                        "representante.nome",
                    ],
                )
                seller_id = _pick_first_value(
                    order,
                    [
                        "vendedor_id",
                        "vendedor.id",
                        "vendedorResponsavel.id",
                        "responsavel.id",
                        "representante.id",
                    ],
                )
                customer = _pick_first_value(
                    order,
                    ["contato.nome", "cliente.nome", "contato", "cliente", "nomeContato"],
                )
                order_total = _pick_first_value(order, ["total", "valorTotal", "valor", "totalProdutos"])
                natureza = _pick_first_value(order, ["naturezaOperacao.id", "naturezaOperacao", "natureza_operacao_id"])
                items = order.get("itens")
                if not isinstance(items, list):
                    items = []
                cfop_order = _first_cfop_from_items(items)

                if not items:
                    rows.append(
                        {
                            "data": order_date,
                            "empresa": order.get("empresa", company),
                            "pedido_id": order.get("id"),
                            "cliente": customer,
                            "vendedor": seller,
                            "vendedor_id": seller_id,
                            "natureza": natureza,
                            "cfop": cfop_order,
                            "produto_id": "",
                            "produto_codigo": "",
                            "produto": "SEM_ITEM_DETALHADO",
                            "tipo_produto": "N/D",
                            "quantidade": 0,
                            "valor_unitario": None,
                            "valor_total": order_total,
                        }
                    )
                    continue

                for item in items:
                    if not isinstance(item, dict):
                        continue
                    product = item.get("produto") if isinstance(item.get("produto"), dict) else {}
                    produto_id = _pick_first_value(item, ["produto.id", "idProduto", "produtoId"]) or product.get("id")
                    produto_codigo = _pick_first_value(
                        item,
                        ["produto.codigo", "codigo", "sku", "item.codigo"],
                    ) or product.get("codigo")
                    produto_nome = _pick_first_value(
                        item,
                        ["produto.nome", "descricao", "nome", "descricaoDetalhada", "item.descricao"],
                    ) or product.get("nome") or product.get("descricao")
                    tipo_produto = _pick_first_value(
                        item,
                        ["produto.tipo", "tipoProduto", "tipo", "categoria"],
                    ) or product.get("tipo")
                    natureza_item = _pick_first_value(item, ["naturezaOperacao.id", "naturezaOperacao"]) or natureza
                    cfop_item = _pick_first_value(item, ["cfop"]) or cfop_order
                    quantidade = _pick_first_value(item, ["quantidade", "qtde", "qtd"])
                    valor_unitario = _pick_first_value(
                        item,
                        ["valor", "valorUnitario", "preco", "valorUnidade"],
                    )
                    valor_total = _pick_first_value(item, ["total", "valorTotal", "subtotal"])

                    rows.append(
                        {
                            "data": order_date,
                            "empresa": order.get("empresa", company),
                            "pedido_id": order.get("id"),
                            "cliente": customer,
                            "vendedor": seller,
                            "vendedor_id": seller_id,
                            "natureza": natureza_item,
                            "cfop": cfop_item,
                            "produto_id": produto_id,
                            "produto_codigo": produto_codigo,
                            "produto": produto_nome,
                            "tipo_produto": tipo_produto,
                            "quantidade": quantidade,
                            "valor_unitario": valor_unitario,
                            "valor_total": valor_total if valor_total not in (None, "") else order_total,
                        }
                    )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0)
    df["valor_unitario"] = pd.to_numeric(df["valor_unitario"], errors="coerce")
    df["valor_total"] = pd.to_numeric(df["valor_total"], errors="coerce")
    calc_total = df["quantidade"].fillna(0) * df["valor_unitario"].fillna(0)
    df["valor_total"] = df["valor_total"].fillna(calc_total)
    if "cliente" in df.columns:
        df["cliente"] = df["cliente"].apply(
            lambda value: value.get("nome", "") if isinstance(value, dict) else value
        )
        df["cliente"] = df["cliente"].fillna("").astype(str).str.strip().replace("", "SEM_CLIENTE")
    if "vendedor_id" in df.columns:
        df["vendedor_id"] = df["vendedor_id"].fillna("").astype(str).str.strip()
    if "vendedor_id" in df.columns:
        vendedor_id_txt = df["vendedor_id"].fillna("").astype(str).str.strip()
        vendedor_txt = df["vendedor"].fillna("").astype(str).str.strip()
        df["vendedor"] = vendedor_txt.mask(vendedor_txt.eq(""), vendedor_id_txt)
    df["vendedor"] = df["vendedor"].replace("", "SEM_VENDEDOR").fillna("SEM_VENDEDOR").astype(str)
    if "natureza" in df.columns:
        df["natureza"] = df["natureza"].fillna("").astype(str).str.strip()
    if "cfop" in df.columns:
        df["cfop"] = df["cfop"].fillna("").astype(str).str.strip()
    df["produto"] = df["produto"].fillna("N/D").astype(str)
    df["tipo_produto"] = df["tipo_produto"].fillna("").astype(str)
    df["produto_id"] = df["produto_id"].fillna("").astype(str)
    df["produto_codigo"] = df["produto_codigo"].fillna("").astype(str)
    df["produto_key"] = df["produto"].map(_normalize_text)
    df["month_start"] = df["data"].dt.to_period("M").dt.to_timestamp()
    df = _append_nature_labels(df)

    produtos = load_bling_produtos()
    if not produtos.empty:
        merged = df.merge(
            produtos[["empresa", "produto_id", "produto_codigo", "produto_key", "tipo_produto"]].rename(
                columns={"tipo_produto": "tipo_produto_catalogo"}
            ),
            on=["empresa", "produto_id", "produto_codigo", "produto_key"],
            how="left",
        )
        fallback = merged["tipo_produto"].replace({"": pd.NA, "N/D": pd.NA})
        merged["tipo_produto"] = fallback.fillna(merged["tipo_produto_catalogo"]).fillna("N/D")
        df = merged.drop(columns=["tipo_produto_catalogo"], errors="ignore")
    else:
        df["tipo_produto"] = df["tipo_produto"].replace({"": "N/D"}).fillna("N/D")

    df = df.dropna(subset=["data", "valor_total"])
    return df[
        [
            "data",
            "month_start",
            "empresa",
            "pedido_id",
            "cliente",
            "vendedor",
            "vendedor_id",
            "natureza",
            "natureza_label",
            "cfop",
            "produto_id",
            "produto_codigo",
            "produto",
            "tipo_produto",
            "quantidade",
            "valor_unitario",
            "valor_total",
        ]
    ].copy()


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
