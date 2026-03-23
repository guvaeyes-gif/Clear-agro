from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


def _read_jsonl(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                rows.append(json.loads(ln))
            except Exception:
                continue
    if not rows:
        return pd.DataFrame()
    return pd.json_normalize(rows)


def _configured_company_code() -> str | None:
    raw = (os.getenv("BLING_COMPANY_CODE") or os.getenv("RECON_COMPANY_CODE") or "").strip().upper()
    return raw if raw in {"CZ", "CR"} else None


def _detect_company_code(bling_api_dir: Path) -> str:
    configured = _configured_company_code()
    if configured:
        return configured
    if (bling_api_dir / "contas_pagar_cache.jsonl").exists() or (bling_api_dir / "contas_receber_cache.jsonl").exists():
        return "CZ"
    if (bling_api_dir / "contas_pagar_cache_cr.jsonl").exists() or (bling_api_dir / "contas_receber_cache_cr.jsonl").exists():
        return "CR"
    return "CZ"


def _cache_path(bling_api_dir: Path, stem: str, company_code: str) -> Path:
    suffix = "" if company_code == "CZ" else "_cr"
    preferred = bling_api_dir / f"{stem}{suffix}.jsonl"
    if preferred.exists():
        return preferred
    fallback = bling_api_dir / f"{stem}.jsonl"
    if fallback.exists():
        return fallback
    return preferred


def _pick_col(df: pd.DataFrame, options: list[str]) -> str | None:
    for c in options:
        if c in df.columns:
            return c
    return None


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    txt = str(value).strip()
    return "" if txt.lower() in {"", "nan", "none", "nat"} else txt


def _normalize_id(value: Any) -> str:
    txt = _clean_text(value)
    return txt[:-2] if txt.endswith(".0") else txt


def _contact_lookup(bling_api_dir: Path, company_code: str) -> dict[str, dict[str, str]]:
    df = _read_jsonl(_cache_path(bling_api_dir, "contatos_cache", company_code))
    if df.empty:
        return {}

    lookup: dict[str, dict[str, str]] = {}
    for _, row in df.iterrows():
        cid = _normalize_id(row.get("id"))
        if not cid:
            continue
        lookup[cid] = {
            "nome": _clean_text(row.get("nome")),
            "numero_documento": _clean_text(row.get("numeroDocumento")),
            "situacao": _clean_text(row.get("situacao")),
        }
    return lookup


def _series_or_default(df: pd.DataFrame, column: str | None, default: Any = "") -> pd.Series:
    if column and column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index)


def _open_status(type_name: str, raw_status: str, due_date: pd.Timestamp) -> tuple[str, bool]:
    code = _clean_text(raw_status)
    numeric = None
    try:
        numeric = int(float(code))
    except Exception:
        numeric = None

    if type_name == "PAGAR":
        if numeric == 1:
            if pd.notna(due_date) and due_date.date() < date.today():
                return "overdue", True
            return "open", True
        if numeric == 2:
            return "paid", False
        if numeric == 3:
            return "cancelled", False
    else:
        if numeric == 1:
            if pd.notna(due_date) and due_date.date() < date.today():
                return "overdue", True
            return "open", True
        if numeric == 2:
            return "received", False
        if numeric == 3:
            return "cancelled", False

    if code:
        return f"unknown_{code}", False
    return "unknown", False


def _normalize_titles(
    df: pd.DataFrame,
    *,
    type_name: str,
    company_code: str,
    contacts: dict[str, dict[str, str]],
) -> pd.DataFrame:
    if df.empty:
        return df

    value_col = _pick_col(df, ["valor", "valorDocumento", "valorOriginal", "total"])
    due_col = _pick_col(df, ["dataVencimento", "vencimento", "dataEmissao"])
    issue_col = _pick_col(df, ["dataEmissao", "origem.dataEmissao", "vencimento", "dataVencimento"])
    status_col = _pick_col(df, ["situacao.descricao", "situacao.valor", "situacao", "status"])
    if type_name == "PAGAR":
        name_col = _pick_col(df, ["contato.nome", "fornecedor.nome", "fornecedor", "contato"])
        entity = "ap"
        fallback_prefix = "Fornecedor Pendente"
    else:
        name_col = _pick_col(df, ["contato.nome", "cliente.nome", "cliente", "contato"])
        entity = "ar"
        fallback_prefix = "Cliente Pendente"

    raw_ids = _series_or_default(df, "id").map(_normalize_id)
    counterparty_ids = _series_or_default(df, "contato.id").map(_normalize_id)
    embedded_names = _series_or_default(df, name_col).map(_clean_text)
    lookup_names = counterparty_ids.map(lambda cid: contacts.get(cid, {}).get("nome", "") if cid else "")
    counterparty = embedded_names.where(embedded_names != "", lookup_names)
    fallback_names = counterparty_ids.map(
        lambda cid: f"{fallback_prefix} (contato_{cid})" if cid else ""
    )
    counterparty = counterparty.where(counterparty != "", fallback_names)

    due_dates = pd.to_datetime(_series_or_default(df, due_col, pd.NaT), errors="coerce")
    issue_dates = pd.to_datetime(_series_or_default(df, issue_col, pd.NaT), errors="coerce")
    amounts = pd.to_numeric(_series_or_default(df, value_col, 0), errors="coerce").fillna(0.0)
    raw_status = _series_or_default(df, status_col).map(_clean_text)

    statuses: list[str] = []
    is_open: list[bool] = []
    for status_value, due_date in zip(raw_status.tolist(), due_dates.tolist()):
        mapped_status, mapped_open = _open_status(type_name, status_value, due_date)
        statuses.append(mapped_status)
        is_open.append(mapped_open)

    out = pd.DataFrame(index=df.index)
    out["id"] = raw_ids
    out["external_id"] = raw_ids.map(lambda rid: f"bling:{company_code}:{entity}:{rid}" if rid else "")
    out["external_ref"] = out["external_id"]
    out["company_code"] = company_code
    out["type"] = type_name
    out["due_date"] = due_dates
    out["issue_date"] = issue_dates
    out["amount"] = amounts
    out["amount_abs"] = amounts.abs()
    out["counterparty_id"] = counterparty_ids
    out["counterparty"] = counterparty
    out["status_raw"] = raw_status
    out["status"] = statuses
    out["is_open"] = is_open
    return out


def load_bling_receber(bling_api_dir: Path) -> pd.DataFrame:
    company_code = _detect_company_code(bling_api_dir)
    df = _read_jsonl(_cache_path(bling_api_dir, "contas_receber_cache", company_code))
    if df.empty:
        return df
    contacts = _contact_lookup(bling_api_dir, company_code)
    return _normalize_titles(df, type_name="RECEBER", company_code=company_code, contacts=contacts)


def load_bling_pagar(bling_api_dir: Path) -> pd.DataFrame:
    company_code = _detect_company_code(bling_api_dir)
    df = _read_jsonl(_cache_path(bling_api_dir, "contas_pagar_cache", company_code))
    if df.empty:
        return df
    contacts = _contact_lookup(bling_api_dir, company_code)
    return _normalize_titles(df, type_name="PAGAR", company_code=company_code, contacts=contacts)


def load_bling_open_titles(bling_api_dir: Path) -> pd.DataFrame:
    r = load_bling_receber(bling_api_dir)
    p = load_bling_pagar(bling_api_dir)
    all_df = pd.concat([r, p], ignore_index=True) if (not r.empty or not p.empty) else pd.DataFrame()
    if all_df.empty:
        return all_df
    open_df = all_df[all_df["is_open"]].copy()
    if open_df.empty:
        return open_df
    return open_df.sort_values(["due_date", "type", "amount_abs", "external_id"], na_position="last").reset_index(drop=True)
