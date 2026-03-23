from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    from ofxparse import OfxParser
except Exception:  # pragma: no cover
    OfxParser = None


@dataclass
class BankFileResult:
    file_name: str
    bank_name: str
    rows: pd.DataFrame


def _guess_bank_name(file_name: str) -> str:
    low = file_name.lower()
    if "santander" in low:
        return "Santander"
    if "itau" in low:
        return "Itau"
    if "bradesco" in low:
        return "Bradesco"
    if "bb" in low or "banco_do_brasil" in low:
        return "Banco do Brasil"
    if "sicredi" in low:
        return "Sicredi"
    if "sicoob" in low:
        return "Sicoob"
    if "caixa" in low:
        return "Caixa"
    return "Banco_Nao_Identificado"


def _clean_col_name(name: str) -> str:
    return (
        str(name)
        .replace("\ufeff", "")
        .strip()
        .lower()
    )


def _parse_csv(file_path: Path) -> pd.DataFrame:
    # Tentativa resiliente de leitura para diferentes layouts bancarios.
    df = pd.read_csv(file_path, sep=None, engine="python", encoding="utf-8-sig", on_bad_lines="skip")
    cols = {_clean_col_name(c): c for c in df.columns}

    def pick(*names: str) -> str | None:
        for n in names:
            if n in cols:
                return cols[n]
        return None

    date_col = pick("data", "data lançamento", "data lancamento", "lançamento", "lancamento", "date")
    desc_col = pick("historico", "histórico", "descricao", "descrição", "complemento", "memo")
    val_col = pick("valor", "valor (r$)", "amount")
    credit_col = pick("credito", "crédito", "credit")
    debit_col = pick("debito", "débito", "debit")
    id_col = pick("documento", "id", "numero", "número")

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df[date_col], errors="coerce") if date_col else pd.NaT
    out["description"] = df[desc_col].astype(str) if desc_col else ""
    if val_col:
        out["amount"] = pd.to_numeric(df[val_col], errors="coerce")
    elif credit_col or debit_col:
        credit = pd.to_numeric(df[credit_col], errors="coerce").fillna(0) if credit_col else 0
        debit = pd.to_numeric(df[debit_col], errors="coerce").fillna(0) if debit_col else 0
        out["amount"] = credit - debit
    else:
        out["amount"] = 0.0
    out["txn_id"] = df[id_col].astype(str) if id_col else ""
    out["source_format"] = "csv"
    return out.dropna(subset=["date"]).reset_index(drop=True)


def _parse_ofx(file_path: Path) -> pd.DataFrame:
    if OfxParser is None:
        raise RuntimeError("ofxparse nao instalado. Rode pip install -r requirements.txt")
    with file_path.open("rb") as f:
        ofx = OfxParser.parse(f)
    rows = []
    for acct in ofx.accounts:
        for t in acct.statement.transactions:
            rows.append(
                {
                    "date": pd.to_datetime(t.date, errors="coerce"),
                    "description": str(t.memo or t.payee or ""),
                    "amount": float(t.amount or 0),
                    "txn_id": str(t.id or ""),
                    "source_format": "ofx",
                }
            )
    return pd.DataFrame(rows).dropna(subset=["date"]).reset_index(drop=True)


def load_bank_transactions(inbox_dir: Path) -> pd.DataFrame:
    inbox_dir.mkdir(parents=True, exist_ok=True)
    all_rows: list[pd.DataFrame] = []
    for p in sorted(inbox_dir.glob("*")):
        if p.is_dir():
            continue
        bank_name = _guess_bank_name(p.name)
        ext = p.suffix.lower()
        try:
            if ext == ".csv":
                df = _parse_csv(p)
            elif ext == ".ofx":
                df = _parse_ofx(p)
            else:
                continue
        except Exception:
            continue
        if df.empty:
            continue
        df["file_name"] = p.name
        df["bank_name"] = bank_name
        all_rows.append(df)
    if not all_rows:
        return pd.DataFrame(columns=["date", "description", "amount", "txn_id", "source_format", "file_name", "bank_name"])
    out = pd.concat(all_rows, ignore_index=True)
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce").fillna(0)
    out["direction"] = out["amount"].apply(lambda x: "CREDIT" if x >= 0 else "DEBIT")
    out["amount_abs"] = out["amount"].abs()
    return out


def iter_supported_files(inbox_dir: Path) -> Iterable[Path]:
    for p in sorted(inbox_dir.glob("*")):
        if p.suffix.lower() in {".csv", ".ofx"}:
            yield p
