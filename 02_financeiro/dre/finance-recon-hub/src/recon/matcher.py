from __future__ import annotations

from difflib import SequenceMatcher

import pandas as pd


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def reconcile(
    bank_txns: pd.DataFrame,
    bling_titles: pd.DataFrame,
    date_window_days: int = 2,
    amount_tolerance: float = 1.0,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if bank_txns.empty:
        return pd.DataFrame(), pd.DataFrame(), bling_titles.copy()
    if bling_titles.empty:
        tx = bank_txns.copy()
        tx["match_status"] = "NO_BLING_TITLE"
        return tx, pd.DataFrame(), pd.DataFrame()

    tx = bank_txns.copy().reset_index(drop=True)
    titles = bling_titles.copy().reset_index(drop=True)
    title_key_col = "external_id" if "external_id" in titles.columns else "id"
    tx["match_status"] = "UNMATCHED"
    tx["match_id"] = None
    tx["match_external_id"] = None
    tx["match_company_code"] = None
    tx["match_score"] = 0.0
    tx["matched_title_type"] = None
    tx["matched_counterparty"] = None

    used_titles = set()
    matches = []

    for i, row in tx.iterrows():
        target_type = "RECEBER" if row["direction"] == "CREDIT" else "PAGAR"
        cand = titles[titles["type"] == target_type].copy()
        if cand.empty:
            continue

        cand = cand[(cand["amount_abs"] - row["amount_abs"]).abs() <= amount_tolerance]
        if cand.empty:
            continue

        d0 = pd.to_datetime(row["date"], errors="coerce")
        if pd.notna(d0) and "due_date" in cand.columns:
            due = pd.to_datetime(cand["due_date"], errors="coerce")
            day_delta = (due - d0).dt.days.abs()
            cand = cand[day_delta <= date_window_days]
        if cand.empty:
            continue

        best_id = None
        best_external_id = None
        best_company_code = ""
        best_score = -1.0
        best_counterparty = ""
        for _, c in cand.iterrows():
            cid = str(c.get(title_key_col) or c.get("id") or "")
            if not cid:
                continue
            if cid in used_titles:
                continue
            s_name = _similarity(str(row.get("description", "")), str(c.get("counterparty", "")))
            s_amt = 1.0 - min(1.0, abs(float(row["amount_abs"]) - float(c["amount_abs"])) / max(1.0, float(row["amount_abs"])))
            score = 0.65 * s_amt + 0.35 * s_name
            if score > best_score:
                best_score = score
                best_id = str(c.get("id") or "")
                best_external_id = str(c.get("external_id") or cid)
                best_company_code = str(c.get("company_code") or "")
                best_counterparty = str(c.get("counterparty", ""))

        if best_external_id is None:
            continue

        used_titles.add(best_external_id)
        tx.loc[i, "match_status"] = "MATCHED"
        tx.loc[i, "match_id"] = best_id
        tx.loc[i, "match_external_id"] = best_external_id
        tx.loc[i, "match_company_code"] = best_company_code
        tx.loc[i, "match_score"] = round(best_score, 4)
        tx.loc[i, "matched_title_type"] = target_type
        tx.loc[i, "matched_counterparty"] = best_counterparty
        matches.append(
            {
                "txn_index": i,
                "txn_date": row["date"],
                "txn_desc": row["description"],
                "txn_amount": row["amount"],
                "title_id": best_id,
                "title_external_id": best_external_id,
                "title_type": target_type,
                "company_code": best_company_code,
                "counterparty": best_counterparty,
                "score": round(best_score, 4),
            }
        )

    matched_ids = set(tx["match_external_id"].dropna().astype(str).tolist())
    pending_bling = titles[~titles[title_key_col].astype(str).isin(matched_ids)].copy()
    return tx, pd.DataFrame(matches), pending_bling


def classify_unmatched_root_cause(
    txns: pd.DataFrame,
    bling_titles: pd.DataFrame,
    date_window_days: int = 2,
    amount_tolerance: float = 1.0,
) -> pd.DataFrame:
    if txns.empty:
        return txns
    tx = txns.copy()
    if "root_cause" not in tx.columns:
        tx["root_cause"] = ""
    if bling_titles.empty:
        tx.loc[tx["match_status"] != "MATCHED", "root_cause"] = "NO_BLING_TITLES"
        return tx

    for i, row in tx.iterrows():
        if row.get("match_status") == "MATCHED":
            tx.loc[i, "root_cause"] = "MATCHED"
            continue

        target_type = "RECEBER" if row["direction"] == "CREDIT" else "PAGAR"
        cand_type = bling_titles[bling_titles["type"] == target_type]
        if cand_type.empty:
            tx.loc[i, "root_cause"] = "NO_TITLE_BY_DIRECTION"
            continue

        cand_amount = cand_type[(cand_type["amount_abs"] - row["amount_abs"]).abs() <= amount_tolerance]
        if cand_amount.empty:
            tx.loc[i, "root_cause"] = "AMOUNT_MISMATCH"
            continue

        d0 = pd.to_datetime(row["date"], errors="coerce")
        due = pd.to_datetime(cand_amount["due_date"], errors="coerce")
        day_delta = (due - d0).dt.days.abs()
        cand_date = cand_amount[day_delta <= date_window_days]
        if cand_date.empty:
            tx.loc[i, "root_cause"] = "DATE_OUT_OF_WINDOW"
            continue

        tx.loc[i, "root_cause"] = "DESCRIPTION_DIVERGENCE"

    return tx
