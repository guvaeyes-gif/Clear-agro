from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


def _get_env(key: str) -> str:
    val = (os.getenv(key) or "").strip()
    if val:
        return val

    # Fallback para .env na raiz do projeto (execucoes agendadas no Windows).
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return ""
    try:
        for ln in env_path.read_text(encoding="utf-8").splitlines():
            ln = ln.strip()
            if not ln or ln.startswith("#") or "=" not in ln:
                continue
            k, v = ln.split("=", 1)
            if k.strip() == key:
                return v.strip().strip('"').strip("'")
    except Exception:
        return ""
    return ""


def write_outputs(
    output_dir: Path,
    txns: pd.DataFrame,
    matches: pd.DataFrame,
    pending_bling: pd.DataFrame,
    run_id: str | None = None,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    xlsx = output_dir / f"conciliacao_{ts}.xlsx"
    csv_match = output_dir / f"conciliacao_matches_{ts}.csv"
    csv_unmatched = output_dir / f"conciliacao_nao_conciliado_{ts}.csv"
    csv_pending_bling = output_dir / f"conciliacao_pendente_bling_{ts}.csv"

    with pd.ExcelWriter(xlsx, engine="openpyxl") as writer:
        txns.to_excel(writer, sheet_name="Transacoes_Banco", index=False)
        matches.to_excel(writer, sheet_name="Matches", index=False)
        pending_bling.to_excel(writer, sheet_name="Pendente_Bling", index=False)
        summary = pd.DataFrame(
            [
                {"metric": "tx_total", "value": len(txns)},
                {"metric": "tx_matched", "value": int((txns["match_status"] == "MATCHED").sum()) if not txns.empty else 0},
                {"metric": "tx_unmatched", "value": int((txns["match_status"] != "MATCHED").sum()) if not txns.empty else 0},
                {"metric": "pending_bling", "value": len(pending_bling)},
                {"metric": "amount_matched", "value": float(txns.loc[txns["match_status"] == "MATCHED", "amount_abs"].sum()) if not txns.empty else 0.0},
            ]
        )
        summary.to_excel(writer, sheet_name="Resumo", index=False)
        if "root_cause" in txns.columns:
            rc = (
                txns[txns["match_status"] != "MATCHED"]["root_cause"]
                .fillna("UNCLASSIFIED")
                .value_counts()
                .reset_index()
            )
            rc.columns = ["root_cause", "count"]
            rc.to_excel(writer, sheet_name="Causas_Nao_Conciliado", index=False)

    txns.to_csv(csv_unmatched, index=False, encoding="utf-8-sig")
    matches.to_csv(csv_match, index=False, encoding="utf-8-sig")
    pending_bling.to_csv(csv_pending_bling, index=False, encoding="utf-8-sig")

    return {
        "xlsx": str(xlsx),
        "csv_matches": str(csv_match),
        "csv_unmatched": str(csv_unmatched),
        "csv_pending_bling": str(csv_pending_bling),
    }


def send_telegram_summary(txns: pd.DataFrame, matches: pd.DataFrame, pending_bling: pd.DataFrame) -> tuple[bool, str]:
    token = _get_env("TELEGRAM_BOT_TOKEN")
    chat_id = _get_env("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False, "TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID nao configurados"

    total = len(txns)
    matched = int((txns["match_status"] == "MATCHED").sum()) if not txns.empty else 0
    unmatched = total - matched
    pct = (matched / total * 100) if total else 0
    msg = (
        "Finance Recon Hub - Resumo\n"
        f"- Transacoes banco: {total}\n"
        f"- Conciliadas: {matched} ({pct:.1f}%)\n"
        f"- Nao conciliadas: {unmatched}\n"
        f"- Pendentes no Bling: {len(pending_bling)}"
    )

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=20)
    if resp.status_code != 200:
        return False, f"Telegram erro {resp.status_code}: {resp.text[:160]}"
    data = resp.json()
    if not data.get("ok"):
        return False, str(data.get("description", "falha"))
    return True, "Telegram enviado"


def append_jsonl_log(log_file: Path, payload: dict) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as f:
        row = {"ts_utc": pd.Timestamp.utcnow().isoformat(), **payload}
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
