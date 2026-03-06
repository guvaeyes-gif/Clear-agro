from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

BASE = ROOT / "out" / "base_unificada.xlsx"

from src.telegram import build_alerts_message, send_telegram_message, telegram_enabled


def main() -> int:
    if not telegram_enabled():
        print("Telegram nao configurado. Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID.")
        return 1

    if not BASE.exists():
        print("Base nao encontrada em ./out/base_unificada.xlsx")
        return 1

    xls = pd.ExcelFile(BASE)
    opps = pd.read_excel(BASE, sheet_name="oportunidades") if "oportunidades" in xls.sheet_names else pd.DataFrame()
    if "data_proximo_passo" in opps.columns:
        sem_passo = int(opps["data_proximo_passo"].isna().sum())
    else:
        sem_passo = "pendente"

    alerts = [("Sem proximo passo", sem_passo)]
    period = pd.Timestamp.now().strftime("%d/%m/%Y %H:%M")
    text = build_alerts_message("McKinsey Agro CRM - Insights", period, alerts)
    ok, detail = send_telegram_message(text)
    print(detail)
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
