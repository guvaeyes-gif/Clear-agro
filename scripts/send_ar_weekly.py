from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from integrations.shared.bling_paths import resolve_bling_file  # noqa: E402
from src.telegram import send_telegram_message, telegram_enabled  # noqa: E402

CACHE = resolve_bling_file("contas_receber_cache.jsonl", mode="pipeline")
CUTOFF = pd.Timestamp.today().normalize()


def _format_brl(value: float) -> str:
    text = f"{value:,.2f}"
    text = text.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {text}"


def _maybe_load_env_from_file() -> None:
    if telegram_enabled():
        return
    candidate = Path.home() / "Documents" / "telegram.txt"
    if not candidate.exists():
        return
    lines = [line.strip() for line in candidate.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return
    token = lines[0]
    chat = ""
    for line in lines[1:]:
        if "id" in line.lower():
            chat = line.replace("Id", "").replace("id", "").replace("=", "").strip()
            break
    if token and chat:
        os.environ["TELEGRAM_BOT_TOKEN"] = token
        os.environ["TELEGRAM_CHAT_ID"] = chat


def _load_open_ar() -> pd.DataFrame:
    rows: list[dict] = []
    with CACHE.open("r", encoding="utf-8") as f:
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

    df = pd.json_normalize(rows)
    df["vencimento"] = pd.to_datetime(df.get("vencimento"), errors="coerce")
    df["situacao"] = pd.to_numeric(df.get("situacao"), errors="coerce")
    df["valor"] = pd.to_numeric(df.get("valor"), errors="coerce").fillna(0)

    open_df = df[df["situacao"].isin([1, 3]) & df["vencimento"].notna()].copy()
    return open_df


def _build_message(open_df: pd.DataFrame) -> str:
    if open_df.empty:
        return "Clear Agro - AR semanal\nSem dados de contas a receber em aberto/parcial."

    cutoff = CUTOFF
    open_df["bucket"] = open_df["vencimento"].apply(lambda d: "vencido" if d <= cutoff else "a_vencer")

    total = float(open_df["valor"].sum())
    vencido = float(open_df.loc[open_df["bucket"] == "vencido", "valor"].sum())
    a_vencer = float(open_df.loc[open_df["bucket"] == "a_vencer", "valor"].sum())

    future = open_df[open_df["bucket"] == "a_vencer"].copy()
    future["mes"] = future["vencimento"].dt.to_period("M").astype(str)
    top_months = (
        future.groupby("mes", as_index=False)["valor"]
        .sum()
        .sort_values("valor", ascending=False)
        .head(3)
    )

    lines = [
        "Clear Agro - AR semanal",
        f"Corte: {cutoff.date()}",
        "",
        f"Aberto+parcial total: {_format_brl(total)}",
        f"Vencido ate hoje: {_format_brl(vencido)}",
        f"A vencer: {_format_brl(a_vencer)}",
        "",
        "Top meses a vencer:",
    ]

    if top_months.empty:
        lines.append("- Sem titulos a vencer")
    else:
        for _, row in top_months.iterrows():
            lines.append(f"- {row['mes']}: {_format_brl(float(row['valor']))}")

    return "\n".join(lines)


def main() -> int:
    if not CACHE.exists():
        print(f"Cache nao encontrado: {CACHE}")
        return 1

    _maybe_load_env_from_file()
    if not telegram_enabled():
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID nao configurados.")
        return 1

    open_df = _load_open_ar()
    message = _build_message(open_df)
    ok, detail = send_telegram_message(message)
    print(detail)
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
