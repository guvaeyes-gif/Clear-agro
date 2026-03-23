from __future__ import annotations

import argparse
import traceback
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.append(str(SRC))

from recon.config import ReconConfig
from recon.bank_parsers import load_bank_transactions, iter_supported_files
from recon.bling_loader import load_bling_open_titles
from recon.matcher import classify_unmatched_root_cause, reconcile
from recon.report import append_jsonl_log, send_telegram_summary, write_outputs


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Executa conciliacao bancaria com Bling.")
    p.add_argument("--run-id", default=None, help="Identificador da execucao (default timestamp).")
    p.add_argument("--data-stale", action="store_true", help="Marca execucao em modo degradado (pre-sync falhou).")
    p.add_argument("--retry-once", action="store_true", help="Faz 1 retentativa em falha transitória.")
    return p.parse_args()


def _execute_once(cfg: ReconConfig, run_id: str, data_stale: bool) -> int:
    log_file = cfg.output_dir / "run_logs" / "recon_runs.jsonl"
    append_jsonl_log(
        log_file,
        {"run_id": run_id, "stage": "start", "data_stale": data_stale, "bank_inbox": str(cfg.bank_inbox_dir)},
    )

    files = list(iter_supported_files(cfg.bank_inbox_dir))
    if not files:
        print(f"Nenhum arquivo CSV/OFX em {cfg.bank_inbox_dir}")
        append_jsonl_log(log_file, {"run_id": run_id, "stage": "finish", "status": "no_input"})
        return 1

    bank_txns = load_bank_transactions(cfg.bank_inbox_dir)
    if bank_txns.empty:
        print("Arquivos encontrados, mas sem transacoes validas.")
        append_jsonl_log(log_file, {"run_id": run_id, "stage": "finish", "status": "invalid_input"})
        return 1

    bling_titles = load_bling_open_titles(cfg.bling_api_dir)
    tx, matches, pending_bling = reconcile(
        bank_txns=bank_txns,
        bling_titles=bling_titles,
        date_window_days=cfg.date_window_days,
        amount_tolerance=cfg.amount_tolerance,
    )
    tx = classify_unmatched_root_cause(
        txns=tx,
        bling_titles=bling_titles,
        date_window_days=cfg.date_window_days,
        amount_tolerance=cfg.amount_tolerance,
    )

    outputs = write_outputs(cfg.output_dir, tx, matches, pending_bling, run_id=run_id)
    print("Conciliacao concluida.")
    print(f"Transacoes banco: {len(tx)}")
    print(f"Conciliadas: {int((tx['match_status'] == 'MATCHED').sum()) if not tx.empty else 0}")
    print(f"Nao conciliadas: {int((tx['match_status'] != 'MATCHED').sum()) if not tx.empty else 0}")
    print(f"Pendentes Bling: {len(pending_bling)}")
    print("Saidas:")
    for k, v in outputs.items():
        print(f"- {k}: {v}")

    ok, detail = send_telegram_summary(tx, matches, pending_bling)
    print(f"Telegram: {detail}")
    append_jsonl_log(
        log_file,
        {
            "run_id": run_id,
            "stage": "finish",
            "status": "ok",
            "data_stale": data_stale,
            "tx_total": int(len(tx)),
            "tx_matched": int((tx["match_status"] == "MATCHED").sum()) if not tx.empty else 0,
            "tx_unmatched": int((tx["match_status"] != "MATCHED").sum()) if not tx.empty else 0,
            "pending_bling": int(len(pending_bling)),
            "outputs": outputs,
            "telegram_ok": bool(ok),
            "telegram_detail": detail,
        },
    )
    return 0


def main() -> int:
    args = _parse_args()
    cfg = ReconConfig.from_env(ROOT)
    run_id = args.run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    retries = 1 if args.retry_once else 0
    attempt = 0
    while True:
        attempt += 1
        try:
            return _execute_once(cfg=cfg, run_id=run_id, data_stale=args.data_stale)
        except Exception as exc:
            log_file = cfg.output_dir / "run_logs" / "recon_runs.jsonl"
            append_jsonl_log(
                log_file,
                {
                    "run_id": run_id,
                    "stage": "error",
                    "attempt": attempt,
                    "error": str(exc),
                    "traceback": traceback.format_exc(limit=6),
                },
            )
            print(f"Erro na conciliacao (tentativa {attempt}): {exc}")
            if attempt > retries:
                return 2
            print("Retentando uma vez...")


if __name__ == "__main__":
    raise SystemExit(main())
