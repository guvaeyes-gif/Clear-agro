from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.google_workspace import read_sheet_range, write_sheet_range


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ler e atualizar aba especifica no Google Sheets.")
    p.add_argument("--spreadsheet-id", default=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip())
    p.add_argument("--sheet", required=True, help="Nome da aba (ex.: Resumo)")
    p.add_argument("--range", default="A1:C10", help="Range relativo da aba (ex.: A1:C10)")
    p.add_argument(
        "--mode",
        choices=["read", "write"],
        default="read",
        help="read = le dados | write = atualiza range",
    )
    p.add_argument(
        "--set",
        dest="set_values",
        default="",
        help='JSON 2D para escrita, ex.: \'[["Status","OK"],["Data","2026-02-21"]]\'',
    )
    p.add_argument(
        "--confirm-write",
        action="store_true",
        help="Obrigatorio para gravar quando --mode write",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.spreadsheet_id:
        print("Informe --spreadsheet-id ou defina GOOGLE_SHEETS_SPREADSHEET_ID.")
        return 1

    full_range = f"{args.sheet}!{args.range}"

    if args.mode == "read":
        values = read_sheet_range(args.spreadsheet_id, full_range)
        print(f"READ OK | range: {full_range} | linhas: {len(values)}")
        for row in values[:20]:
            print(row)
        return 0

    if not args.confirm_write:
        print("Para gravar, use --confirm-write.")
        return 2
    if not args.set_values:
        print("Para gravar, informe --set com JSON 2D.")
        return 2

    try:
        values = json.loads(args.set_values)
    except Exception as exc:  # nosec B110
        print(f"JSON invalido em --set: {exc}")
        return 2

    if not isinstance(values, list) or (values and not isinstance(values[0], list)):
        print("Formato de --set invalido. Use lista 2D, ex.: [[\"A\",\"B\"],[1,2]].")
        return 2

    result = write_sheet_range(args.spreadsheet_id, full_range, values)
    print(
        "WRITE OK | updatedRange: "
        + str(result.get("updatedRange"))
        + " | updatedRows: "
        + str(result.get("updatedRows"))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
