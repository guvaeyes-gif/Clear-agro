from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.google_workspace import write_sheet_range

DEFAULT_SPREADSHEET_ID = "1CAi2XFq0F0AoQ7xJoSzygy3VYgdBkCBVhd6VNoDxzK4"
DEFAULT_SHEET = "Página1"
DEFAULT_RANGE = "E3:G4"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Atualiza KPI diario no relatorio Google Sheets.")
    p.add_argument("--spreadsheet-id", default=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID))
    p.add_argument("--sheet", default=os.getenv("GOOGLE_SHEETS_STATUS_SHEET", DEFAULT_SHEET))
    p.add_argument("--range", dest="range_name", default=os.getenv("GOOGLE_SHEETS_STATUS_RANGE", DEFAULT_RANGE))
    p.add_argument("--kpi", required=True, help="Nome do KPI (ex.: Pipeline ponderado)")
    p.add_argument("--valor", required=True, help="Valor do KPI (ex.: R$ 120.000)")
    p.add_argument("--status", required=True, help="Status (ex.: Em linha)")
    p.add_argument("--confirm-write", action="store_true", help="Obrigatorio para gravar")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.confirm_write:
        print("Para gravar, use --confirm-write.")
        return 2

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    values = [[args.kpi, args.valor, args.status], ["Atualizado em", now, "Clara"]]
    full_range = f"{args.sheet}!{args.range_name}"

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
