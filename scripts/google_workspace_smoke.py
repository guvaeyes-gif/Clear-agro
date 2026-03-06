from __future__ import annotations

import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.google_workspace import gmail_profile, list_calendars, list_drive_files, read_sheet_range


def main() -> int:
    print("Smoke test Google Workspace")

    try:
        profile = gmail_profile()
        print(f"Gmail OK | email: {profile.get('emailAddress')} | messagesTotal: {profile.get('messagesTotal')}")
    except Exception as exc:
        print(f"Gmail FAIL: {exc}")

    try:
        calendars = list_calendars()
        print(f"Calendar OK | qtd: {len(calendars)}")
    except Exception as exc:
        print(f"Calendar FAIL: {exc}")

    try:
        files = list_drive_files(page_size=5)
        print(f"Drive OK | qtd listada: {len(files)}")
    except Exception as exc:
        print(f"Drive FAIL: {exc}")

    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SMOKE_SPREADSHEET_ID", "").strip()
    sheet_range = os.getenv("GOOGLE_SHEETS_SMOKE_RANGE", "A1:C5").strip()
    if spreadsheet_id:
        try:
            values = read_sheet_range(spreadsheet_id, sheet_range)
            print(f"Sheets OK | range {sheet_range} | linhas: {len(values)}")
        except Exception as exc:
            print(f"Sheets FAIL: {exc}")
    else:
        print("Sheets SKIP: defina GOOGLE_SHEETS_SMOKE_SPREADSHEET_ID para testar leitura.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
