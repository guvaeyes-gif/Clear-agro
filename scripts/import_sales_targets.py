from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.metas_db import (
    build_quarter_rollups_from_monthly,
    import_sales_targets_dataframe,
    prepare_sales_targets_import,
)
from src.google_workspace import read_sheet_range


def read_input_file(path: Path, sheet_name: str | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        xls = pd.ExcelFile(path)
        selected_sheet = sheet_name or ("metas" if "metas" in xls.sheet_names else xls.sheet_names[0])
        return pd.read_excel(path, sheet_name=selected_sheet)
    if suffix in {".csv", ".txt"}:
        return pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    raise ValueError(f"Formato nao suportado: {suffix}")


def read_google_sheet(spreadsheet_id: str, sheet_name: str, range_name: str) -> pd.DataFrame:
    values = read_sheet_range(spreadsheet_id, f"{sheet_name}!{range_name}")
    if not values:
        return pd.DataFrame()
    headers = [str(col).strip() for col in values[0]]
    rows = values[1:]
    return pd.DataFrame(rows, columns=headers)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Importa metas comerciais a partir de CSV/XLSX.")
    parser.add_argument("--input", default="", help="Arquivo CSV/XLSX com as metas.")
    parser.add_argument(
        "--source",
        choices=["file", "google-sheet"],
        default="file",
        help="Fonte dos dados: arquivo local ou Google Sheets compartilhado.",
    )
    parser.add_argument(
        "--spreadsheet-id",
        default=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip(),
        help="ID da planilha compartilhada no Google Sheets.",
    )
    parser.add_argument(
        "--sheet",
        default=os.getenv("GOOGLE_SHEETS_TARGETS_SHEET", "metas").strip(),
        help="Nome da aba da planilha compartilhada.",
    )
    parser.add_argument(
        "--range",
        dest="sheet_range",
        default=os.getenv("GOOGLE_SHEETS_TARGETS_RANGE", "A:O").strip(),
        help="Range lido da aba compartilhada.",
    )
    parser.add_argument(
        "--default-company",
        default="CZ",
        help="Empresa padrao quando a planilha nao traz a coluna empresa.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Valida e mostra o resultado sem gravar.")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Imprime o resumo em JSON para integracao com automacao.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.source == "google-sheet":
        if not args.spreadsheet_id:
            print("Informe --spreadsheet-id ou defina GOOGLE_SHEETS_SPREADSHEET_ID.", file=sys.stderr)
            return 2
        try:
            df = read_google_sheet(args.spreadsheet_id, args.sheet, args.sheet_range)
        except Exception as exc:
            print(f"Falha ao ler Google Sheets: {exc}", file=sys.stderr)
            return 2
        input_label = f"gsheets:{args.spreadsheet_id}:{args.sheet}!{args.sheet_range}"
        input_path = None
    else:
        input_path = Path(args.input).expanduser().resolve()
        if not input_path.exists():
            print(f"Arquivo nao encontrado: {input_path}", file=sys.stderr)
            return 2
        try:
            df = read_input_file(input_path, sheet_name=args.sheet)
        except Exception as exc:
            print(f"Falha ao ler planilha: {exc}", file=sys.stderr)
            return 2
        input_label = str(input_path)

    if args.dry_run:
        valid, invalid, warnings = prepare_sales_targets_import(df, default_empresa=args.default_company)
        quarter_rollups = build_quarter_rollups_from_monthly(valid)
        payload = {
            "input": input_label,
            "valid_rows": int(len(valid)),
            "quarter_rollups": int(len(quarter_rollups)),
            "invalid_rows": int(len(invalid)),
            "warnings": warnings,
            "invalid_preview": invalid.head(20).to_dict(orient="records"),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False))
        else:
            print(f"Fonte: {input_label}")
            print(f"Linhas validas: {payload['valid_rows']}")
            print(f"Fechamentos trimestrais gerados: {payload['quarter_rollups']}")
            print(f"Linhas invalidas: {payload['invalid_rows']}")
            for warning in warnings:
                print(f"AVISO: {warning}")
            if not invalid.empty:
                print("PREVIEW_INVALIDAS:")
                print(invalid.head(20).to_string(index=False))
        return 0

    result = import_sales_targets_dataframe(df, actor_id="cli", default_empresa=args.default_company)
    if args.json:
        print(
            json.dumps(
                {
                    "input": input_label,
                    "created": result.get("created", 0),
                    "updated": result.get("updated", 0),
                    "skipped": result.get("skipped", 0),
                    "quarter_rollups": result.get("rollup_rows", 0),
                    "warnings": result.get("warnings", []),
                    "message": result.get("message", ""),
                },
                ensure_ascii=False,
            )
        )
    else:
        print(result.get("message", "Importacao concluida."))
        for warning in result.get("warnings", []):
            print(f"AVISO: {warning}")
        invalid_rows = result.get("invalid_rows")
        if isinstance(invalid_rows, pd.DataFrame) and not invalid_rows.empty:
            print("PREVIEW_INVALIDAS:")
            print(invalid_rows.head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
