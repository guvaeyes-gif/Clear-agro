from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from client import BlingClient


ROOT = Path(__file__).resolve().parent
ACCOUNT_ALIASES = {"cz": "CZ", "cr": "CR"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Atualiza cache de NF-e do Bling a partir da ultima data local.")
    parser.add_argument("--company", default="CZ", choices=["CZ", "CR", "cz", "cr"])
    parser.add_argument("--year", type=int, default=date.today().year)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--lookback-days", type=int, default=1)
    return parser.parse_args()


def company_tag(company: str) -> str:
    return ACCOUNT_ALIASES[(company or "").strip().lower()]


def cache_path(year: int, company: str) -> Path:
    tag = company_tag(company).lower()
    base = f"nfe_{year}_cache.jsonl"
    if tag == "cz":
        return ROOT / base
    stem, ext = base.rsplit(".", 1)
    return ROOT / f"{stem}_{tag}.{ext}"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def parse_issue_date(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw or raw.startswith("0000-00-00"):
        return None
    token = raw.split()[0]
    try:
        return datetime.strptime(token, "%Y-%m-%d").date()
    except ValueError:
        return None


def fetch_details(
    client: BlingClient,
    start_date: date,
    end_date: date,
    limit: int,
) -> list[dict[str, Any]]:
    page = 1
    rows: list[dict[str, Any]] = []
    while True:
        payload = client.get_data(
            "/nfe",
            params={
                "pagina": page,
                "limite": limit,
                "dataEmissaoInicial": start_date.strftime("%Y-%m-%d"),
                "dataEmissaoFinal": end_date.strftime("%Y-%m-%d"),
            },
        )
        if not payload:
            break
        for item in payload:
            rid = item.get("id")
            if rid in (None, ""):
                continue
            detail = client.get_detail(f"/nfe/{rid}")
            if isinstance(detail, dict) and detail:
                rows.append(detail)
        page += 1
    return rows


def merge_rows(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in existing:
        rid = row.get("id")
        if rid in (None, ""):
            continue
        merged[str(rid)] = row
    for row in incoming:
        rid = row.get("id")
        if rid in (None, ""):
            continue
        merged[str(rid)] = row
    return sorted(
        merged.values(),
        key=lambda row: (
            str(row.get("dataEmissao") or row.get("dataOperacao") or ""),
            str(row.get("numero") or ""),
            str(row.get("id") or ""),
        ),
    )


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    args = parse_args()
    company = company_tag(args.company)
    cache = cache_path(args.year, company)
    existing = read_jsonl(cache)

    last_dates = [dt for dt in (parse_issue_date(row.get("dataEmissao")) for row in existing) if dt]
    default_start = date(args.year, 1, 1)
    if last_dates:
        start_date = max(default_start, max(last_dates) - timedelta(days=max(args.lookback_days, 0)))
    else:
        start_date = default_start
    end_date = date.today()

    client = BlingClient(company.lower())
    fetched = fetch_details(client, start_date, end_date, args.limit)
    merged = merge_rows(existing, fetched)
    write_jsonl(cache, merged)

    print(
        json.dumps(
            {
                "company": company,
                "cache": str(cache),
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "existing_rows": len(existing),
                "fetched_rows": len(fetched),
                "merged_rows": len(merged),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
