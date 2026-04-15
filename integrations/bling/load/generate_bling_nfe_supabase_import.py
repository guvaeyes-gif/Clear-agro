from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from integrations.shared.bling_paths import resolve_bling_root


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[3]
    default_bling = resolve_bling_root("pipeline")
    default_mig = root / "supabase" / "migrations"
    default_out = root / "logs" / "integration" / "status"

    ap = argparse.ArgumentParser(
        description="Generate Bling NF-e -> Supabase import migration for commercial sales."
    )
    ap.add_argument("--bling-dir", default=str(default_bling))
    ap.add_argument("--migrations-dir", default=str(default_mig))
    ap.add_argument("--status-dir", default=str(default_out))
    ap.add_argument("--run-id", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    ap.add_argument("--batch-size", type=int, default=300)
    ap.add_argument("--company", default="CZ", choices=["CZ", "CR", "cz", "cr"])
    return ap.parse_args()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
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


def parse_dt(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw or raw.startswith("0000-00-00"):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw[:19], fmt).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
    return None


def to_decimal(value: Any) -> Decimal:
    if value is None or str(value).strip() == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def sql_quote(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    txt = str(value).replace("'", "''")
    return f"'{txt}'"


def sql_json(value: dict[str, Any]) -> str:
    txt = json.dumps(value, ensure_ascii=False).replace("'", "''")
    return f"'{txt}'::jsonb"


def chunked(items: list[Any], size: int) -> Iterable[list[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def first_cfop(items: Any) -> str | None:
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        cfop = str(item.get("cfop") or "").strip()
        if cfop:
            return cfop
    return None


def normalize_rows(rows: list[dict[str, Any]], company: str) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        bling_id = row.get("id")
        if bling_id in (None, ""):
            continue
        contato = row.get("contato") if isinstance(row.get("contato"), dict) else {}
        vendedor = row.get("vendedor") if isinstance(row.get("vendedor"), dict) else {}
        natureza = row.get("naturezaOperacao") if isinstance(row.get("naturezaOperacao"), dict) else {}
        payload = dict(row)
        payload["empresa"] = str(row.get("empresa") or company).strip().upper()
        normalized.append(
            {
                "company": payload["empresa"],
                "bling_nfe_id": int(bling_id),
                "invoice_number": str(row.get("numero") or "").strip(),
                "issue_datetime": parse_dt(row.get("dataEmissao")),
                "operation_datetime": parse_dt(row.get("dataOperacao")),
                "access_key": str(row.get("chaveAcesso") or "").strip(),
                "series": str(row.get("serie") or "").strip(),
                "customer_bling_id": str(contato.get("id") or "").strip(),
                "customer_name": str(contato.get("nome") or "").strip(),
                "customer_tax_id": str(contato.get("numeroDocumento") or "").strip(),
                "customer_state": str(((contato.get("endereco") or {}).get("uf")) or "").strip(),
                "natureza_id": str(natureza.get("id") or "").strip(),
                "salesperson_bling_id": str(vendedor.get("id") or "").strip(),
                "total_amount": to_decimal(row.get("valorNota")),
                "freight_amount": to_decimal(row.get("valorFrete")),
                "first_cfop": first_cfop(row.get("itens")),
                "payload": payload,
                "external_ref": f"bling_nfe:{payload['empresa']}:{bling_id}",
            }
        )
    return normalized


def build_sql(rows: list[dict[str, Any]], batch_size: int, run_id: str) -> str:
    lines: list[str] = []
    lines.append("-- Clear OS / Supabase")
    lines.append(f"-- Migration: {run_id}_bling_nfe_import_v1.sql")
    lines.append("-- Purpose: incremental Bling NF-e import to raw commercial table")
    lines.append("")
    lines.append("create table if not exists public.bling_nfe_documents (")
    lines.append("  id uuid primary key default gen_random_uuid(),")
    lines.append("  company text not null check (company in ('CZ', 'CR')),")
    lines.append("  bling_nfe_id bigint not null,")
    lines.append("  invoice_number text,")
    lines.append("  issue_datetime timestamp without time zone,")
    lines.append("  operation_datetime timestamp without time zone,")
    lines.append("  access_key text,")
    lines.append("  series text,")
    lines.append("  customer_bling_id text,")
    lines.append("  customer_name text,")
    lines.append("  customer_tax_id text,")
    lines.append("  customer_state text,")
    lines.append("  natureza_id text,")
    lines.append("  salesperson_bling_id text,")
    lines.append("  total_amount numeric(14,2) not null default 0,")
    lines.append("  freight_amount numeric(14,2) not null default 0,")
    lines.append("  first_cfop text,")
    lines.append("  payload jsonb not null default '{}'::jsonb,")
    lines.append("  source_system text not null default 'bling',")
    lines.append("  external_ref text not null,")
    lines.append("  created_at timestamp with time zone not null default now(),")
    lines.append("  updated_at timestamp with time zone not null default now(),")
    lines.append("  unique (source_system, external_ref)")
    lines.append(");")
    lines.append("")
    lines.append("-- Upsert NF-e rows from Bling")
    for batch in chunked(rows, batch_size):
        lines.append(
            "insert into public.bling_nfe_documents "
            "(company, bling_nfe_id, invoice_number, issue_datetime, operation_datetime, access_key, series, "
            "customer_bling_id, customer_name, customer_tax_id, customer_state, natureza_id, salesperson_bling_id, "
            "total_amount, freight_amount, first_cfop, payload, source_system, external_ref)"
        )
        lines.append("values")
        values: list[str] = []
        for row in batch:
            values.append(
                "("
                + ", ".join(
                    [
                        sql_quote(row["company"]),
                        sql_quote(row["bling_nfe_id"]),
                        sql_quote(row["invoice_number"]),
                        sql_quote(row["issue_datetime"]),
                        sql_quote(row["operation_datetime"]),
                        sql_quote(row["access_key"]),
                        sql_quote(row["series"]),
                        sql_quote(row["customer_bling_id"]),
                        sql_quote(row["customer_name"]),
                        sql_quote(row["customer_tax_id"]),
                        sql_quote(row["customer_state"]),
                        sql_quote(row["natureza_id"]),
                        sql_quote(row["salesperson_bling_id"]),
                        sql_quote(row["total_amount"]),
                        sql_quote(row["freight_amount"]),
                        sql_quote(row["first_cfop"]),
                        sql_json(row["payload"]),
                        sql_quote("bling"),
                        sql_quote(row["external_ref"]),
                    ]
                )
                + ")"
            )
        lines.append(",\n".join(values))
        lines.append(
            "on conflict (source_system, external_ref) do update set "
            "company = excluded.company, "
            "bling_nfe_id = excluded.bling_nfe_id, "
            "invoice_number = excluded.invoice_number, "
            "issue_datetime = excluded.issue_datetime, "
            "operation_datetime = excluded.operation_datetime, "
            "access_key = excluded.access_key, "
            "series = excluded.series, "
            "customer_bling_id = excluded.customer_bling_id, "
            "customer_name = excluded.customer_name, "
            "customer_tax_id = excluded.customer_tax_id, "
            "customer_state = excluded.customer_state, "
            "natureza_id = excluded.natureza_id, "
            "salesperson_bling_id = excluded.salesperson_bling_id, "
            "total_amount = excluded.total_amount, "
            "freight_amount = excluded.freight_amount, "
            "first_cfop = excluded.first_cfop, "
            "payload = excluded.payload, "
            "updated_at = now();"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    company = str(args.company).upper()
    suffix = "_cr" if company == "CR" else ""
    bling_dir = Path(args.bling_dir)
    migrations_dir = Path(args.migrations_dir)
    status_dir = Path(args.status_dir)
    migrations_dir.mkdir(parents=True, exist_ok=True)
    status_dir.mkdir(parents=True, exist_ok=True)

    cache_path = bling_dir / f"nfe_2026_cache{suffix}.jsonl"
    rows = normalize_rows(read_jsonl(cache_path), company)
    timestamp = str(args.run_id).replace("_", "")
    run_id = f"{timestamp}_{company.lower()}"
    migration_name = f"{timestamp}_bling_sales_{company.lower()}_nfe_import_v1.sql"
    migration_path = migrations_dir / migration_name
    migration_path.write_text(build_sql(rows, args.batch_size, run_id), encoding="utf-8")

    status = {
        "status": "ok",
        "run_id": run_id,
        "company": company,
        "cache_path": str(cache_path),
        "rows": len(rows),
        "migration_path": str(migration_path),
        "generated_at": datetime.now().isoformat(),
    }
    status_path = status_dir / f"{run_id}_status.json"
    status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    print(migration_path)


if __name__ == "__main__":
    main()
