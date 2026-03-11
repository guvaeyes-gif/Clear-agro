from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[3]
    default_bling = (
        root
        / "11_agentes_automacoes"
        / "11_dev_codex_agent"
        / "repos"
        / "CRM_Clear_Agro"
        / "bling_api"
    )
    default_mig = root / "supabase" / "migrations"
    default_out = (
        root
        / "11_agentes_automacoes"
        / "12_integracoes_agent"
        / "pipeline"
        / "out"
        / "status"
    )

    ap = argparse.ArgumentParser(
        description="Generate Bling -> Supabase import migration for suppliers/customers/AP/AR."
    )
    ap.add_argument("--bling-dir", default=str(default_bling))
    ap.add_argument("--migrations-dir", default=str(default_mig))
    ap.add_argument("--status-dir", default=str(default_out))
    ap.add_argument(
        "--from-date",
        default="2025-01-01",
        help="Filter by due date (vencimento) >= YYYY-MM-DD.",
    )
    ap.add_argument("--run-id", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    ap.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows per SQL batch.",
    )
    ap.add_argument(
        "--company",
        default="CZ",
        choices=["CZ", "CR", "cz", "cr"],
        help="Bling account/company tag to load source caches from.",
    )
    return ap.parse_args()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                obj = json.loads(ln)
            except Exception:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def digits_only(value: Any) -> str | None:
    txt = "".join(ch for ch in str(value or "") if ch.isdigit())
    return txt or None


def parse_date(value: Any) -> str | None:
    if not value:
        return None
    s = str(value).strip()
    if len(s) >= 10:
        s = s[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date().isoformat()
    except Exception:
        return None


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    txt = str(value).strip().replace(".", "").replace(",", ".")
    # Bling JSON values are usually numeric; this fallback handles stringed commas.
    try:
        return Decimal(str(value))
    except Exception:
        try:
            return Decimal(txt)
        except (InvalidOperation, ValueError):
            return None


def sql_quote(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
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


def map_payable_status(situacao: Any, due_iso: str | None) -> str:
    try:
        s = int(situacao)
    except Exception:
        s = None
    if s == 2:
        return "paid"
    if s == 3:
        return "cancelled"
    if due_iso:
        try:
            if datetime.strptime(due_iso, "%Y-%m-%d").date() < date.today():
                return "overdue"
        except Exception:
            pass
    return "open"


def map_receivable_status(situacao: Any, due_iso: str | None) -> str:
    try:
        s = int(situacao)
    except Exception:
        s = None
    if s == 2:
        return "received"
    if s == 3:
        return "cancelled"
    if due_iso:
        try:
            if datetime.strptime(due_iso, "%Y-%m-%d").date() < date.today():
                return "overdue"
        except Exception:
            pass
    return "open"


def build_sql(
    suppliers: list[dict[str, Any]],
    customers: list[dict[str, Any]],
    ap_rows: list[dict[str, Any]],
    ar_rows: list[dict[str, Any]],
    batch_size: int,
    run_id: str,
) -> str:
    lines: list[str] = []
    lines.append("-- Clear OS / Supabase")
    lines.append(f"-- Migration: {run_id}_bling_import_v1.sql")
    lines.append("-- Purpose: incremental Bling import to core tables")
    lines.append("")
    lines.append("-- 1) Add source tracking columns for idempotent upserts")
    for table in ("suppliers", "customers", "accounts_payable", "accounts_receivable"):
        lines.append(
            f"ALTER TABLE public.{table} ADD COLUMN IF NOT EXISTS source_system text NOT NULL DEFAULT 'manual';"
        )
        lines.append(f"ALTER TABLE public.{table} ADD COLUMN IF NOT EXISTS external_ref text;")
        lines.append(
            f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_source_external_ref "
            f"ON public.{table}(source_system, external_ref);"
        )
    lines.append("")
    lines.append("-- 2) Ensure import categories exist")
    lines.append(
        "INSERT INTO public.financial_categories "
        "(code, name, category_group, is_cash_flow, status, notes, metadata) "
        "VALUES "
        "('BLING_AP_IMPORT', 'Bling AP Import', 'expense', TRUE, 'active', "
        "'Auto-created for Bling import', jsonb_build_object('source','bling')), "
        "('BLING_AR_IMPORT', 'Bling AR Import', 'revenue', TRUE, 'active', "
        "'Auto-created for Bling import', jsonb_build_object('source','bling')) "
        "ON CONFLICT (code) DO UPDATE SET updated_at = now();"
    )
    lines.append("")

    lines.append("-- 3) Upsert suppliers")
    for batch in chunked(suppliers, batch_size):
        lines.append("WITH src(external_ref, legal_name, trade_name, tax_id, metadata) AS (")
        lines.append("VALUES")
        values: list[str] = []
        for row in batch:
            values.append(
                "("
                + ", ".join(
                    [
                        sql_quote(row["external_ref"]),
                        sql_quote(row["legal_name"]),
                        sql_quote(row.get("trade_name")),
                        sql_quote(row.get("tax_id")),
                        sql_json(row["metadata"]),
                    ]
                )
                + ")"
            )
        lines.append(",\n".join(values))
        lines.append(
            "), src_norm AS ("
            " SELECT "
            "src.external_ref, src.legal_name, src.trade_name, src.tax_id, src.metadata, "
            "CASE "
            "WHEN src.tax_id IS NULL OR src.tax_id = '' THEN NULL "
            "WHEN row_number() OVER (PARTITION BY src.tax_id ORDER BY src.external_ref) > 1 THEN NULL "
            "WHEN EXISTS ("
            "  SELECT 1 FROM public.suppliers sx "
            "  WHERE sx.tax_id = src.tax_id "
            "    AND (sx.source_system <> 'bling' OR sx.external_ref <> src.external_ref)"
            ") THEN NULL "
            "ELSE src.tax_id END AS tax_id_safe "
            "FROM src"
            ")"
        )
        lines.append(
            "INSERT INTO public.suppliers "
            "(legal_name, trade_name, tax_id, status, payment_terms_days, notes, metadata, source_system, external_ref) "
            "SELECT src_norm.legal_name, src_norm.trade_name, src_norm.tax_id_safe, "
            "'active', 30, 'Imported from Bling contatos/contas_pagar', src_norm.metadata, 'bling', src_norm.external_ref "
            "FROM src_norm"
        )
        lines.append(
            "ON CONFLICT (source_system, external_ref) DO UPDATE SET "
            "legal_name = EXCLUDED.legal_name, "
            "trade_name = EXCLUDED.trade_name, "
            "tax_id = COALESCE(EXCLUDED.tax_id, public.suppliers.tax_id), "
            "status = 'active', "
            "metadata = COALESCE(public.suppliers.metadata, '{}'::jsonb) || EXCLUDED.metadata, "
            "updated_at = now();"
        )
    lines.append("")

    lines.append("-- 4) Upsert customers")
    for batch in chunked(customers, batch_size):
        lines.append("WITH src(external_ref, legal_name, trade_name, tax_id, metadata) AS (")
        lines.append("VALUES")
        values = []
        for row in batch:
            values.append(
                "("
                + ", ".join(
                    [
                        sql_quote(row["external_ref"]),
                        sql_quote(row["legal_name"]),
                        sql_quote(row.get("trade_name")),
                        sql_quote(row.get("tax_id")),
                        sql_json(row["metadata"]),
                    ]
                )
                + ")"
            )
        lines.append(",\n".join(values))
        lines.append(
            "), src_norm AS ("
            " SELECT "
            "src.external_ref, src.legal_name, src.trade_name, src.tax_id, src.metadata, "
            "CASE "
            "WHEN src.tax_id IS NULL OR src.tax_id = '' THEN NULL "
            "WHEN row_number() OVER (PARTITION BY src.tax_id ORDER BY src.external_ref) > 1 THEN NULL "
            "WHEN EXISTS ("
            "  SELECT 1 FROM public.customers cx "
            "  WHERE cx.tax_id = src.tax_id "
            "    AND (cx.source_system <> 'bling' OR cx.external_ref <> src.external_ref)"
            ") THEN NULL "
            "ELSE src.tax_id END AS tax_id_safe "
            "FROM src"
            ")"
        )
        lines.append(
            "INSERT INTO public.customers "
            "(legal_name, trade_name, tax_id, status, notes, metadata, source_system, external_ref) "
            "SELECT src_norm.legal_name, src_norm.trade_name, src_norm.tax_id_safe, "
            "'active', 'Imported from Bling contas_receber', src_norm.metadata, 'bling', src_norm.external_ref "
            "FROM src_norm"
        )
        lines.append(
            "ON CONFLICT (source_system, external_ref) DO UPDATE SET "
            "legal_name = EXCLUDED.legal_name, "
            "trade_name = EXCLUDED.trade_name, "
            "tax_id = COALESCE(EXCLUDED.tax_id, public.customers.tax_id), "
            "status = 'active', "
            "metadata = COALESCE(public.customers.metadata, '{}'::jsonb) || EXCLUDED.metadata, "
            "updated_at = now();"
        )
    lines.append("")

    lines.append("-- 5) Upsert accounts_payable from Bling")
    for batch in chunked(ap_rows, batch_size):
        lines.append(
            "WITH src(external_ref, supplier_external_ref, issue_date, due_date, amount, status, description, document_number, metadata) AS ("
        )
        lines.append("VALUES")
        values = []
        for row in batch:
            values.append(
                "("
                + ", ".join(
                    [
                        sql_quote(row["external_ref"]),
                        sql_quote(row["supplier_external_ref"]),
                        sql_quote(row["issue_date"]),
                        sql_quote(row["due_date"]),
                        sql_quote(row["amount"]),
                        sql_quote(row["status"]),
                        sql_quote(row["description"]),
                        sql_quote(row["document_number"]),
                        sql_json(row["metadata"]),
                    ]
                )
                + ")"
            )
        lines.append(",\n".join(values))
        lines.append("), cat AS (")
        lines.append("  SELECT id FROM public.financial_categories WHERE code = 'BLING_AP_IMPORT' LIMIT 1")
        lines.append(")")
        lines.append(
            "INSERT INTO public.accounts_payable "
            "(supplier_id, category_id, description, document_number, issue_date, due_date, amount, currency_code, status, notes, metadata, source_system, external_ref)"
        )
        lines.append(
            "SELECT s.id, cat.id, src.description, src.document_number, "
            "LEAST(src.issue_date::date, src.due_date::date), "
            "GREATEST(src.issue_date::date, src.due_date::date), "
            "src.amount::numeric(14,2), 'BRL', src.status::payable_status, "
            "'Imported from Bling contas_pagar', src.metadata, 'bling', src.external_ref "
            "FROM src "
            "JOIN public.suppliers s ON s.source_system = 'bling' AND s.external_ref = src.supplier_external_ref "
            "CROSS JOIN cat "
            "ON CONFLICT (source_system, external_ref) DO UPDATE SET "
            "supplier_id = EXCLUDED.supplier_id, "
            "category_id = EXCLUDED.category_id, "
            "description = EXCLUDED.description, "
            "document_number = EXCLUDED.document_number, "
            "issue_date = EXCLUDED.issue_date, "
            "due_date = EXCLUDED.due_date, "
            "amount = EXCLUDED.amount, "
            "status = EXCLUDED.status, "
            "metadata = COALESCE(public.accounts_payable.metadata, '{}'::jsonb) || EXCLUDED.metadata, "
            "updated_at = now();"
        )
    lines.append("")

    lines.append("-- 6) Upsert accounts_receivable from Bling")
    for batch in chunked(ar_rows, batch_size):
        lines.append(
            "WITH src(external_ref, customer_external_ref, issue_date, due_date, amount, status, description, document_number, metadata) AS ("
        )
        lines.append("VALUES")
        values = []
        for row in batch:
            values.append(
                "("
                + ", ".join(
                    [
                        sql_quote(row["external_ref"]),
                        sql_quote(row["customer_external_ref"]),
                        sql_quote(row["issue_date"]),
                        sql_quote(row["due_date"]),
                        sql_quote(row["amount"]),
                        sql_quote(row["status"]),
                        sql_quote(row["description"]),
                        sql_quote(row["document_number"]),
                        sql_json(row["metadata"]),
                    ]
                )
                + ")"
            )
        lines.append(",\n".join(values))
        lines.append("), cat AS (")
        lines.append("  SELECT id FROM public.financial_categories WHERE code = 'BLING_AR_IMPORT' LIMIT 1")
        lines.append(")")
        lines.append(
            "INSERT INTO public.accounts_receivable "
            "(customer_id, category_id, description, invoice_number, issue_date, due_date, amount, currency_code, status, notes, metadata, source_system, external_ref)"
        )
        lines.append(
            "SELECT c.id, cat.id, src.description, src.document_number, "
            "LEAST(src.issue_date::date, src.due_date::date), "
            "GREATEST(src.issue_date::date, src.due_date::date), "
            "src.amount::numeric(14,2), 'BRL', src.status::receivable_status, "
            "'Imported from Bling contas_receber', src.metadata, 'bling', src.external_ref "
            "FROM src "
            "JOIN public.customers c ON c.source_system = 'bling' AND c.external_ref = src.customer_external_ref "
            "CROSS JOIN cat "
            "ON CONFLICT (source_system, external_ref) DO UPDATE SET "
            "customer_id = EXCLUDED.customer_id, "
            "category_id = EXCLUDED.category_id, "
            "description = EXCLUDED.description, "
            "invoice_number = EXCLUDED.invoice_number, "
            "issue_date = EXCLUDED.issue_date, "
            "due_date = EXCLUDED.due_date, "
            "amount = EXCLUDED.amount, "
            "status = EXCLUDED.status, "
            "metadata = COALESCE(public.accounts_receivable.metadata, '{}'::jsonb) || EXCLUDED.metadata, "
            "updated_at = now();"
        )

    lines.append("")
    lines.append("DO $$ BEGIN RAISE NOTICE 'Bling import migration finished.'; END $$;")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    bling_dir = Path(args.bling_dir)
    migrations_dir = Path(args.migrations_dir)
    status_dir = Path(args.status_dir)
    migrations_dir.mkdir(parents=True, exist_ok=True)
    status_dir.mkdir(parents=True, exist_ok=True)

    from_date = datetime.strptime(args.from_date, "%Y-%m-%d").date()
    company_tag = str(args.company or "CZ").strip().upper()
    if company_tag not in {"CZ", "CR"}:
        raise RuntimeError(f"Unsupported company: {args.company}")
    suffix = "" if company_tag == "CZ" else "_cr"

    contatos = read_jsonl(bling_dir / f"contatos_cache{suffix}.jsonl")
    ap_raw = read_jsonl(bling_dir / f"contas_pagar_cache{suffix}.jsonl")
    ar_raw = read_jsonl(bling_dir / f"contas_receber_cache{suffix}.jsonl")

    contatos_by_id: dict[str, dict[str, Any]] = {str(c.get("id")): c for c in contatos if c.get("id") is not None}

    ap_filtered: list[dict[str, Any]] = []
    for row in ap_raw:
        due = parse_date(row.get("vencimento"))
        if not due:
            continue
        if datetime.strptime(due, "%Y-%m-%d").date() < from_date:
            continue
        ap_filtered.append(row)

    ar_filtered: list[dict[str, Any]] = []
    for row in ar_raw:
        due = parse_date(row.get("vencimento"))
        if not due:
            continue
        if datetime.strptime(due, "%Y-%m-%d").date() < from_date:
            continue
        ar_filtered.append(row)

    supplier_by_ext: dict[str, dict[str, Any]] = {}
    for row in ap_filtered:
        contato = row.get("contato") or {}
        contato_id = str(contato.get("id") or "").strip()
        if not contato_id:
            continue
        ext = f"bling_contact:{contato_id}"
        ref = contatos_by_id.get(contato_id, {})
        name = (
            str(contato.get("nome") or "").strip()
            or str(ref.get("nome") or "").strip()
            or f"BLING_CONTATO_{contato_id}"
        )
        tax_id = digits_only(contato.get("numeroDocumento") or ref.get("numeroDocumento"))
        supplier_by_ext[ext] = {
            "external_ref": ext,
            "legal_name": name,
            "trade_name": name,
            "tax_id": tax_id,
            "metadata": {
                "bling_contact_id": contato_id,
                "contact_status": ref.get("situacao"),
                "source": "bling",
                "company": company_tag,
            },
        }

    customer_by_ext: dict[str, dict[str, Any]] = {}
    for row in ar_filtered:
        contato = row.get("contato") or {}
        contato_id = str(contato.get("id") or "").strip()
        if not contato_id:
            continue
        ext = f"bling_contact:{contato_id}"
        name = str(contato.get("nome") or "").strip() or f"BLING_CLIENTE_{contato_id}"
        tax_id = digits_only(contato.get("numeroDocumento"))
        customer_by_ext[ext] = {
            "external_ref": ext,
            "legal_name": name,
            "trade_name": name,
            "tax_id": tax_id,
            "metadata": {
                "bling_contact_id": contato_id,
                "contact_type": contato.get("tipo"),
                "source": "bling",
                "company": company_tag,
            },
        }

    ap_rows: list[dict[str, Any]] = []
    for row in ap_filtered:
        rid = row.get("id")
        if rid is None:
            continue
        due = parse_date(row.get("vencimento"))
        issue = parse_date(row.get("dataEmissao")) or parse_date((row.get("origem") or {}).get("dataEmissao")) or due
        amount = to_decimal(row.get("valor"))
        contato = row.get("contato") or {}
        contato_id = str(contato.get("id") or "").strip()
        if not due or not issue or amount is None or amount <= 0 or not contato_id:
            continue
        origem = row.get("origem") or {}
        description = f"Bling AP {rid}"
        if origem.get("tipoOrigem") and origem.get("numero"):
            description = f"Bling AP {origem.get('tipoOrigem')} {origem.get('numero')}"
        ap_rows.append(
            {
                "external_ref": f"bling_ap:{rid}",
                "supplier_external_ref": f"bling_contact:{contato_id}",
                "issue_date": issue,
                "due_date": due,
                "amount": amount,
                "status": map_payable_status(row.get("situacao"), due),
                "description": description,
                "document_number": str(origem.get("numero") or rid),
                "metadata": {
                    "bling_id": rid,
                    "contato_id": contato_id,
                    "situacao": row.get("situacao"),
                    "origem_id": origem.get("id"),
                    "forma_pagamento_id": (row.get("formaPagamento") or {}).get("id"),
                    "source": "bling",
                    "company": company_tag,
                },
            }
        )

    ar_rows: list[dict[str, Any]] = []
    for row in ar_filtered:
        rid = row.get("id")
        if rid is None:
            continue
        due = parse_date(row.get("vencimento"))
        issue = parse_date(row.get("dataEmissao")) or parse_date((row.get("origem") or {}).get("dataEmissao")) or due
        amount = to_decimal(row.get("valor"))
        contato = row.get("contato") or {}
        contato_id = str(contato.get("id") or "").strip()
        if not due or not issue or amount is None or amount <= 0 or not contato_id:
            continue
        origem = row.get("origem") or {}
        description = f"Bling AR {rid}"
        if origem.get("tipoOrigem") and origem.get("numero"):
            description = f"Bling AR {origem.get('tipoOrigem')} {origem.get('numero')}"
        ar_rows.append(
            {
                "external_ref": f"bling_ar:{rid}",
                "customer_external_ref": f"bling_contact:{contato_id}",
                "issue_date": issue,
                "due_date": due,
                "amount": amount,
                "status": map_receivable_status(row.get("situacao"), due),
                "description": description,
                "document_number": str(origem.get("numero") or rid),
                "metadata": {
                    "bling_id": rid,
                    "contato_id": contato_id,
                    "situacao": row.get("situacao"),
                    "origem_id": origem.get("id"),
                    "forma_pagamento_id": (row.get("formaPagamento") or {}).get("id"),
                    "source": "bling",
                    "company": company_tag,
                },
            }
        )

    suppliers = sorted(supplier_by_ext.values(), key=lambda x: x["external_ref"])
    customers = sorted(customer_by_ext.values(), key=lambda x: x["external_ref"])
    ap_rows = sorted(ap_rows, key=lambda x: x["external_ref"])
    ar_rows = sorted(ar_rows, key=lambda x: x["external_ref"])

    migration_stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    migration_name = f"{migration_stamp}_bling_import_v1.sql"
    migration_path = migrations_dir / migration_name
    sql = build_sql(
        suppliers=suppliers,
        customers=customers,
        ap_rows=ap_rows,
        ar_rows=ar_rows,
        batch_size=max(50, args.batch_size),
        run_id=args.run_id,
    )
    migration_path.write_text(sql, encoding="utf-8")

    status = {
        "status": "success",
        "run_id": args.run_id,
        "company": company_tag,
        "from_date": args.from_date,
        "source_suffix": suffix,
        "migration_file": str(migration_path),
        "counts": {
            "contatos_total": len(contatos),
            "contas_pagar_total": len(ap_raw),
            "contas_receber_total": len(ar_raw),
            "contas_pagar_filtrado": len(ap_filtered),
            "contas_receber_filtrado": len(ar_filtered),
            "suppliers_upsert_rows": len(suppliers),
            "customers_upsert_rows": len(customers),
            "accounts_payable_upsert_rows": len(ap_rows),
            "accounts_receivable_upsert_rows": len(ar_rows),
        },
    }
    status_path = status_dir / f"bling_import_generator_{args.run_id}_status.json"
    status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

    print(str(migration_path))
    print(str(status_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
