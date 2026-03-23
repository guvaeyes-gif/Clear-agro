from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


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
    default_status = (
        root
        / "11_agentes_automacoes"
        / "12_integracoes_agent"
        / "pipeline"
        / "out"
        / "status"
    )
    default_project_ref = root / "supabase" / ".temp" / "project-ref"

    ap = argparse.ArgumentParser(description="Reconcile Bling caches with Supabase AP/AR tables.")
    ap.add_argument("--bling-dir", default=str(default_bling))
    ap.add_argument("--status-dir", default=str(default_status))
    ap.add_argument("--run-id", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    ap.add_argument("--from-date", default="2025-01-01", help="Filter due_date >= YYYY-MM-DD.")
    ap.add_argument("--project-ref", default=None, help="Supabase project ref.")
    ap.add_argument("--project-ref-file", default=str(default_project_ref))
    ap.add_argument("--supabase-token-path", default=str(Path.home() / "Documents" / "token supabase.txt"))
    ap.add_argument("--page-size", type=int, default=1000)
    ap.add_argument(
        "--company",
        default="ALL",
        choices=["ALL", "CZ", "CR", "all", "cz", "cr"],
        help="Company scope for checks (ALL, CZ, CR).",
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


def parse_date_iso(value: Any) -> str | None:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    txt = txt[:10]
    try:
        return datetime.strptime(txt, "%Y-%m-%d").date().isoformat()
    except Exception:
        return None


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        txt = str(value).strip().replace(".", "").replace(",", ".")
        try:
            return Decimal(txt)
        except (InvalidOperation, ValueError):
            return None


def read_supabase_token(token_path: Path) -> str:
    env_token = (os.getenv("SUPABASE_ACCESS_TOKEN") or "").strip()
    if env_token:
        return env_token
    if not token_path.exists():
        raise RuntimeError(f"Supabase token file not found: {token_path}")
    token = token_path.read_text(encoding="utf-8").strip()
    if not token:
        raise RuntimeError(f"Supabase token file is empty: {token_path}")
    return token


def resolve_project_ref(project_ref: str | None, project_ref_file: Path) -> str:
    if project_ref:
        return project_ref.strip()
    if not project_ref_file.exists():
        raise RuntimeError(f"Project ref file not found: {project_ref_file}")
    resolved = project_ref_file.read_text(encoding="utf-8").strip()
    if not resolved:
        raise RuntimeError(f"Project ref file is empty: {project_ref_file}")
    return resolved


def fetch_service_role_key(root: Path, project_ref: str, access_token: str) -> str:
    env = dict(os.environ)
    env["SUPABASE_ACCESS_TOKEN"] = access_token
    cmd = [
        "npx.cmd",
        "supabase",
        "projects",
        "api-keys",
        "--project-ref",
        project_ref,
        "-o",
        "json",
    ]
    proc = subprocess.run(
        cmd,
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Failed to fetch Supabase API keys: {proc.stderr.strip() or proc.stdout.strip()}")
    try:
        items = json.loads(proc.stdout)
    except Exception as exc:
        raise RuntimeError(f"Invalid API keys payload from Supabase CLI: {exc}") from exc
    if not isinstance(items, list):
        raise RuntimeError("Unexpected API keys response format.")

    for row in items:
        if not isinstance(row, dict):
            continue
        key = str(row.get("api_key") or "").strip()
        if not key:
            continue
        role_hint = str((row.get("secret_jwt_template") or {}).get("role") or "").strip().lower()
        name_hint = str(row.get("name") or "").strip().lower()
        id_hint = str(row.get("id") or "").strip().lower()
        desc_hint = str(row.get("description") or "").strip().lower()
        if "service_role" in {role_hint, name_hint, id_hint} or "service_role" in desc_hint:
            if key.startswith("sb_secret_") and key.endswith("..."):
                continue
            return key
    raise RuntimeError("Service role key not found in Supabase API keys list.")


def fetch_table_rows(
    project_ref: str,
    service_role_key: str,
    table: str,
    page_size: int,
) -> list[dict[str, Any]]:
    base_url = f"https://{project_ref}.supabase.co/rest/v1/{table}"
    params = urlencode(
        {
            "select": "external_ref,amount,due_date,metadata",
            "source_system": "eq.bling",
        }
    )
    offset = 0
    out: list[dict[str, Any]] = []

    while True:
        req = Request(f"{base_url}?{params}")
        req.add_header("apikey", service_role_key)
        req.add_header("Authorization", f"Bearer {service_role_key}")
        req.add_header("Accept", "application/json")
        req.add_header("Range", f"{offset}-{offset + page_size - 1}")
        req.add_header("Prefer", "count=exact")
        with urlopen(req, timeout=60) as resp:
            payload = resp.read().decode("utf-8")
        rows = json.loads(payload)
        if not isinstance(rows, list) or not rows:
            break
        for row in rows:
            if isinstance(row, dict):
                out.append(row)
        if len(rows) < page_size:
            break
        offset += page_size
    return out


def load_source_metrics(
    bling_dir: Path,
    from_date: str,
    selected_companies: list[str],
) -> tuple[dict[str, Any], dict[str, str]]:
    from_dt = datetime.strptime(from_date, "%Y-%m-%d").date()
    source: dict[str, Any] = {
        "accounts_payable": {},
        "accounts_receivable": {},
    }
    contato_to_company: dict[str, str] = {}

    for company, suffix in (("CZ", ""), ("CR", "_cr")):
        ap_path = bling_dir / f"contas_pagar_cache{suffix}.jsonl"
        ar_path = bling_dir / f"contas_receber_cache{suffix}.jsonl"

        ap_sum = Decimal("0")
        ap_count = 0
        for row in read_jsonl(ap_path):
            due = parse_date_iso(row.get("vencimento"))
            if not due or datetime.strptime(due, "%Y-%m-%d").date() < from_dt:
                continue
            amount = to_decimal(row.get("valor"))
            if amount is None or amount <= 0:
                continue
            contato_id = str((row.get("contato") or {}).get("id") or "").strip()
            if contato_id:
                contato_to_company[contato_id] = company
            ap_count += 1
            ap_sum += amount

        ar_sum = Decimal("0")
        ar_count = 0
        for row in read_jsonl(ar_path):
            due = parse_date_iso(row.get("vencimento"))
            if not due or datetime.strptime(due, "%Y-%m-%d").date() < from_dt:
                continue
            amount = to_decimal(row.get("valor"))
            if amount is None or amount <= 0:
                continue
            contato_id = str((row.get("contato") or {}).get("id") or "").strip()
            if contato_id:
                contato_to_company[contato_id] = company
            ar_count += 1
            ar_sum += amount

        if company in selected_companies:
            source["accounts_payable"][company] = {"count": ap_count, "amount": f"{ap_sum:.2f}"}
            source["accounts_receivable"][company] = {"count": ar_count, "amount": f"{ar_sum:.2f}"}

    for table in ("accounts_payable", "accounts_receivable"):
        total_count = sum(int(source[table][company]["count"]) for company in selected_companies)
        total_amount = sum((Decimal(str(source[table][company]["amount"])) for company in selected_companies), Decimal("0"))
        source[table]["TOTAL"] = {"count": total_count, "amount": f"{total_amount:.2f}"}
    return source, contato_to_company


def normalize_company_hint(value: Any) -> str | None:
    txt = str(value or "").strip().upper()
    if not txt:
        return None
    if txt in {"CZ", "CR"}:
        return txt
    if txt.startswith("CZ"):
        return "CZ"
    if txt.startswith("CR"):
        return "CR"
    return None


def classify_company(row: dict[str, Any], contato_to_company: dict[str, str]) -> str:
    meta = row.get("metadata")
    metadata = meta if isinstance(meta, dict) else {}

    for key in ("company", "empresa", "source_company", "account"):
        hit = normalize_company_hint(metadata.get(key))
        if hit:
            return hit

    for key in ("contato_id", "contact_id", "bling_contact_id"):
        cid = str(metadata.get(key) or "").strip()
        if cid and cid in contato_to_company:
            return contato_to_company[cid]

    ext = str(row.get("external_ref") or "").strip().lower()
    if ext.startswith("cz:"):
        return "CZ"
    if ext.startswith("cr:"):
        return "CR"
    return "UNKNOWN"


def build_destination_metrics(
    ap_rows: list[dict[str, Any]],
    ar_rows: list[dict[str, Any]],
    from_date: str,
    contato_to_company: dict[str, str],
    selected_companies: list[str],
) -> dict[str, Any]:
    from_dt = datetime.strptime(from_date, "%Y-%m-%d").date()

    def init_table() -> dict[str, dict[str, Any]]:
        data = {company: {"count": 0, "amount": "0.00"} for company in selected_companies}
        data["UNKNOWN"] = {"count": 0, "amount": "0.00"}
        data["TOTAL"] = {"count": 0, "amount": "0.00"}
        return data

    out = {"accounts_payable": init_table(), "accounts_receivable": init_table()}

    def apply_rows(table: str, rows: list[dict[str, Any]]) -> None:
        sums = {company: Decimal("0") for company in selected_companies}
        sums["UNKNOWN"] = Decimal("0")
        counts = {company: 0 for company in selected_companies}
        counts["UNKNOWN"] = 0

        for row in rows:
            due = parse_date_iso(row.get("due_date"))
            if not due or datetime.strptime(due, "%Y-%m-%d").date() < from_dt:
                continue
            amount = to_decimal(row.get("amount"))
            if amount is None:
                continue
            company = classify_company(row, contato_to_company)
            if company not in selected_companies and company != "UNKNOWN":
                continue
            if company not in counts:
                company = "UNKNOWN"
            counts[company] += 1
            sums[company] += amount

        for company in [*selected_companies, "UNKNOWN"]:
            out[table][company] = {"count": counts[company], "amount": f"{sums[company]:.2f}"}
        out[table]["TOTAL"] = {
            "count": sum(counts[company] for company in selected_companies),
            "amount": f"{sum((sums[company] for company in selected_companies), Decimal('0')):.2f}",
        }

    apply_rows("accounts_payable", ap_rows)
    apply_rows("accounts_receivable", ar_rows)
    return out


def build_checks(source: dict[str, Any], dest: dict[str, Any], selected_companies: list[str]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    tolerance = Decimal("0.01")
    for table in ("accounts_payable", "accounts_receivable"):
        for company in [*selected_companies, "TOTAL"]:
            src_count = int(source[table][company]["count"])
            dst_count = int(dest[table][company]["count"])
            src_amount = Decimal(str(source[table][company]["amount"]))
            dst_amount = Decimal(str(dest[table][company]["amount"]))
            count_diff = dst_count - src_count
            amount_diff = dst_amount - src_amount
            amount_ok = abs(amount_diff) <= tolerance
            status = "PASS" if count_diff == 0 and amount_ok else "FAIL"
            checks.append(
                {
                    "check_id": f"{table}:{company}",
                    "table": table,
                    "company": company,
                    "source_count": src_count,
                    "dest_count": dst_count,
                    "count_diff": count_diff,
                    "source_amount": f"{src_amount:.2f}",
                    "dest_amount": f"{dst_amount:.2f}",
                    "amount_diff": f"{amount_diff:.2f}",
                    "status": status,
                }
            )

        unknown_count = int(dest[table]["UNKNOWN"]["count"])
        unknown_amount = Decimal(str(dest[table]["UNKNOWN"]["amount"]))
        checks.append(
            {
                "check_id": f"{table}:UNKNOWN",
                "table": table,
                "company": "UNKNOWN",
                "source_count": 0,
                "dest_count": unknown_count,
                "count_diff": unknown_count,
                "source_amount": "0.00",
                "dest_amount": f"{unknown_amount:.2f}",
                "amount_diff": f"{unknown_amount:.2f}",
                "status": "PASS" if unknown_count == 0 and unknown_amount == Decimal("0.00") else "FAIL",
            }
        )
    return checks


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "check_id",
        "table",
        "company",
        "source_count",
        "dest_count",
        "count_diff",
        "source_amount",
        "dest_amount",
        "amount_diff",
        "status",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k) for k in fields})


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[3]
    bling_dir = Path(args.bling_dir)
    status_dir = Path(args.status_dir)
    project_ref_file = Path(args.project_ref_file)
    token_path = Path(args.supabase_token_path)
    status_dir.mkdir(parents=True, exist_ok=True)

    project_ref = resolve_project_ref(args.project_ref, project_ref_file)
    supabase_access_token = read_supabase_token(token_path)
    service_role_key = fetch_service_role_key(root, project_ref, supabase_access_token)
    selected = str(args.company or "ALL").strip().upper()
    selected_companies = ["CZ", "CR"] if selected == "ALL" else [selected]

    source, contato_to_company = load_source_metrics(bling_dir, args.from_date, selected_companies)
    ap_rows = fetch_table_rows(project_ref, service_role_key, "accounts_payable", page_size=max(100, args.page_size))
    ar_rows = fetch_table_rows(
        project_ref, service_role_key, "accounts_receivable", page_size=max(100, args.page_size)
    )
    destination = build_destination_metrics(ap_rows, ar_rows, args.from_date, contato_to_company, selected_companies)
    checks = build_checks(source, destination, selected_companies)
    pass_count = sum(1 for row in checks if row["status"] == "PASS")
    fail_count = sum(1 for row in checks if row["status"] == "FAIL")
    overall = "success" if fail_count == 0 else "failed"

    status = {
        "status": overall,
        "run_id": args.run_id,
        "from_date": args.from_date,
        "company_scope": selected,
        "project_ref": project_ref,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "destination": destination,
        "checks_summary": {"pass": pass_count, "fail": fail_count},
        "checks": checks,
    }

    status_path = status_dir / f"bling_supabase_reconciliation_{args.run_id}_status.json"
    qa_path = status_dir / f"bling_supabase_reconciliation_{args.run_id}_qa.csv"
    status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(qa_path, checks)

    print(str(status_path))
    print(str(qa_path))
    return 0 if fail_count == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
