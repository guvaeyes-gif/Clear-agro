from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src import metas_db


SEED_SOURCE = "crm_seed_phase1c"
SEED_TAG = "crm_phase1c_sales_opportunities"
STAGE_BLUEPRINT = [
    ("lead", 4, 15, 25000),
    ("qualified", 4, 35, 42000),
    ("proposal", 4, 55, 68000),
    ("negotiation", 4, 75, 98000),
    ("won", 4, 100, 120000),
    ("lost", 4, 0, 30000),
]


def _fetch_seed_context() -> tuple[list[dict], str | None, set[str]]:
    customers = metas_db._rest_request(
        "GET",
        "customers",
        params={"select": "id,legal_name,trade_name,owner_sales_rep_id", "limit": "200"},
    ) or []
    users = metas_db._rest_request(
        "GET",
        "app_users",
        params={"select": "id,role", "order": "created_at.asc", "limit": "10"},
    ) or []
    existing = metas_db._rest_request(
        "GET",
        "sales_opportunities",
        params={"select": "title", "source": f"eq.{SEED_SOURCE}", "limit": "500"},
    ) or []
    created_by = next((row["id"] for row in users if row.get("role") == "super_admin"), None)
    titles = {str(row.get("title", "")).strip() for row in existing if str(row.get("title", "")).strip()}
    return customers, created_by, titles


def _build_rows(customers: list[dict], created_by: str | None, existing_titles: set[str]) -> list[dict]:
    if not customers:
        raise RuntimeError("No customers found in Supabase.")

    today = date.today()
    rows: list[dict] = []
    sequence = 1
    for stage, qty, probability, base_value in STAGE_BLUEPRINT:
        for offset in range(qty):
            customer = customers[(sequence - 1) % len(customers)]
            title = f"Seed CRM {sequence:02d} - {stage.upper()}"
            if title in existing_titles:
                sequence += 1
                continue

            customer_name = customer.get("trade_name") or customer.get("legal_name") or "Cliente"
            rows.append(
                {
                    "customer_id": customer["id"],
                    "owner_sales_rep_id": customer.get("owner_sales_rep_id"),
                    "title": title,
                    "stage": stage,
                    "expected_value": float(base_value + offset * 3500 + sequence * 250),
                    "probability": probability,
                    "expected_close_date": (today + timedelta(days=10 + offset * 3 + sequence)).isoformat(),
                    "source": SEED_SOURCE,
                    "status": "active",
                    "notes": f"Oportunidade seed para ativar pipeline operacional do CRM ({customer_name}).",
                    "metadata": {
                        "seed_tag": SEED_TAG,
                        "seed_sequence": sequence,
                        "seed_stage": stage,
                    },
                    "created_by": created_by,
                }
            )
            sequence += 1
    return rows


def main() -> None:
    if metas_db._backend_mode() == "sqlite":
        raise RuntimeError("Supabase backend not configured.")
    customers, created_by, existing_titles = _fetch_seed_context()
    payload = _build_rows(customers, created_by, existing_titles)
    created = metas_db._rest_request(
        "POST",
        "sales_opportunities",
        payload=payload,
        prefer="return=representation",
    ) or []
    print(
        f"customers={len(customers)} existing_seeded={len(existing_titles)} inserted={len(created)} created_by={created_by}"
    )


if __name__ == "__main__":
    main()
