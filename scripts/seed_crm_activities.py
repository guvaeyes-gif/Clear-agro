from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src import metas_db


SEED_SOURCE = "crm_seed_phase1c"
SEED_TAG = "crm_phase1c_activities"


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _fetch_seed_context() -> tuple[list[dict], str | None, set[str]]:
    raw_opportunities = metas_db._rest_request(
        "GET",
        "sales_opportunities",
        params={
            "select": "id,title,customer_id,owner_sales_rep_id,stage,created_at",
            "order": "created_at.asc",
            "limit": "500",
        },
    ) or []
    opportunities = [
        row
        for row in raw_opportunities
        if str(row.get("title", "")).strip().startswith("Seed CRM ")
    ]
    users = metas_db._rest_request(
        "GET",
        "app_users",
        params={"select": "id,role", "order": "created_at.asc", "limit": "10"},
    ) or []
    existing = metas_db._rest_request(
        "GET",
        "crm_activities",
        params={"select": "external_ref", "source_system": f"eq.{SEED_SOURCE}", "limit": "2000"},
    ) or []
    created_by = next((row["id"] for row in users if row.get("role") == "super_admin"), None)
    external_refs = {
        str(row.get("external_ref", "")).strip()
        for row in existing
        if str(row.get("external_ref", "")).strip()
    }
    return opportunities, created_by, external_refs


def _build_rows(opportunities: list[dict], created_by: str | None, existing_refs: set[str]) -> list[dict]:
    if not opportunities:
        raise RuntimeError("No seeded opportunities found. Run seed_sales_opportunities.py first.")

    now = datetime.now(timezone.utc)
    rows: list[dict] = []
    for sequence, opportunity in enumerate(opportunities, start=1):
        opportunity_id = opportunity["id"]
        customer_id = opportunity.get("customer_id")
        owner_sales_rep_id = opportunity.get("owner_sales_rep_id")
        stage = str(opportunity.get("stage", "lead")).strip().lower()
        title = str(opportunity.get("title", "Seed CRM")).strip()

        blueprints = [
            {
                "activity_type": "call",
                "subject": f"{title} - Call inicial",
                "due_at": _iso(now - timedelta(days=12 - (sequence % 4), hours=2)),
                "completed_at": _iso(now - timedelta(days=12 - (sequence % 4))),
                "priority": "normal",
                "status": "completed",
                "notes": "Contato inicial gerado automaticamente para aquecer a camada operacional do CRM.",
            }
        ]

        if stage in {"lead", "qualified"}:
            blueprints.append(
                {
                    "activity_type": "follow_up",
                    "subject": f"{title} - Follow-up comercial",
                    "due_at": _iso(now + timedelta(days=sequence % 5 + 1)),
                    "completed_at": None,
                    "priority": "high" if stage == "qualified" else "normal",
                    "status": "open",
                    "notes": "Proximo contato para qualificar a oportunidade.",
                }
            )
        elif stage in {"proposal", "negotiation"}:
            blueprints.append(
                {
                    "activity_type": "proposal" if stage == "proposal" else "meeting",
                    "subject": f"{title} - Revisar proposta",
                    "due_at": _iso(now - timedelta(days=sequence % 3 + 1)),
                    "completed_at": None,
                    "priority": "high" if stage == "proposal" else "critical",
                    "status": "overdue",
                    "notes": "Item vencido para popular a fila de prioridade do CRM.",
                }
            )
        elif stage == "won":
            blueprints.append(
                {
                    "activity_type": "visit",
                    "subject": f"{title} - Pos-venda",
                    "due_at": _iso(now + timedelta(days=5)),
                    "completed_at": None,
                    "priority": "normal",
                    "status": "open",
                    "notes": "Follow-up pos-venda para oportunidades ganhas.",
                }
            )
        else:
            blueprints.append(
                {
                    "activity_type": "follow_up",
                    "subject": f"{title} - Reavaliar perda",
                    "due_at": _iso(now + timedelta(days=14)),
                    "completed_at": None,
                    "priority": "low",
                    "status": "open",
                    "notes": "Revisao futura para oportunidades perdidas.",
                }
            )

        for item_index, blueprint in enumerate(blueprints, start=1):
            external_ref = f"{SEED_TAG}:{sequence}:{item_index}"
            if external_ref in existing_refs:
                continue
            rows.append(
                {
                    "customer_id": customer_id,
                    "opportunity_id": opportunity_id,
                    "owner_sales_rep_id": owner_sales_rep_id,
                    "activity_type": blueprint["activity_type"],
                    "subject": blueprint["subject"],
                    "due_at": blueprint["due_at"],
                    "completed_at": blueprint["completed_at"],
                    "priority": blueprint["priority"],
                    "status": blueprint["status"],
                    "source_system": SEED_SOURCE,
                    "external_ref": external_ref,
                    "notes": blueprint["notes"],
                    "metadata": {"seed_tag": SEED_TAG, "seed_sequence": sequence, "stage": stage},
                    "created_by": created_by,
                }
            )
    return rows


def main() -> None:
    if metas_db._backend_mode() == "sqlite":
        raise RuntimeError("Supabase backend not configured.")
    opportunities, created_by, existing_refs = _fetch_seed_context()
    payload = _build_rows(opportunities, created_by, existing_refs)
    created = metas_db._rest_request(
        "POST",
        "crm_activities",
        payload=payload,
        prefer="return=representation,resolution=merge-duplicates",
    ) or []
    print(
        f"seeded_opportunities={len(opportunities)} existing_seeded={len(existing_refs)} inserted={len(created)} created_by={created_by}"
    )


if __name__ == "__main__":
    main()
