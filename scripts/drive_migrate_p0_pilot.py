from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "out"
PROPOSAL_CSV = OUT_DIR / "drive_organizacao_proposta.csv"
LOG_CSV = OUT_DIR / "drive_migracao_p0_log.csv"

import sys

if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.google_workspace import build_google_service


def ensure_folder(
    svc: Any,
    name: str,
    parent_id: str | None = None,
) -> str:
    escaped_name = name.replace("'", "\\'")
    q_parts = [
        "trashed=false",
        "mimeType='application/vnd.google-apps.folder'",
        f"name='{escaped_name}'",
    ]
    if parent_id:
        q_parts.append(f"'{parent_id}' in parents")
    q = " and ".join(q_parts)
    res = svc.files().list(
        q=q,
        fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        pageSize=10,
    ).execute()
    files = res.get("files", [])
    if files:
        return files[0]["id"]

    body = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        body["parents"] = [parent_id]
    created = svc.files().create(
        body=body,
        fields="id,name",
        supportsAllDrives=True,
    ).execute()
    return created["id"]


def parse_dest(path_text: str) -> list[str]:
    parts = [p for p in str(path_text).split("/") if p]
    if not parts:
        return []
    # remove prefix "ClearAgro" da proposta e usar raiz piloto
    if parts and parts[0].lower() == "clearagro":
        parts = parts[1:]
    return parts


def main() -> int:
    parser = argparse.ArgumentParser(description="Migracao piloto por prioridade no Google Drive (sem pastas).")
    parser.add_argument("--apply", action="store_true", help="Executa movimento real. Sem isso, faz dry-run.")
    parser.add_argument("--limit", type=int, default=20, help="Limite de itens P0 processados.")
    parser.add_argument(
        "--priority",
        choices=["P0", "P1", "P2"],
        default="P0",
        help="Prioridade a migrar no lote.",
    )
    args = parser.parse_args()

    if not PROPOSAL_CSV.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {PROPOSAL_CSV}")

    df = pd.read_csv(PROPOSAL_CSV)
    if df.empty:
        print("Sem dados para migrar.")
        return 0

    # Pilotar apenas arquivos (nao pasta) da prioridade solicitada
    batch = df[(df["migration_priority"] == args.priority) & (df["type_label"] != "Folder")].copy()
    batch = batch.head(max(1, args.limit))
    if batch.empty:
        print(f"Sem arquivos {args.priority} elegiveis para piloto.")
        return 0

    svc = build_google_service("drive", "v3")
    root_id = ensure_folder(svc, "ClearAgro_MigracaoPiloto")

    logs: list[dict[str, Any]] = []
    moved = 0
    for _, r in batch.iterrows():
        file_id = str(r["id"])
        file_name = str(r["name"])
        proposed_path = str(r.get("proposed_path", ""))
        parent_names = str(r.get("parent_names", ""))
        dest_parts = parse_dest(proposed_path)

        current_meta = svc.files().get(
            fileId=file_id,
            fields="id,name,parents,mimeType",
            supportsAllDrives=True,
        ).execute()
        current_parents = current_meta.get("parents", []) or []

        parent_id = root_id
        for part in dest_parts[:-1]:
            parent_id = ensure_folder(svc, part, parent_id=parent_id)

        status = "DRY_RUN"
        err = ""
        if args.apply:
            try:
                svc.files().update(
                    fileId=file_id,
                    addParents=parent_id,
                    removeParents=",".join(current_parents) if current_parents else None,
                    fields="id,parents",
                    supportsAllDrives=True,
                ).execute()
                status = "MOVED"
                moved += 1
            except Exception as exc:  # nosec B110
                status = "ERROR"
                err = str(exc)

        logs.append(
            {
                "file_id": file_id,
                "file_name": file_name,
                "old_parent_ids": "|".join(current_parents),
                "old_parent_names": parent_names,
                "target_parent_id": parent_id,
                "target_path": f"/ClearAgro_MigracaoPiloto/{'/'.join(dest_parts[:-1])}",
                "status": status,
                "error": err,
            }
        )

    pd.DataFrame(logs).to_csv(LOG_CSV, index=False, encoding="utf-8-sig")
    print(str(LOG_CSV))
    print(f"PRIORITY={args.priority} FILES={len(batch)} MOVED={moved} APPLY={args.apply}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
