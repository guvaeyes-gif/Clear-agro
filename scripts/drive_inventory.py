from __future__ import annotations

from collections import Counter
from pathlib import Path
import argparse

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "out"
OUT_CSV = OUT_DIR / "drive_inventory.csv"
OUT_MD = OUT_DIR / "drive_inventory_summary.md"

import sys

if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.google_workspace import build_google_service


def _mime_label(mime: str) -> str:
    m = (mime or "").lower()
    if m == "application/vnd.google-apps.folder":
        return "Folder"
    if m == "application/vnd.google-apps.spreadsheet":
        return "Google Sheets"
    if m == "application/vnd.google-apps.document":
        return "Google Docs"
    if m == "application/vnd.google-apps.presentation":
        return "Google Slides"
    if m == "application/pdf":
        return "PDF"
    if "excel" in m or m.endswith("spreadsheetml.sheet"):
        return "Excel"
    return "Outros"


def fetch_inventory(limit: int) -> pd.DataFrame:
    svc = build_google_service("drive", "v3")
    rows: list[dict] = []
    page_token = None
    while len(rows) < limit:
        req = (
            svc.files()
            .list(
                q="trashed=false",
                pageSize=min(100, limit - len(rows)),
                pageToken=page_token,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields=(
                    "nextPageToken,"
                    "files(id,name,mimeType,createdTime,modifiedTime,size,shared,webViewLink,"
                    "parents,owners(displayName,emailAddress))"
                ),
            )
            .execute()
        )
        files = req.get("files", [])
        for f in files:
            owners = f.get("owners") or []
            owner_name = owners[0].get("displayName", "") if owners else ""
            owner_email = owners[0].get("emailAddress", "") if owners else ""
            parents = f.get("parents") or []
            rows.append(
                {
                    "id": f.get("id"),
                    "name": f.get("name"),
                    "mime_type": f.get("mimeType"),
                    "type_label": _mime_label(f.get("mimeType", "")),
                    "created_time": f.get("createdTime"),
                    "modified_time": f.get("modifiedTime"),
                    "size_bytes": pd.to_numeric(f.get("size", None), errors="coerce"),
                    "shared": bool(f.get("shared", False)),
                    "owner_name": owner_name,
                    "owner_email": owner_email,
                    "parent_ids": "|".join(parents),
                    "web_view_link": f.get("webViewLink"),
                }
            )
            if len(rows) >= limit:
                break
        page_token = req.get("nextPageToken")
        if not page_token:
            break

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # resolve nomes das pastas pai (quando possivel)
    folder_map: dict[str, str] = {}
    folder_rows = df[df["mime_type"] == "application/vnd.google-apps.folder"]
    for _, r in folder_rows.iterrows():
        folder_map[str(r["id"])] = str(r["name"])

    unique_parents = set()
    for p in df["parent_ids"].fillna("").tolist():
        for pid in str(p).split("|"):
            pid = pid.strip()
            if pid:
                unique_parents.add(pid)

    for pid in unique_parents:
        if pid in folder_map:
            continue
        try:
            meta = svc.files().get(
                fileId=pid,
                fields="id,name,mimeType",
                supportsAllDrives=True,
            ).execute()
            folder_map[pid] = meta.get("name", pid)
        except Exception:
            folder_map[pid] = pid

    parent_names = []
    for p in df["parent_ids"].fillna("").tolist():
        names = []
        for pid in str(p).split("|"):
            pid = pid.strip()
            if not pid:
                continue
            names.append(folder_map.get(pid, pid))
        parent_names.append(" | ".join(names))
    df["parent_names"] = parent_names
    return df


def build_summary(df: pd.DataFrame) -> str:
    lines = ["# Inventario Drive (Top arquivos)\n"]
    if df.empty:
        lines.append("Sem arquivos retornados.")
        return "\n".join(lines)

    lines.append(f"- Total inventariado: {len(df)}")
    lines.append(f"- Compartilhados: {int(df['shared'].fillna(False).sum())}")
    lines.append("")

    lines.append("## Tipos\n")
    for t, n in Counter(df["type_label"].fillna("Outros").tolist()).most_common():
        lines.append(f"- {t}: {n}")
    lines.append("")

    lines.append("## Donos (top 10)\n")
    owner_counts = (
        df["owner_name"]
        .fillna("")
        .replace("", "Sem dono")
        .value_counts()
        .head(10)
    )
    for owner, n in owner_counts.items():
        lines.append(f"- {owner}: {int(n)}")
    lines.append("")

    lines.append("## Pastas pai mais frequentes (top 10)\n")
    parent_counts = (
        df["parent_names"]
        .fillna("")
        .replace("", "Sem pasta pai")
        .value_counts()
        .head(10)
    )
    for parent, n in parent_counts.items():
        lines.append(f"- {parent}: {int(n)}")
    lines.append("")

    recent = df.sort_values("modified_time", ascending=False).head(10)
    lines.append("## Arquivos mais recentes (top 10)\n")
    for _, r in recent.iterrows():
        lines.append(f"- {r['modified_time']} | {r['name']} | {r['type_label']}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Inventario Google Drive.")
    parser.add_argument("--limit", type=int, default=200, help="Numero maximo de arquivos para inventario.")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = fetch_inventory(limit=max(1, args.limit))
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    OUT_MD.write_text(build_summary(df), encoding="utf-8")
    print(str(OUT_CSV))
    print(str(OUT_MD))
    print(f"TOTAL={len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
