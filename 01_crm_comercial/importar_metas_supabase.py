from __future__ import annotations

import argparse
import sys
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.metas_db import create_meta, list_metas, update_meta  # noqa: E402


REQUIRED_COLUMNS = ["ano", "mes", "empresa", "estado", "vendedor_id", "meta_valor"]
OPTIONAL_COLUMNS = ["vendedor", "status", "canal", "cultura", "meta_volume", "observacoes"]
DEFAULT_INPUT = Path(__file__).resolve().parent / "metas_comerciais_template.csv"


def normalize_column(value: object) -> str:
    text = str(value or "").strip().lower()
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    return text.replace(" ", "_")


def load_sheet(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path, encoding="utf-8-sig")
    df = df.copy()
    df.columns = [normalize_column(col) for col in df.columns]
    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Colunas obrigatorias ausentes: {', '.join(missing)}")

    out = df.copy()
    for column in REQUIRED_COLUMNS + OPTIONAL_COLUMNS:
        if column not in out.columns:
            out[column] = ""

    out["ano"] = pd.to_numeric(out["ano"], errors="coerce")
    out["mes"] = pd.to_numeric(out["mes"], errors="coerce")
    out["meta_valor"] = pd.to_numeric(out["meta_valor"], errors="coerce")
    out["meta_volume"] = pd.to_numeric(out["meta_volume"], errors="coerce")

    out["empresa"] = out["empresa"].fillna("").astype(str).str.strip().str.upper()
    out["estado"] = out["estado"].fillna("").astype(str).str.strip().str.upper()
    out["vendedor_id"] = out["vendedor_id"].fillna("").astype(str).str.strip()
    out["vendedor"] = out["vendedor"].fillna("").astype(str).str.strip().str.upper()
    out["status"] = out["status"].fillna("").astype(str).str.strip().str.upper().replace("", "ATIVO")
    out["canal"] = out["canal"].fillna("").astype(str).str.strip()
    out["cultura"] = out["cultura"].fillna("").astype(str).str.strip()
    out["observacoes"] = out["observacoes"].fillna("").astype(str).str.strip()

    out = out[
        out["ano"].notna()
        & out["mes"].notna()
        & out["meta_valor"].notna()
        & out["empresa"].isin(["CZ", "CR"])
        & out["vendedor_id"].ne("")
    ].copy()
    out["ano"] = out["ano"].astype(int)
    out["mes"] = out["mes"].astype(int)
    out["meta_valor"] = out["meta_valor"].astype(float)
    out["meta_volume"] = out["meta_volume"].fillna(0.0).astype(float)

    invalid_month = ~out["mes"].between(1, 12)
    if invalid_month.any():
        rows = ", ".join(str(idx + 2) for idx in out.index[invalid_month])
        raise ValueError(f"Mes invalido nas linhas: {rows}")

    valid_status = {"ATIVO", "PAUSADO", "DESLIGADO", "TRANSFERIDO"}
    bad_status = ~out["status"].isin(valid_status)
    if bad_status.any():
        rows = ", ".join(str(idx + 2) for idx in out.index[bad_status])
        raise ValueError(f"Status invalido nas linhas: {rows}")

    return out.reset_index(drop=True)


def key_tuple(row: pd.Series) -> tuple[Any, ...]:
    return (
        int(row["ano"]),
        int(row["mes"]),
        str(row["empresa"]).strip().upper(),
        str(row["estado"]).strip().upper(),
        str(row["vendedor_id"]).strip(),
        str(row.get("canal", "")).strip().upper(),
        str(row.get("cultura", "")).strip().upper(),
    )


def build_existing_index(years: list[int]) -> dict[tuple[Any, ...], dict[str, Any]]:
    existing = list_metas({"ano": years})
    if existing.empty:
        return {}
    out: dict[tuple[Any, ...], dict[str, Any]] = {}
    for _, row in existing.iterrows():
        if str(row.get("periodo_tipo", "")).upper() != "MONTH":
            continue
        key = (
            int(row.get("ano")),
            int(row.get("mes")),
            str(row.get("empresa", "")).strip().upper(),
            str(row.get("estado", "")).strip().upper(),
            str(row.get("vendedor_id", "")).strip(),
            str(row.get("canal", "")).strip().upper(),
            str(row.get("cultura", "")).strip().upper(),
        )
        out[key] = row.to_dict()
    return out


def sync_rows(df: pd.DataFrame, dry_run: bool) -> tuple[int, int]:
    existing_index = build_existing_index(sorted(df["ano"].dropna().astype(int).unique().tolist()))
    created = 0
    updated = 0

    for _, row in df.iterrows():
        payload = {
            "ano": int(row["ano"]),
            "periodo_tipo": "MONTH",
            "mes": int(row["mes"]),
            "quarter": None,
            "empresa": str(row["empresa"]).strip().upper(),
            "estado": str(row["estado"]).strip().upper(),
            "vendedor_id": str(row["vendedor_id"]).strip(),
            "canal": str(row.get("canal", "")).strip() or None,
            "cultura": str(row.get("cultura", "")).strip() or None,
            "meta_valor": float(row["meta_valor"]),
            "meta_volume": float(row["meta_volume"]) if pd.notna(row["meta_volume"]) else None,
            "status": str(row["status"]).strip().upper(),
            "observacoes": str(row.get("observacoes", "")).strip() or None,
        }
        key = key_tuple(row)
        existing = existing_index.get(key)
        if existing is None:
            created += 1
            if not dry_run:
                create_meta(payload, actor_id="metas_import")
            continue

        updates = {}
        if float(existing.get("meta_valor", 0) or 0) != payload["meta_valor"]:
            updates["meta_valor"] = payload["meta_valor"]
        if str(existing.get("status", "")).strip().upper() != payload["status"]:
            updates["status"] = payload["status"]
        if str(existing.get("observacoes", "") or "").strip() != (payload["observacoes"] or ""):
            updates["observacoes"] = payload["observacoes"]
        if str(existing.get("canal", "") or "").strip() != (payload["canal"] or ""):
            updates["canal"] = payload["canal"]
        if str(existing.get("cultura", "") or "").strip() != (payload["cultura"] or ""):
            updates["cultura"] = payload["cultura"]
        existing_volume = existing.get("meta_volume")
        existing_volume = float(existing_volume) if existing_volume not in (None, "") and pd.notna(existing_volume) else None
        payload_volume = payload["meta_volume"]
        if existing_volume != payload_volume:
            updates["meta_volume"] = payload_volume

        if updates:
            updated += 1
            if not dry_run:
                update_meta(existing["id"], updates, actor_id="metas_import")

    return created, updated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Importa metas mensais da planilha para sales_targets.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="CSV ou XLSX com metas mensais.")
    parser.add_argument("--dry-run", action="store_true", help="Valida e mostra o resumo sem gravar.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    path = Path(args.input)
    if not path.exists():
        raise SystemExit(f"Arquivo nao encontrado: {path}")

    df = validate(load_sheet(path))
    if df.empty:
        raise SystemExit("Nenhuma linha valida para importar.")

    created, updated = sync_rows(df, dry_run=args.dry_run)
    print(f"arquivo={path}")
    print(f"linhas_validas={len(df)}")
    print(f"criadas={created}")
    print(f"atualizadas={updated}")
    print(f"modo={'dry-run' if args.dry_run else 'apply'}")


if __name__ == "__main__":
    main()
