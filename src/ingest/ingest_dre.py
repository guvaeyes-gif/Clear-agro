from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils import ensure_dir, normalize_columns, save_quality_log, stable_hash
RAW_DIR = ROOT / "data" / "raw" / "dre"
STAGING_PATH = ROOT / "data" / "staging" / "stg_dre.csv"
QUALITY_PATH = ROOT / "data" / "quality" / "dre_ingest_report.json"

TARGET_HINTS = {
    "data": ["data", "date", "competencia", "mes", "periodo"],
    "valor": ["valor", "amount", "total", "saldo"],
    "conta": ["conta", "account", "codigo_conta", "descricao_conta"],
    "centro": ["centro", "cost_center", "centro_de_custo"],
    "pais": ["pais", "country"],
}


def _suggest_mapping(columns: list[str]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for target, hints in TARGET_HINTS.items():
        match = next((c for c in columns if any(h in c for h in hints)), None)
        out[target] = match
    return out


def _read_any(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    return pd.read_csv(path)


def main() -> int:
    ensure_dir(STAGING_PATH.parent)
    files = sorted([p for p in RAW_DIR.glob("**/*") if p.suffix.lower() in {".csv", ".xlsx", ".xls"}])

    frames = []
    for file in files:
        df = _read_any(file)
        df = normalize_columns(df)
        df["source"] = "dre"
        df["source_file"] = str(file.relative_to(ROOT))
        frames.append(df)

    if not frames:
        save_quality_log(
            QUALITY_PATH,
            {
                "status": "warning",
                "message": "No DRE raw files found",
                "raw_dir": str(RAW_DIR),
            },
        )
        pd.DataFrame().to_csv(STAGING_PATH, index=False)
        print(f"{STAGING_PATH} (empty)")
        return 0

    df_all = pd.concat(frames, ignore_index=True)
    mapping = _suggest_mapping(list(df_all.columns))

    if mapping.get("data"):
        df_all["data"] = pd.to_datetime(df_all[mapping["data"]], errors="coerce")
    if mapping.get("valor"):
        df_all["valor"] = pd.to_numeric(df_all[mapping["valor"]], errors="coerce")
    if mapping.get("conta"):
        df_all["conta"] = df_all[mapping["conta"]].astype(str)
    if mapping.get("centro"):
        df_all["centro"] = df_all[mapping["centro"]].astype(str)
    if mapping.get("pais"):
        df_all["country"] = df_all[mapping["pais"]].astype(str).str.upper()
    else:
        df_all["country"] = "BR"

    df_all["raw_id"] = df_all.index.astype(str)
    df_all["hash"] = df_all.apply(
        lambda r: stable_hash(
            [r.get("source"), r.get("source_file"), r.get("raw_id"), r.get("data"), r.get("valor")]
        ),
        axis=1,
    )

    df_all.to_csv(STAGING_PATH, index=False)

    save_quality_log(
        QUALITY_PATH,
        {
            "status": "ok",
            "rows": int(len(df_all)),
            "files": [str(f.relative_to(ROOT)) for f in files],
            "suggested_mapping": mapping,
            "null_data": int(df_all["data"].isna().sum()) if "data" in df_all else None,
            "null_valor": int(df_all["valor"].isna().sum()) if "valor" in df_all else None,
        },
    )
    print(str(STAGING_PATH))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
