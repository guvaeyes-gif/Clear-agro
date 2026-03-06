from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils import ensure_dir, normalize_columns, save_quality_log, stable_hash
RAW_DIR = ROOT / "data" / "raw" / "banks"
STAGING_PATH = ROOT / "data" / "staging" / "stg_banks.csv"
QUALITY_PATH = ROOT / "data" / "quality" / "banks_ingest_report.json"

TARGET_HINTS = {
    "data": ["data", "date", "lancamento", "posted"],
    "valor": ["valor", "amount", "credito", "debito", "saldo"],
    "descricao": ["descricao", "historico", "memo", "detalhe"],
    "entidade": ["beneficiario", "favorecido", "cliente", "fornecedor"],
}


def _suggest_mapping(columns: list[str]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for target, hints in TARGET_HINTS.items():
        out[target] = next((c for c in columns if any(h in c for h in hints)), None)
    return out


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def main() -> int:
    ensure_dir(STAGING_PATH.parent)
    csv_files = sorted([p for p in RAW_DIR.glob("**/*.csv")])
    ofx_files = sorted([p for p in RAW_DIR.glob("**/*.ofx")])

    frames = []
    for file in csv_files:
        df = _read_csv(file)
        df = normalize_columns(df)
        df["source"] = "banks"
        df["source_file"] = str(file.relative_to(ROOT))
        frames.append(df)

    if not frames:
        save_quality_log(
            QUALITY_PATH,
            {
                "status": "warning",
                "message": "No bank CSV files found (OFX parser placeholder only)",
                "raw_dir": str(RAW_DIR),
                "ofx_files_detected": [str(f.relative_to(ROOT)) for f in ofx_files],
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
    if mapping.get("descricao"):
        df_all["descricao"] = df_all[mapping["descricao"]].astype(str)
    if mapping.get("entidade"):
        df_all["entidade"] = df_all[mapping["entidade"]].astype(str)
    else:
        df_all["entidade"] = ""

    df_all["country"] = "BR"
    df_all["raw_id"] = df_all.index.astype(str)
    df_all["hash"] = df_all.apply(
        lambda r: stable_hash([r.get("source"), r.get("source_file"), r.get("raw_id"), r.get("valor")]),
        axis=1,
    )

    df_all.to_csv(STAGING_PATH, index=False)

    save_quality_log(
        QUALITY_PATH,
        {
            "status": "ok",
            "rows": int(len(df_all)),
            "csv_files": [str(f.relative_to(ROOT)) for f in csv_files],
            "ofx_files_detected": [str(f.relative_to(ROOT)) for f in ofx_files],
            "suggested_mapping": mapping,
            "duplicate_hashes": int(df_all.duplicated("hash").sum()),
        },
    )
    print(str(STAGING_PATH))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
