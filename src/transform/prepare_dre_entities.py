from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW_DRE = ROOT / "data" / "raw" / "dre" / "DRE_GRUPO_CZ_CLEAR_2025_latest.xlsx"
OUT_DIR = ROOT / "data" / "staging"


def _norm(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip().lower().replace(" ", "_") for c in out.columns]
    return out


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    return _norm(df)


def _load_sheet(path: Path, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet)
    df = _clean_df(df)
    df["source_sheet"] = sheet
    df["source_file"] = str(path.relative_to(ROOT))
    return df


def _find_sheet(sheet_names: list[str], pattern: str, fallback: str | None = None) -> str | None:
    rgx = re.compile(pattern, flags=re.IGNORECASE)
    for s in sheet_names:
        if rgx.search(s):
            return s
    return fallback


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not RAW_DRE.exists():
        print(f"Missing raw DRE file: {RAW_DRE}")
        return 1

    xls = pd.ExcelFile(RAW_DRE)
    sheets = xls.sheet_names

    sheet_empresa = _find_sheet(sheets, r"dre_grupo|grupo|consolid") or sheets[0]
    sheet_cr = _find_sheet(sheets, r"clear|\bcr\b")
    sheet_cz = _find_sheet(sheets, r"\bcz\b")

    if sheet_cr is None and len(sheets) >= 2:
        sheet_cr = sheets[1]
    if sheet_cz is None and len(sheets) >= 3:
        sheet_cz = sheets[2]

    df_empresa = _load_sheet(RAW_DRE, sheet_empresa)
    df_empresa["empresa"] = "EMPRESA"

    df_cr = _load_sheet(RAW_DRE, sheet_cr) if sheet_cr else pd.DataFrame()
    if not df_cr.empty:
        df_cr["empresa"] = "CR"

    df_cz = _load_sheet(RAW_DRE, sheet_cz) if sheet_cz else pd.DataFrame()
    if not df_cz.empty:
        df_cz["empresa"] = "CZ"

    out_empresa = OUT_DIR / "stg_dre_empresa.csv"
    out_cr = OUT_DIR / "stg_dre_cr.csv"
    out_cz = OUT_DIR / "stg_dre_cz.csv"
    out_parallel = OUT_DIR / "stg_dre_parallel.csv"

    df_empresa.to_csv(out_empresa, index=False)
    (df_cr if not df_cr.empty else pd.DataFrame()).to_csv(out_cr, index=False)
    (df_cz if not df_cz.empty else pd.DataFrame()).to_csv(out_cz, index=False)

    frames = [d for d in [df_cr, df_cz, df_empresa] if not d.empty]
    if frames:
        pd.concat(frames, ignore_index=True).to_csv(out_parallel, index=False)
    else:
        pd.DataFrame().to_csv(out_parallel, index=False)

    print(f"empresa_sheet={sheet_empresa} rows={len(df_empresa)} -> {out_empresa}")
    print(f"cr_sheet={sheet_cr} rows={len(df_cr)} -> {out_cr}")
    print(f"cz_sheet={sheet_cz} rows={len(df_cz)} -> {out_cz}")
    print(f"parallel_rows={sum(len(d) for d in frames)} -> {out_parallel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
