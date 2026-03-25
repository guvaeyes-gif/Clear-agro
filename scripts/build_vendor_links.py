from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.data import load_bling_realizado, load_bling_vendor_map, load_vendor_links


OUT = ROOT / "data" / "vendor_links.csv"


def main() -> None:
    vendor_map = load_bling_vendor_map()
    realized = load_bling_realizado()
    existing = load_vendor_links()

    rows: list[dict[str, str]] = []
    if not vendor_map.empty:
        base = vendor_map.copy()
        base["vendedor_id"] = base["vendedor_id"].fillna("").astype(str).str.strip()
        base["empresa"] = base["empresa"].fillna("").astype(str).str.strip()
        for _, row in base.iterrows():
            if not row["vendedor_id"]:
                continue
            rows.append(
                {
                    "vendedor_id": row["vendedor_id"],
                    "nome_meta": "",
                    "nome_exibicao": str(row.get("vendedor") or "").strip(),
                    "empresa": row["empresa"],
                    "status": "ativo",
                }
            )

    if not realized.empty and {"vendedor_id", "vendedor"}.issubset(realized.columns):
        rr = realized.copy()
        rr["vendedor_id"] = rr["vendedor_id"].fillna("").astype(str).str.strip()
        rr["vendedor"] = rr["vendedor"].fillna("").astype(str).str.strip()
        rr["empresa"] = rr.get("empresa", "").fillna("").astype(str).str.strip()
        rr = rr[
            (rr["vendedor_id"] != "")
            & (rr["vendedor"] != "")
            & rr["vendedor"].ne("SEM_VENDEDOR")
            & rr["vendedor"].ne(rr["vendedor_id"])
        ]
        if not rr.empty:
            ranked = (
                rr.groupby(["vendedor_id", "vendedor", "empresa"], dropna=False)
                .size()
                .reset_index(name="cnt")
                .sort_values(["vendedor_id", "cnt", "vendedor"], ascending=[True, False, True])
                .drop_duplicates(subset=["vendedor_id"], keep="first")
            )
            for _, row in ranked.iterrows():
                rows.append(
                    {
                        "vendedor_id": row["vendedor_id"],
                        "nome_meta": row["vendedor"],
                        "nome_exibicao": row["vendedor"],
                        "empresa": row["empresa"],
                        "status": "ativo",
                    }
                )

    built = pd.DataFrame(rows)
    if built.empty:
        built = pd.DataFrame(columns=["vendedor_id", "nome_meta", "nome_exibicao", "empresa", "status"])

    if not existing.empty:
        merged = built.merge(
            existing.rename(
                columns={
                    "nome_meta": "__nome_meta_existing",
                    "nome_exibicao": "__nome_exibicao_existing",
                    "empresa": "__empresa_existing",
                    "status": "__status_existing",
                }
            ),
            on="vendedor_id",
            how="outer",
        )
        for col, existing_col in [
            ("nome_meta", "__nome_meta_existing"),
            ("nome_exibicao", "__nome_exibicao_existing"),
            ("empresa", "__empresa_existing"),
            ("status", "__status_existing"),
        ]:
            merged[col] = merged.get(col, "").fillna("").astype(str).str.strip().replace("nan", "")
            merged[existing_col] = merged.get(existing_col, "").fillna("").astype(str).str.strip().replace("nan", "")
            merged[col] = merged[existing_col].mask(merged[existing_col].eq(""), merged[col])
        built = merged[["vendedor_id", "nome_meta", "nome_exibicao", "empresa", "status"]].copy()

    built["vendedor_id"] = built["vendedor_id"].fillna("").astype(str).str.strip()
    built["nome_meta"] = built["nome_meta"].fillna("").astype(str).str.strip().replace("nan", "")
    built["nome_exibicao"] = built["nome_exibicao"].fillna("").astype(str).str.strip().replace("nan", "")
    built["empresa"] = built["empresa"].fillna("").astype(str).str.strip()
    built["status"] = built["status"].fillna("").astype(str).str.strip().replace("", "ativo")
    built["nome_meta"] = built["nome_meta"].mask(built["nome_meta"].eq(""), built["nome_exibicao"])
    built = built[built["vendedor_id"] != ""].copy()
    built = built.drop_duplicates(subset=["vendedor_id"], keep="first")
    built = built.sort_values(["empresa", "vendedor_id"]).reset_index(drop=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    built.to_csv(OUT, index=False, encoding="utf-8-sig")
    print(str(OUT))
    print(f"Registros: {len(built)}")


if __name__ == "__main__":
    main()
