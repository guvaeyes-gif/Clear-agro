from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.data import load_bling_realizado, load_bling_vendor_map


def main() -> None:
    df = load_bling_realizado()
    vmap = load_bling_vendor_map()

    if df.empty or "vendedor_id" not in df.columns:
        print("Nenhum realizado com vendedor_id encontrado.")
        return

    unresolved = df.copy()
    unresolved["vendedor_id"] = unresolved["vendedor_id"].fillna("").astype(str).str.strip()
    unresolved["vendedor"] = unresolved.get("vendedor", "").fillna("").astype(str).str.strip()
    unresolved = unresolved[unresolved["vendedor_id"] != ""]

    if not vmap.empty:
        named_ids = set(
            vmap.loc[vmap["vendedor"].fillna("").astype(str).str.strip() != "", "vendedor_id"]
            .fillna("")
            .astype(str)
            .str.strip()
            .tolist()
        )
        unresolved = unresolved[~unresolved["vendedor_id"].isin(named_ids)]

    summary = (
        unresolved.groupby(["empresa", "vendedor_id"], dropna=False)
        .agg(
            notas=("vendedor_id", "size"),
            receita_total=("receita", "sum"),
            cliente_exemplo=("cliente", "first"),
        )
        .reset_index()
        .sort_values(["notas", "receita_total"], ascending=[False, False])
    )
    summary["vendedor"] = ""
    summary = summary[["vendedor_id", "vendedor", "empresa", "notas", "receita_total", "cliente_exemplo"]]

    out = ROOT / "bling_api" / "vendedores_map_pendentes.csv"
    summary.to_csv(out, index=False, encoding="utf-8-sig")
    print(str(out))
    print(f"IDs pendentes: {len(summary)}")


if __name__ == "__main__":
    main()
