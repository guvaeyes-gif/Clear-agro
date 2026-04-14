from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.metas_db import build_quarter_rollups_from_monthly

OUT_DIR = ROOT / "templates"
OUT_XLSX = OUT_DIR / "metas_comerciais_modelo_google_sheets.xlsx"
OUT_CSV = OUT_DIR / "metas_comerciais_modelo_google_sheets.csv"
BASE_TARGETS = ROOT / "out" / "base_unificada.xlsx"
MONTHLY_SALES = ROOT / "out" / "vendas_mensais_2026_por_vendedor.csv"
VENDOR_LINKS = ROOT / "data" / "vendor_links.csv"

TARGET_COLUMNS = [
    "ano",
    "periodo_tipo",
    "mes",
    "quarter",
    "estado",
    "vendedor_id",
    "empresa",
    "canal",
    "cultura",
    "meta_valor",
    "meta_volume",
    "realizado_valor",
    "realizado_volume",
    "status",
    "observacoes",
]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, encoding="utf-8-sig")


def _read_xlsx_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    xls = pd.ExcelFile(path)
    if sheet_name not in xls.sheet_names:
        return pd.DataFrame()
    return pd.read_excel(path, sheet_name=sheet_name)


def _month_num(value: object) -> int | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return int(pd.to_datetime(txt, errors="coerce").month)
    except Exception:
        return None


def _quarter_from_month(month_num: int | None) -> int | None:
    if month_num is None:
        return None
    return ((int(month_num) - 1) // 3) + 1


def build_targets_template() -> pd.DataFrame:
    sales = _read_csv(MONTHLY_SALES)
    if sales.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    sales = sales.copy()
    sales.columns = [str(c).strip().lower() for c in sales.columns]
    for column in ["empresa", "mes", "vendedor", "receita"]:
        if column not in sales.columns:
            sales[column] = ""

    sales = sales[sales["vendedor"].fillna("").astype(str).str.strip().ne("0")].copy()
    sales["mes"] = sales["mes"].astype(str).str.strip()
    sales["mes_num"] = sales["mes"].map(_month_num)
    sales = sales[sales["mes_num"].notna()].copy()
    sales["ano"] = pd.to_numeric(sales["mes"].str.slice(0, 4), errors="coerce")
    sales["quarter"] = sales["mes_num"].map(_quarter_from_month)
    sales["receita"] = pd.to_numeric(sales["receita"], errors="coerce").fillna(0)
    sales["meta_valor"] = (sales["receita"] * 1.15).round(-2)

    out = pd.DataFrame(
        {
            "ano": sales["ano"].astype(int),
            "periodo_tipo": "MONTH",
            "mes": sales["mes_num"].astype(int),
            "quarter": pd.NA,
            "estado": "PR",
            "vendedor_id": sales["vendedor"].astype(str).str.strip(),
            "empresa": sales["empresa"].astype(str).str.strip().str.upper(),
            "canal": "",
            "cultura": "",
            "meta_valor": sales["meta_valor"],
            "meta_volume": pd.NA,
            "realizado_valor": pd.NA,
            "realizado_volume": pd.NA,
            "status": "ATIVO",
            "observacoes": "EXEMPLO GERADO A PARTIR DE VENDAS 2026",
        }
    )
    return out[TARGET_COLUMNS].head(60).reset_index(drop=True)


def build_current_targets_reference() -> pd.DataFrame:
    df = _read_xlsx_sheet(BASE_TARGETS, "metas")
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df["ano"] = df["data"].dt.year
        df["mes"] = df["data"].dt.month
        df["quarter"] = ((df["mes"] - 1) // 3) + 1
    return df


def build_sales_reference() -> pd.DataFrame:
    sales = _read_csv(MONTHLY_SALES)
    if sales.empty:
        return pd.DataFrame()
    sales = sales.copy()
    sales.columns = [str(c).strip().lower() for c in sales.columns]
    sales["mes"] = sales["mes"].astype(str)
    sales["mes_num"] = sales["mes"].map(_month_num)
    sales["quarter"] = sales["mes_num"].map(_quarter_from_month)
    sales["receita"] = pd.to_numeric(sales.get("receita"), errors="coerce").fillna(0)
    sales["meta_sugerida_115pct"] = (sales["receita"] * 1.15).round(-2)
    return sales[["empresa", "mes", "quarter", "vendedor", "receita", "meta_sugerida_115pct"]].head(200)


def build_vendor_map_reference() -> pd.DataFrame:
    df = _read_csv(VENDOR_LINKS)
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    for column in ["vendedor_id", "nome_meta", "nome_exibicao", "empresa", "status"]:
        if column not in df.columns:
            df[column] = ""
    return df[["vendedor_id", "nome_meta", "nome_exibicao", "empresa", "status"]].drop_duplicates()


def build_instructions() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ["1", "Edite apenas a aba metas."],
            ["2", "Preencha uma linha por meta mensal por vendedor."],
            ["3", "quarter pode ficar em branco; o importador gera o fechamento trimestral."],
            ["4", "estado precisa ter 2 letras, por exemplo PR ou SC."],
            ["5", "vendedor_id pode ser o codigo do vendedor ou o nome padronizado usado hoje."],
            ["6", "Depois clique em Validar planilha compartilhada no dashboard."],
            ["7", "Se estiver ok, clique em Sincronizar agora."],
            ["8", "A aba metas_trimestrais mostra o somatorio automatico de 3 meses por vendedor."],
        ],
        columns=["passo", "orientacao"],
    )


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    metas = build_targets_template()
    quarter_suggestions = build_quarter_rollups_from_monthly(metas)
    referencia = build_current_targets_reference()
    vendas = build_sales_reference()
    vendedores = build_vendor_map_reference()
    instrucoes = build_instructions()

    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
        metas.to_excel(writer, index=False, sheet_name="metas")
        if not quarter_suggestions.empty:
            quarter_suggestions.to_excel(writer, index=False, sheet_name="metas_trimestrais")
        if not referencia.empty:
            referencia.to_excel(writer, index=False, sheet_name="metas_atual")
        if not vendas.empty:
            vendas.to_excel(writer, index=False, sheet_name="referencia_vendas")
        if not vendedores.empty:
            vendedores.to_excel(writer, index=False, sheet_name="vendedores")
        instrucoes.to_excel(writer, index=False, sheet_name="instrucoes")

    metas.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"Gerado: {OUT_XLSX}")
    print(f"Gerado: {OUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
