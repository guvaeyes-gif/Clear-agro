from __future__ import annotations

from pathlib import Path
import re
import unicodedata

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "out"
INV_CSV = OUT_DIR / "drive_inventory.csv"
PROP_CSV = OUT_DIR / "drive_organizacao_proposta.csv"
PLAN_MD = OUT_DIR / "drive_migracao_plano.md"


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def _extract_year(text: str) -> str:
    m = re.search(r"(20\d{2})", text or "")
    return m.group(1) if m else "Sem_Ano"


def classify(name: str, parent_names: str, type_label: str) -> tuple[str, str]:
    t = f"{name} {parent_names}".lower()
    if type_label == "Folder":
        return "00_Pastas_Originais", "Pasta mantida para migracao gradual"
    if any(k in t for k in ["dre", "finance", "custos", "preco", "precos", "gastos", "lucros"]):
        return "02_Financeiro", "Keyword financeira"
    if any(k in t for k in ["relatorio", "dashboard", "status", "kpi", "indicador"]):
        return "01_Relatorios", "Keyword relatorio/KPI"
    if any(k in t for k in ["produto", "produtos", "cotacao", "compras", "b2b", "pipeline"]):
        return "03_Comercial_Produtos", "Keyword comercial/produto"
    if any(k in t for k in ["contrato", "juridico", "lgpd", "compliance"]):
        return "04_Legal_Compliance", "Keyword legal/compliance"
    return "99_Arquivo_Geral", "Sem classificacao automatica"


def main() -> int:
    if not INV_CSV.exists():
        raise FileNotFoundError(f"Inventario nao encontrado: {INV_CSV}")

    df = pd.read_csv(INV_CSV)
    if df.empty:
        PROP_CSV.write_text("", encoding="utf-8")
        PLAN_MD.write_text("Sem dados no inventario.", encoding="utf-8")
        return 0

    df["name_norm"] = df["name"].fillna("").map(_norm)
    dup_count = df["name_norm"].value_counts()
    df["possible_duplicate_count"] = df["name_norm"].map(dup_count).fillna(1).astype(int)
    df["year_bucket"] = df["name"].fillna("").map(_extract_year)

    areas: list[str] = []
    reasons: list[str] = []
    proposed_paths: list[str] = []
    priorities: list[str] = []
    for _, r in df.iterrows():
        area, reason = classify(str(r.get("name", "")), str(r.get("parent_names", "")), str(r.get("type_label", "")))
        year = str(r.get("year_bucket", "Sem_Ano"))
        path = f"/ClearAgro/{area}/{year}/{r.get('name', '')}"
        dup_n = int(r.get("possible_duplicate_count", 1))
        shared = bool(r.get("shared", False))
        if dup_n >= 3:
            priority = "P0"
        elif shared:
            priority = "P1"
        else:
            priority = "P2"
        areas.append(area)
        reasons.append(reason)
        proposed_paths.append(path)
        priorities.append(priority)

    df["proposed_area"] = areas
    df["proposed_path"] = proposed_paths
    df["rationale"] = reasons
    df["migration_priority"] = priorities

    cols = [
        "id",
        "name",
        "type_label",
        "owner_name",
        "shared",
        "modified_time",
        "parent_names",
        "possible_duplicate_count",
        "migration_priority",
        "proposed_area",
        "proposed_path",
        "rationale",
        "web_view_link",
    ]
    out_df = df[[c for c in cols if c in df.columns]].copy()
    out_df.to_csv(PROP_CSV, index=False, encoding="utf-8-sig")

    p0 = int((out_df["migration_priority"] == "P0").sum())
    p1 = int((out_df["migration_priority"] == "P1").sum())
    p2 = int((out_df["migration_priority"] == "P2").sum())
    dup_files = int((out_df["possible_duplicate_count"] >= 2).sum())

    lines = [
        "# Plano de Migracao do Drive",
        "",
        "## Estrutura alvo sugerida",
        "- /ClearAgro/01_Relatorios/{ANO}/...",
        "- /ClearAgro/02_Financeiro/{ANO}/...",
        "- /ClearAgro/03_Comercial_Produtos/{ANO}/...",
        "- /ClearAgro/04_Legal_Compliance/{ANO}/...",
        "- /ClearAgro/99_Arquivo_Geral/{ANO}/...",
        "",
        "## Diagnostico rapido",
        f"- Arquivos analisados: {len(out_df)}",
        f"- Possiveis duplicados: {dup_files}",
        f"- Prioridade P0: {p0}",
        f"- Prioridade P1: {p1}",
        f"- Prioridade P2: {p2}",
        "",
        "## Sequencia de migracao segura (sem apagar)",
        "1. Congelar nomenclatura nova por 7 dias e comunicar equipe.",
        "2. Mover primeiro P0 (duplicados) para pastas-alvo com sufixo de revisao.",
        "3. Mover P1 (arquivos compartilhados) com validacao de permissao apos cada lote.",
        "4. Mover P2 em lotes de 20 arquivos por vez.",
        "5. Revisar links quebrados e ajustar atalhos.",
        "6. So apos 30 dias, arquivar pasta legada (sem exclusao permanente).",
        "",
        "## Arquivos gerados",
        f"- Proposta detalhada: {PROP_CSV}",
        f"- Inventario base: {INV_CSV}",
    ]
    PLAN_MD.write_text("\n".join(lines), encoding="utf-8")
    print(str(PROP_CSV))
    print(str(PLAN_MD))
    print(f"P0={p0} P1={p1} P2={p2}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
