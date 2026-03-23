import os
import json
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIRS = {
    "planilhas_individuais": ROOT / "data" / "planilhas_individuais",
    "cadastros": ROOT / "data" / "cadastros",
    "realizado": ROOT / "data" / "realizado",
}

OUT = ROOT / "out" / "inventario_planilhas.md"

KEYWORDS = {
    "pipeline": ["pipeline", "etapa", "oportunidade", "probabilidade", "lead", "negociacao"],
    "atividades": ["visita", "ligacao", "reuniao", "follow", "atividade"],
    "vendas": ["venda", "pedido", "faturamento", "receita", "nota"],
    "testes_fabricas": ["teste", "fabrica", "lote", "qualidade"],
    "cadastro_cliente": ["cliente", "cnpj", "ruc", "endereco", "contato"],
}

EXTS = {".xlsx", ".xls", ".csv"}


def infer_type(cols):
    cols_l = " ".join([str(c).lower() for c in cols])
    for t, keys in KEYWORDS.items():
        if any(k in cols_l for k in keys):
            return t
    return "desconhecido"


def read_sample(path, sheet=None):
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, nrows=10)
        return df
    else:
        df = pd.read_excel(path, sheet_name=sheet, nrows=10)
        return df


lines = []
lines.append("# Inventario de planilhas\n")

for group, d in DATA_DIRS.items():
    lines.append(f"## {group}\n")
    if not d.exists():
        lines.append(f"- Diretorio nao encontrado: {d}\n")
        continue

    files = [p for p in d.rglob("*") if p.is_file() and p.suffix.lower() in EXTS]
    if not files:
        lines.append("- Nenhum arquivo encontrado.\n")
        continue

    for f in files:
        lines.append(f"### Arquivo: {f}\n")
        if f.suffix.lower() == ".csv":
            try:
                df = read_sample(f)
                cols = list(df.columns)
                t = infer_type(cols)
                lines.append(f"- Aba: (csv)")
                lines.append(f"- Colunas: {cols}")
                lines.append(f"- Tipo provavel: {t}")
                lines.append("- Amostra (10 linhas):")
                lines.append(df.to_markdown(index=False))
            except Exception as e:
                lines.append(f"- Erro ao ler: {e}")
        else:
            try:
                xls = pd.ExcelFile(f)
                for sheet in xls.sheet_names:
                    try:
                        df = read_sample(f, sheet=sheet)
                        cols = list(df.columns)
                        t = infer_type(cols)
                        lines.append(f"- Aba: {sheet}")
                        lines.append(f"  - Colunas: {cols}")
                        lines.append(f"  - Tipo provavel: {t}")
                        lines.append("  - Amostra (10 linhas):")
                        lines.append(df.to_markdown(index=False))
                    except Exception as e:
                        lines.append(f"  - Erro ao ler aba {sheet}: {e}")
            except Exception as e:
                lines.append(f"- Erro ao abrir: {e}")
        lines.append("")

OUT.write_text("\n".join(lines), encoding="utf-8")
print(str(OUT))
