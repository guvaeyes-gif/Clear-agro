import os
import json
import hashlib
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIRS = [
    ROOT / "data" / "planilhas_individuais",
    ROOT / "data" / "cadastros",
    ROOT / "data" / "realizado",
]
OUT = ROOT / "out" / "dicionario_dados.md"

SYN = {
    "cliente": ["cliente", "razao_social", "nome_cliente", "nome"],
    "cnpj_ruc": ["cnpj", "ruc", "documento"],
    "vendedor": ["vendedor", "consultor", "representante"],
    "etapa": ["etapa", "fase", "status_pipeline"],
    "probabilidade": ["prob", "probabilidade"],
    "data_criacao": ["data_criacao", "criado_em", "data"],
    "data_proximo_passo": ["proximo_passo_data", "data_followup"],
    "proximo_passo": ["proximo_passo", "proxima_acao"],
    "canal": ["canal", "segmento"],
    "receita": ["receita", "faturamento", "valor"],
}

lines = []
lines.append("# Dicionario de Dados (modelo mestre)\n")
lines.append("## Sinonimos detectados (fixo)\n")
for k, v in SYN.items():
    lines.append(f"- {k}: {v}")

# Append inventory of detected columns
lines.append("\n## Inventario de colunas encontradas\n")
found_any = False
for d in DATA_DIRS:
    if not d.exists():
        lines.append(f"- Diretorio nao encontrado: {d}")
        continue
    files = [p for p in d.rglob("*") if p.is_file() and p.suffix.lower() in {".xlsx",".xls",".csv"}]
    if not files:
        lines.append(f"- Nenhum arquivo em: {d}")
        continue
    found_any = True
    for f in files:
        lines.append(f"### {f}")
        try:
            if f.suffix.lower() == ".csv":
                df = pd.read_csv(f, nrows=1)
                lines.append(f"- Colunas: {list(df.columns)}")
            else:
                xls = pd.ExcelFile(f)
                for sheet in xls.sheet_names:
                    df = xls.parse(sheet, nrows=1)
                    lines.append(f"- Aba: {sheet} | Colunas: {list(df.columns)}")
        except Exception as e:
            lines.append(f"- Erro ao ler: {e}")

if not found_any:
    lines.append("\nObservacao: sem dados locais. Coloque arquivos em ./data/planilhas_individuais, ./data/cadastros, ./data/realizado")

OUT.write_text("\n".join(lines), encoding="utf-8")
print(str(OUT))
