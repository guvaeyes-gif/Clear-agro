import os
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "out" / "base_unificada.xlsx"
OUT_ENR = ROOT / "out" / "oportunidades_enriquecidas.xlsx"
OUT_INS = ROOT / "out" / "insights_executivos.md"

if not BASE.exists():
    # write empty outputs
    pd.DataFrame().to_excel(OUT_ENR, index=False)
    OUT_INS.write_text("Sem base_unificada.xlsx para analisar.", encoding="utf-8")
    raise SystemExit(0)

opps = pd.read_excel(BASE, sheet_name="oportunidades") if "oportunidades" in pd.ExcelFile(BASE).sheet_names else pd.DataFrame()

if opps.empty:
    pd.DataFrame().to_excel(OUT_ENR, index=False)
    OUT_INS.write_text("Sem oportunidades para analisar.", encoding="utf-8")
    raise SystemExit(0)

# basic scoring
opps = opps.copy()
opps["score"] = 0

# rules (light, only if columns exist)
if "data_proximo_passo" in opps.columns:
    opps["score"] += opps["data_proximo_passo"].notna().astype(int) * 20
if "etapa" in opps.columns:
    opps["score"] += opps["etapa"].astype(str).str.contains("proposta|negociacao", case=False, na=False).astype(int) * 20
if "barreira_principal" in opps.columns:
    opps["score"] += opps["barreira_principal"].notna().astype(int) * 10

# alerts
alerts = []
if "data_proximo_passo" in opps.columns:
    alerts.append("Oportunidades sem proximo passo: " + str(opps[opps["data_proximo_passo"].isna()].shape[0]))

# output
opps.to_excel(OUT_ENR, index=False)
OUT_INS.write_text("\n".join(alerts) if alerts else "Sem alertas gerados.", encoding="utf-8")
print(str(OUT_ENR))
