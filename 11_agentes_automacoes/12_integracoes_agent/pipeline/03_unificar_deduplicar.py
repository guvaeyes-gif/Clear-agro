import os
import hashlib
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIRS = [
    ROOT / "data" / "planilhas_individuais",
    ROOT / "data" / "cadastros",
    ROOT / "data" / "realizado",
]

OUT_BASE = ROOT / "out" / "base_unificada.xlsx"
OUT_QUAL = ROOT / "out" / "data_quality.md"
OUT_PEND = ROOT / "out" / "pendencias.xlsx"

# master tables
opps = []
acts = []
real = []
tests = []
clients = []
metas = []

pend_cols = []

def hash_id(*parts):
    s = "|".join([str(p) for p in parts if p is not None])
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

# simple loader
def norm_cols(df):
    return [str(c).strip().lower() for c in df.columns]


def get_col(df, name):
    for c in df.columns:
        if str(c).strip().lower() == name:
            return df[c]
    return None


def infer_vendedor_from_filename(path):
    name = Path(path).stem
    name = name.replace("Relatório", "").replace("Relatório", "").replace("Relatorio", "").strip()
    return name if name else None


def find_vendedor_in_sheet(raw):
    labels = {
        "REPRESENTANTE",
        "Data Abertura",
        "Mês",
        "Cliente",
        "Valor (R$)",
        "Status",
        "Data de Fechamento",
        "META",
        "REALIZADO",
        "GAP (%)",
    }
    for i in range(min(6, len(raw))):
        row = raw.iloc[i].tolist()
        if any(isinstance(v, str) and v.strip().upper() == "REPRESENTANTE" for v in row):
            for v in row:
                if isinstance(v, str):
                    s = v.strip()
                    if s and s.upper() != "REPRESENTANTE":
                        return s
        for v in row:
            if isinstance(v, str):
                s = v.strip()
                if s and s not in labels:
                    return s
    return None


def _norm_txt(s):
    import unicodedata
    s = str(s).strip().upper()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s


def find_meta_section(raw):
    # prefer section after "META MENSAL POR VENDEDOR"
    target_row = None
    for i in range(len(raw)):
        row = [_norm_txt(x) if isinstance(x, str) else "" for x in raw.iloc[i].tolist()]
        if any("META MENSAL POR VENDEDOR" in c for c in row):
            target_row = i
            break
    search_range = range(target_row + 1, min(target_row + 5, len(raw))) if target_row is not None else range(len(raw))
    for i in search_range:
        row = [_norm_txt(x) if isinstance(x, str) else "" for x in raw.iloc[i].tolist()]
        if "MES" in row and "META" in row:
            # choose rightmost META after MES
            col_mes = row.index("MES")
            meta_cols = [idx for idx, val in enumerate(row) if val == "META" and idx > col_mes]
            col_meta = meta_cols[-1] if meta_cols else row.index("META")
            return i, col_mes, col_meta
    return None


def month_to_int(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (int, float)):
        m = int(val)
        return m if 1 <= m <= 12 else None
    s = _norm_txt(val).lower()
    mapa = {
        "janeiro": 1, "jan": 1,
        "fevereiro": 2, "fev": 2,
        "marco": 3, "mar": 3,
        "abril": 4, "abr": 4,
        "maio": 5, "mai": 5,
        "junho": 6, "jun": 6,
        "julho": 7, "jul": 7,
        "agosto": 8, "ago": 8,
        "setembro": 9, "set": 9,
        "outubro": 10, "out": 10,
        "novembro": 11, "nov": 11,
        "dezembro": 12, "dez": 12,
    }
    return mapa.get(s)


def find_meta_header(raw):
    best = None
    best_score = -1
    for i in range(len(raw)):
        row = raw.iloc[i].tolist()
        if "META" in row and "REALIZADO" in row:
            col_meta = row.index("META")
            col_real = row.index("REALIZADO")
            score = 0
            for m in range(12):
                r = i + 1 + m
                if r >= len(raw):
                    break
                meta_val = pd.to_numeric(raw.iloc[r, col_meta], errors="coerce")
                real_val = pd.to_numeric(raw.iloc[r, col_real], errors="coerce")
                if not pd.isna(meta_val) or not pd.isna(real_val):
                    score += 1
            if score > best_score:
                best_score = score
                best = (i, col_meta, col_real)
    return best


for d in DATA_DIRS:
    if not d.exists():
        continue
    files = [p for p in d.rglob("*") if p.is_file() and p.suffix.lower() in {".xlsx",".xls",".csv"}]
    for f in files:
        try:
            if f.suffix.lower() == ".csv":
                dfs = {"csv": pd.read_csv(f)}
            else:
                xls = pd.ExcelFile(f)
                dfs = {s: xls.parse(s) for s in xls.sheet_names}
        except Exception as e:
            pend_cols.append({"arquivo": str(f), "aba": "", "coluna": "(erro)", "motivo": str(e)})
            continue

        for sheet, df in dfs.items():
            cols = norm_cols(df)
            # naive type detection
            if any(k in " ".join(cols) for k in ["etapa","oportunidade","pipeline"]):
                # opportunities
                df2 = pd.DataFrame({
                    "cliente": df.get("cliente") if "cliente" in df.columns else None,
                    "vendedor": df.get("vendedor") if "vendedor" in df.columns else None,
                    "etapa": df.get("etapa") if "etapa" in df.columns else None,
                    "data_criacao": df.get("data") if "data" in df.columns else None,
                })
                for _, r in df2.iterrows():
                    oid = hash_id(r.get("cliente"), r.get("etapa"), r.get("data_criacao"))
                    opps.append({"id_oportunidade": oid, **r.to_dict()})
            elif any(k in " ".join(cols) for k in ["visita","ligacao","atividade"]):
                df2 = pd.DataFrame({
                    "data": df.get("data") if "data" in df.columns else None,
                    "cliente": df.get("cliente") if "cliente" in df.columns else None,
                    "tipo": df.get("tipo") if "tipo" in df.columns else None,
                    "resumo": df.get("resumo") if "resumo" in df.columns else None,
                })
                for _, r in df2.iterrows():
                    aid = hash_id(r.get("cliente"), r.get("data"), r.get("tipo"))
                    acts.append({"id_atividade": aid, **r.to_dict()})
            elif any(k in " ".join(cols) for k in ["faturamento","receita","nota","pedido"]):
                df2 = pd.DataFrame({
                    "data": df.get("data") if "data" in df.columns else None,
                    "cliente": df.get("cliente") if "cliente" in df.columns else None,
                    "produto": df.get("produto") if "produto" in df.columns else None,
                    "receita": df.get("receita") if "receita" in df.columns else None,
                })
                for _, r in df2.iterrows():
                    real.append(r.to_dict())
            elif {"vendedor", "mês", "valor (r$)"}.issubset(set(cols)):
                # Banco de dados: ignorado para metas (metas oficiais na Planilha Geral)
                pass
            elif sheet.lower() == "planilha geral":
                # parse realizado de oportunidades (Valor R$) e metas mensais
                try:
                    df_raw = pd.read_excel(f, sheet_name=sheet, header=None)
                    df_pg = pd.read_excel(f, sheet_name=sheet, header=2)
                except Exception:
                    df_raw = df.copy()
                    df_pg = df.copy()
                cols_map = {c: str(c).strip().lower() for c in df_pg.columns}
                data_col = None
                val_col = None
                cli_col = None
                for c, cl in cols_map.items():
                    if "data" in cl and "abertura" in cl:
                        data_col = c
                    if "valor" in cl and "r$" in cl:
                        val_col = c
                    if "cliente" in cl:
                        cli_col = c
                if val_col is not None:
                    vend = find_vendedor_in_sheet(df_raw) or infer_vendedor_from_filename(f)
                    for _, r in df_pg.iterrows():
                        val = r.get(val_col)
                        if pd.isna(val):
                            continue
                        real.append({
                            "data": r.get(data_col) if data_col else None,
                            "cliente": r.get(cli_col) if cli_col else None,
                            "produto": None,
                            "receita": val,
                            "vendedor": vend,
                            "origem": "planilha_geral",
                        })

                # metas mensais
                meta_section = find_meta_section(df_raw)
                if meta_section is not None:
                    idx, col_mes, col_meta = meta_section
                    vend = find_vendedor_in_sheet(df_raw) or infer_vendedor_from_filename(f)
                    for r in range(idx + 1, min(idx + 20, len(df_raw))):
                        mes_val = df_raw.iloc[r, col_mes]
                        meta_val = df_raw.iloc[r, col_meta]
                        m_int = month_to_int(mes_val)
                        if not m_int:
                            continue
                        try:
                            meta_num = float(meta_val)
                        except Exception:
                            continue
                        metas.append({
                            "data": pd.Timestamp(year=2026, month=m_int, day=1),
                            "vendedor": vend,
                            "meta": meta_num,
                            "realizado": None,
                        })
            elif {"nome", "canal", "potencial de venda"}.issubset(set(cols)):
                # clientes/potencial (pipeline)
                vend_name = infer_vendedor_from_filename(f)
                for _, r in df.iterrows():
                    opps.append({
                        "id_oportunidade": hash_id(r.get("NOME"), r.get("CANAL"), r.get("POTENCIAL DE VENDA"), vend_name),
                        "cliente": r.get("NOME"),
                        "canal": r.get("CANAL"),
                        "volume_potencial": r.get("POTENCIAL DE VENDA"),
                        "vendedor": vend_name,
                        "etapa": "Prospeccao",
                    })
            elif any(k in " ".join(cols) for k in ["teste","fabrica"]):
                df2 = pd.DataFrame({
                    "fabrica": df.get("fabrica") if "fabrica" in df.columns else None,
                    "status_teste": df.get("status") if "status" in df.columns else None,
                })
                for _, r in df2.iterrows():
                    tests.append(r.to_dict())
            elif any(k in " ".join(cols) for k in ["cliente","cnpj","ruc"]):
                for _, r in df.iterrows():
                    clients.append(r.to_dict())
            else:
                pend_cols.append({"arquivo": str(f), "aba": sheet, "coluna": "*", "motivo": "tipo nao identificado"})

# build dataframes
opps_df = pd.DataFrame(opps)
acts_df = pd.DataFrame(acts)
real_df = pd.DataFrame(real)
tests_df = pd.DataFrame(tests)
clients_df = pd.DataFrame(clients)
metas_df = pd.DataFrame(metas)

# dedup
if not opps_df.empty:
    opps_df = opps_df.drop_duplicates(subset=["id_oportunidade"])
if not acts_df.empty:
    acts_df = acts_df.drop_duplicates(subset=["id_atividade"])

# export base
with pd.ExcelWriter(OUT_BASE, engine="openpyxl") as writer:
    opps_df.to_excel(writer, index=False, sheet_name="oportunidades")
    acts_df.to_excel(writer, index=False, sheet_name="atividades")
    real_df.to_excel(writer, index=False, sheet_name="realizado")
    tests_df.to_excel(writer, index=False, sheet_name="testes")
    clients_df.to_excel(writer, index=False, sheet_name="clientes_dim")
    if not metas_df.empty:
        metas_df.to_excel(writer, index=False, sheet_name="metas")

# data quality
lines = []
lines.append("# Data Quality")
lines.append(f"- oportunidades: {len(opps_df)}")
lines.append(f"- atividades: {len(acts_df)}")
lines.append(f"- realizado: {len(real_df)}")
lines.append(f"- testes: {len(tests_df)}")
lines.append(f"- clientes_dim: {len(clients_df)}")
lines.append(f"- metas: {len(metas_df)}")
lines.append(f"- pendencias: {len(pend_cols)}")
OUT_QUAL.write_text("\n".join(lines), encoding="utf-8")

# pendencias
pend_df = pd.DataFrame(pend_cols)
with pd.ExcelWriter(OUT_PEND, engine="openpyxl") as writer:
    pend_df.to_excel(writer, index=False, sheet_name="pendencias")

print(str(OUT_BASE))
