from __future__ import annotations

import argparse
import json
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ADMIN_DEFAULT_CATEGORY = "Fixo - G&A"
ADMIN_DEFAULT_SUBCATEGORY = "Administrativo geral"
REVIEW_CATEGORY = "A revisar - Governanca"
REVIEW_SUBCATEGORY = "Classificacao ambigua"
PENDING_CATEGORY = "A revisar - Governanca"
PENDING_SUBCATEGORY = "Nome do fornecedor pendente"


def load_config(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8-sig")
    if path.suffix.lower() == ".json":
        return json.loads(text)
    try:
        import yaml  # type: ignore

        return yaml.safe_load(text)
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("YAML config requires PyYAML (`pip install pyyaml`) or use .json config") from exc


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.lower().replace("/", " ").replace("-", " ").split())


def match_any(text: str, keywords: list[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def detect_document_type(numero_documento: str) -> str:
    digits = "".join(ch for ch in str(numero_documento or "") if ch.isdigit())
    if len(digits) == 11:
        return "pf"
    if len(digits) == 14:
        return "pj"
    return "unknown"


def looks_like_company(text: str) -> bool:
    company_tokens = [
        " ltda",
        " eireli",
        " me",
        " epp",
        " sa",
        " s a ",
        " industria",
        " comercio",
        " servicos",
        " servico",
        " consultoria",
        " associacao",
        " cooperativa",
        " banco",
        " corretora",
        " quimica",
        " fertilizantes",
        " embalagens",
        " plast",
        " transport",
        " locadora",
        " grafica",
        " papelaria",
        " tecnologia",
        " software",
    ]
    padded = f" {text} "
    return match_any(padded, company_tokens)


def classify_supplier(name: str, doc_type: str) -> dict[str, str]:
    normalized = normalize_text(name)

    if "fornecedor pendente" in normalized or normalized == "sem contato":
        return {
            "categoria_mckinsey": PENDING_CATEGORY,
            "subcategoria_mckinsey": PENDING_SUBCATEGORY,
            "status_mapeamento": "pendente_nome_contato",
            "regra_classificacao": "pendencia_nome_contato",
            "confianca_classificacao": "baixa",
            "motivo_governanca": "contato sem nome validado",
        }

    if match_any(normalized, ["google", "meta", "facebook", "instagram", "linkedin", "tiktok", "ads"]):
        return {
            "categoria_mckinsey": "Variavel - Marketing",
            "subcategoria_mckinsey": "Aquisicao e midia",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "marketing_ads",
            "confianca_classificacao": "alta",
            "motivo_governanca": "",
        }

    if "genetica tecnologias ambientais" in normalized:
        return {
            "categoria_mckinsey": "Variavel - Insumos",
            "subcategoria_mckinsey": "Quimicos e materias-primas",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "insumos_override_genetica",
            "confianca_classificacao": "alta",
            "motivo_governanca": "",
        }

    if match_any(
        normalized,
        [
            "frete",
            "transport",
            "logistica",
            "correio",
            "jadlog",
            "distribuicao",
            "rodoviario",
            "rodoviarios",
            "motoboy",
            "entrega",
        ],
    ):
        return {
            "categoria_mckinsey": "Variavel - Logistica",
            "subcategoria_mckinsey": "Frete e distribuicao",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "logistica_frete",
            "confianca_classificacao": "alta",
            "motivo_governanca": "",
        }

    if match_any(
        normalized,
        [
            "receita federal",
            "sefaz",
            "darf",
            "simples nacional",
            "gps",
            "inss",
            "fgts",
            "imposto",
            "tribut",
            "icms",
            "pis",
            "cofins",
            "iptu",
        ],
    ):
        return {
            "categoria_mckinsey": "Fixo - Tributos",
            "subcategoria_mckinsey": "Impostos e encargos",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "tributos_obrigacoes",
            "confianca_classificacao": "alta",
            "motivo_governanca": "",
        }

    if match_any(
        normalized,
        [
            "vale alimentacao",
            "vale refeicao",
            "vale transporte",
            "plano de saude",
            "unimed",
            "odonto",
            "beneficio",
            "cesta basica",
        ],
    ):
        return {
            "categoria_mckinsey": "Fixo - Pessoal",
            "subcategoria_mckinsey": "Beneficios",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "pessoal_beneficios",
            "confianca_classificacao": "alta",
            "motivo_governanca": "",
        }

    if match_any(
        normalized,
        [
            "santander",
            "sicredi",
            "itau",
            "caixa economica",
            "caixa federal",
            "sicoob",
            "uniprime",
            "unicred",
            "safra",
            "banco ",
            "capital de giro",
            "cap giro",
            "garantida",
            "antecipacao",
            "antecipa",
            "descontadas",
            "dpl",
            "financiamento",
            "transferencia",
            "corretora de seguros",
            "seguros",
            "juros",
        ],
    ):
        return {
            "categoria_mckinsey": "Fixo - Financeiro",
            "subcategoria_mckinsey": "Juros e operacoes financeiras",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "financeiro_bancos",
            "confianca_classificacao": "alta",
            "motivo_governanca": "",
        }

    if match_any(
        normalized,
        [
            "software",
            "sistema",
            "saas",
            "aws",
            "cloud",
            "internet",
            "locaweb",
            "hostgator",
            "microsoft",
            "google workspace",
            "zoom",
            "tangerino",
            "tecnologia",
        ],
    ):
        return {
            "categoria_mckinsey": "Fixo - Tecnologia",
            "subcategoria_mckinsey": "Software e infraestrutura",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "tecnologia_software",
            "confianca_classificacao": "alta",
            "motivo_governanca": "",
        }

    if match_any(
        normalized,
        [
            "energia",
            "agua",
            "sanepar",
            "copel",
            "aluguel",
            "condominio",
            "gas",
            "telefonia",
            "claro",
            "tim",
            "vivo",
            "oi ",
        ],
    ):
        return {
            "categoria_mckinsey": "Fixo - Estrutura",
            "subcategoria_mckinsey": "Facilities e ocupacao",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "estrutura_facilities",
            "confianca_classificacao": "alta",
            "motivo_governanca": "",
        }

    if match_any(
        normalized,
        [
            "consultoria",
            "contabilidade",
            "contabil",
            "advoc",
            "jurid",
            "auditoria",
            "certificacao",
            "assessoria",
            "servicos agropecuarios",
            "representacao",
            "representacoes",
            "gestao de creditos",
            "comunicacao",
        ],
    ):
        return {
            "categoria_mckinsey": "Fixo - Servicos Profissionais",
            "subcategoria_mckinsey": "Consultoria e servicos especializados",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "servicos_profissionais",
            "confianca_classificacao": "media",
            "motivo_governanca": "",
        }

    if match_any(
        normalized,
        [
            "quimica",
            "quimicos",
            "chemicals",
            "bioquimica",
            "biochemicals",
            "fertilizantes",
            "sementes",
            "insumos agricolas",
            "produtos agricolas",
            "bioprocessos",
            "oleos essenciais",
            "fragrancias",
            "oleoquimica",
            "biologicos",
            "buschle",
            "limagrain",
            "livital",
            "sigma aldrich",
            "fermentos",
            "rural citrus",
            "agro commodities",
            "gtec agro insumos",
            "tarim agro",
            "genetica tecnologias ambientais",
            "allbiom",
            "atomizer",
        ],
    ):
        return {
            "categoria_mckinsey": "Variavel - Insumos",
            "subcategoria_mckinsey": "Quimicos e materias-primas",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "insumos_quimicos",
            "confianca_classificacao": "alta",
            "motivo_governanca": "",
        }

    if match_any(
        normalized,
        [
            "embalagens",
            "embalagem",
            "plast",
            "bombonas",
            "cartonagem",
            "cartovel",
            "greif",
            "embs",
            "grafica",
            "rotulo",
            "etiqueta",
            "papelaria",
            "danpack",
            "zenaplast",
            "soroplast",
            "hecaplast",
        ],
    ):
        return {
            "categoria_mckinsey": "Variavel - Insumos",
            "subcategoria_mckinsey": "Embalagens e materiais",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "insumos_embalagens",
            "confianca_classificacao": "alta",
            "motivo_governanca": "",
        }

    if match_any(
        normalized,
        [
            "locadora",
            "maquinas",
            "equip",
            "manutenc",
            "retec",
            "atomizer",
            "acinox",
        ],
    ):
        return {
            "categoria_mckinsey": "Fixo - Estrutura",
            "subcategoria_mckinsey": "Manutencao e equipamentos",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "estrutura_manutencao",
            "confianca_classificacao": "media",
            "motivo_governanca": "",
        }

    if match_any(
        normalized,
        [
            "associacao comercial",
            "livraria",
            "papelaria",
            "panificadora",
            "cartorio",
            "certfoz",
            "schneider bebidas",
            "grafica",
        ],
    ):
        return {
            "categoria_mckinsey": ADMIN_DEFAULT_CATEGORY,
            "subcategoria_mckinsey": "Despesas administrativas",
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "administrativo_escritorio",
            "confianca_classificacao": "media",
            "motivo_governanca": "",
        }

    if doc_type == "pf":
        return {
            "categoria_mckinsey": REVIEW_CATEGORY,
            "subcategoria_mckinsey": REVIEW_SUBCATEGORY,
            "status_mapeamento": "revisao_manual_governanca",
            "regra_classificacao": "pf_ambigua",
            "confianca_classificacao": "baixa",
            "motivo_governanca": "fornecedor pessoa fisica sem regra deterministica",
        }

    if looks_like_company(normalized):
        return {
            "categoria_mckinsey": ADMIN_DEFAULT_CATEGORY,
            "subcategoria_mckinsey": ADMIN_DEFAULT_SUBCATEGORY,
            "status_mapeamento": "classificado_regra_nome",
            "regra_classificacao": "default_empresa",
            "confianca_classificacao": "media",
            "motivo_governanca": "",
        }

    return {
        "categoria_mckinsey": REVIEW_CATEGORY,
        "subcategoria_mckinsey": REVIEW_SUBCATEGORY,
        "status_mapeamento": "revisao_manual_governanca",
        "regra_classificacao": "default_revisao",
        "confianca_classificacao": "baixa",
        "motivo_governanca": "fornecedor sem sinal suficiente no nome",
    }


def build_checks(df: pd.DataFrame, review_df: pd.DataFrame, pending_df: pd.DataFrame) -> pd.DataFrame:
    supplier_totals = (
        df.groupby(["fornecedor", "categoria_mckinsey", "subcategoria_mckinsey", "status_mapeamento"], as_index=False)
        .agg(valor_total=("valor", "sum"))
        .sort_values("valor_total", ascending=False)
    )
    top_20 = supplier_totals.head(20)
    top_20_problematic = top_20["status_mapeamento"].isin(["revisao_manual_governanca", "pendente_nome_contato"]).mean()
    admin_default_ratio = (
        (df["categoria_mckinsey"] == ADMIN_DEFAULT_CATEGORY)
        & (df["subcategoria_mckinsey"] == ADMIN_DEFAULT_SUBCATEGORY)
    ).mean()
    review_ratio = review_df["valor_total"].sum() / max(float(df["valor"].sum()), 1.0)
    pending_ratio = pending_df["valor_total"].sum() / max(float(df["valor"].sum()), 1.0)

    checks = [
        {"check": "ap_rows_gt_zero", "ok": len(df) > 0},
        {"check": "has_category", "ok": df["categoria_mckinsey"].notna().all() if len(df) else False},
        {"check": "pending_names_value_ratio_lt_10pct", "ok": pending_ratio <= 0.10, "details": f"ratio={pending_ratio:.4f}"},
        {"check": "manual_review_value_ratio_lt_25pct", "ok": review_ratio <= 0.25, "details": f"ratio={review_ratio:.4f}"},
        {"check": "default_admin_row_ratio_lt_45pct", "ok": admin_default_ratio <= 0.45, "details": f"ratio={admin_default_ratio:.4f}"},
        {"check": "top20_problematic_ratio_lt_35pct", "ok": top_20_problematic <= 0.35, "details": f"ratio={top_20_problematic:.4f}"},
    ]

    qa = pd.DataFrame(checks)
    if "details" not in qa.columns:
        qa["details"] = ""
    qa["details"] = qa["details"].fillna("")
    qa["status"] = qa["ok"].map(lambda value: "PASS" if bool(value) else "FAIL")
    return qa


def render_summary(run_id: str, qa: pd.DataFrame, review_df: pd.DataFrame, pending_df: pd.DataFrame) -> str:
    lines = [
        "# AP Cost Classifier Summary",
        "",
        f"- Run ID: {run_id}",
        f"- Checks: {len(qa)}",
        f"- FAIL: {int((qa['status'] == 'FAIL').sum())}",
        f"- Fornecedores em revisao: {len(review_df)}",
        f"- Fornecedores com nome pendente: {len(pending_df)}",
        "",
    ]
    for _, row in qa.iterrows():
        details = f" ({row['details']})" if row["details"] else ""
        lines.append(f"- {row['check']}: {row['status']}{details}")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    args = parser.parse_args()

    cfg = load_config(Path(args.config))
    out_dir = Path(cfg["output_dir"])
    status_dir = Path(cfg.get("status_dir", out_dir))
    out_dir.mkdir(parents=True, exist_ok=True)
    status_dir.mkdir(parents=True, exist_ok=True)

    ap_rows = read_jsonl(Path(cfg["ap_jsonl"]))
    contacts_rows = read_jsonl(Path(cfg["contacts_jsonl"]))

    names_by_id: dict[int, str] = {}
    doc_type_by_id: dict[int, str] = {}
    for contact in contacts_rows:
        contact_id = contact.get("id")
        if contact_id is None:
            continue
        try:
            normalized_id = int(contact_id)
        except (TypeError, ValueError):
            continue
        if contact.get("nome"):
            names_by_id[normalized_id] = str(contact["nome"])
        doc_type_by_id[normalized_id] = detect_document_type(str(contact.get("numeroDocumento", "")))

    for sales_path in cfg.get("fallback_sales_jsonl", []):
        for sale in read_jsonl(Path(sales_path)):
            contato = sale.get("contato") or {}
            contact_id = contato.get("id")
            if contact_id is None:
                continue
            try:
                normalized_id = int(contact_id)
            except (TypeError, ValueError):
                continue
            if contato.get("nome"):
                names_by_id.setdefault(normalized_id, str(contato["nome"]))

    rows: list[dict[str, Any]] = []
    for row in ap_rows:
        contato = row.get("contato") or {}
        contact_id = contato.get("id")
        normalized_contact_id: int | None = None
        if contact_id is not None:
            try:
                normalized_contact_id = int(contact_id)
            except (TypeError, ValueError):
                normalized_contact_id = None
        supplier_name = (
            names_by_id.get(normalized_contact_id, f"Fornecedor Pendente (contato_{normalized_contact_id})")
            if normalized_contact_id is not None
            else "sem_contato"
        )
        doc_type = doc_type_by_id.get(normalized_contact_id or -1, "unknown")
        classified = classify_supplier(supplier_name, doc_type)
        rows.append(
            {
                "id": row.get("id"),
                "vencimento": row.get("vencimento"),
                "mes": str(row.get("vencimento") or "")[:7],
                "valor": float(row.get("valor") or 0.0),
                "contato_id": normalized_contact_id,
                "fornecedor": supplier_name,
                "tipo_documento_contato": doc_type,
                **classified,
            }
        )

    df = pd.DataFrame(rows).sort_values(["mes", "fornecedor", "id"]).reset_index(drop=True)
    ap_out = out_dir / "ap_bling_classificado.csv"
    map_out = out_dir / "mapeamento_fornecedores_mckinsey.csv"
    pending_out = out_dir / "fornecedores_pendentes_nome.csv"
    review_out = out_dir / "fornecedores_revisao_governanca.csv"
    qa_out = status_dir / f"ap_cost_classifier_{args.run_id}_qa.csv"
    summary_out = status_dir / f"ap_cost_classifier_{args.run_id}_summary.md"
    status_out = status_dir / f"ap_cost_classifier_{args.run_id}_status.json"

    df.to_csv(ap_out, index=False, encoding="utf-8-sig")

    supplier_map = (
        df.groupby(
            [
                "contato_id",
                "fornecedor",
                "tipo_documento_contato",
                "categoria_mckinsey",
                "subcategoria_mckinsey",
                "status_mapeamento",
                "regra_classificacao",
                "confianca_classificacao",
                "motivo_governanca",
            ],
            as_index=False,
        )
        .agg(qtd_lancamentos=("id", "count"), valor_total=("valor", "sum"))
        .sort_values("valor_total", ascending=False)
    )
    supplier_map.to_csv(map_out, index=False, encoding="utf-8-sig")

    pending_df = (
        supplier_map[supplier_map["status_mapeamento"] == "pendente_nome_contato"][
            ["contato_id", "fornecedor", "qtd_lancamentos", "valor_total", "motivo_governanca"]
        ]
        .sort_values("valor_total", ascending=False)
        .reset_index(drop=True)
    )
    pending_df.to_csv(pending_out, index=False, encoding="utf-8-sig")

    review_df = (
        supplier_map[supplier_map["status_mapeamento"] == "revisao_manual_governanca"][
            [
                "contato_id",
                "fornecedor",
                "tipo_documento_contato",
                "categoria_mckinsey",
                "subcategoria_mckinsey",
                "qtd_lancamentos",
                "valor_total",
                "regra_classificacao",
                "confianca_classificacao",
                "motivo_governanca",
            ]
        ]
        .sort_values("valor_total", ascending=False)
        .reset_index(drop=True)
    )
    review_df.to_csv(review_out, index=False, encoding="utf-8-sig")

    qa = build_checks(df, review_df, pending_df)
    qa.to_csv(qa_out, index=False, encoding="utf-8-sig")
    summary_out.write_text(render_summary(args.run_id, qa, review_df, pending_df), encoding="utf-8")

    failed_checks = qa[qa["status"] == "FAIL"]["check"].tolist()
    status = {
        "status": "success" if not failed_checks else "partial",
        "run_id": args.run_id,
        "inputs": [cfg.get("ap_jsonl"), cfg.get("contacts_jsonl")],
        "outputs": [str(ap_out), str(map_out), str(pending_out), str(review_out), str(qa_out), str(summary_out)],
        "warnings": failed_checks,
        "error": "",
    }
    write_json(status_out, status)

    print(str(status_out))
    print(str(qa_out))


if __name__ == "__main__":
    main()
