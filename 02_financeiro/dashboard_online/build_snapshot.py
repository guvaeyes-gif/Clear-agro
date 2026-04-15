from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from integrations.shared.bling_paths import resolve_bling_file
except Exception:
    resolve_bling_file = None

CLASSIFIER_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "dre" / "finance-recon-hub" / "scripts"
if str(CLASSIFIER_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(CLASSIFIER_SCRIPTS_DIR))

try:
    from ap_cost_classifier_local import classify_supplier, detect_document_type
except Exception:
    classify_supplier = None
    detect_document_type = None


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def to_float(value: str | None) -> float:
    return float((value or "0").strip())


def parse_decimal(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        pass
    if "," in text and "." in text:
        try:
            return float(text.replace(".", "").replace(",", "."))
        except Exception:
            return None
    if "," in text:
        try:
            return float(text.replace(",", "."))
        except Exception:
            return None
    return None


def first_present(payload: dict[str, Any], candidates: list[str]) -> str:
    for key in candidates:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def load_jsonl_rows(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def load_bank_balance_snapshot(clear_os_root: Path) -> dict[str, Any]:
    staging_candidates = [
        ROOT / "data" / "staging" / "stg_banks.csv",
        clear_os_root / "data" / "staging" / "stg_banks.csv",
    ]
    staging_path = next((path for path in staging_candidates if path.exists()), None)
    if staging_path is not None:
        with staging_path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))

        latest_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in rows:
            bank_name = first_present(row, ["bank_name", "banco", "bank", "instituicao"]) or "Banco"
            account_name = first_present(row, ["conta", "account", "conta_bancaria", "source_file"]) or "Conta principal"
            company = first_present(row, ["empresa", "company"]) or "CLEAR"
            balance_raw = first_present(
                row,
                ["saldo", "balance", "saldo_final", "saldo_disponivel", "saldo_atual"],
            )
            if not balance_raw:
                continue
            balance = parse_decimal(balance_raw)
            if balance is None:
                continue
            date_value = first_present(row, ["data", "date", "data_saldo", "data_ref"])
            balance_dt = parse_date(date_value) if date_value else None
            sort_key = balance_dt.isoformat() if balance_dt else ""
            key = (company, bank_name, account_name)
            current = latest_by_key.get(key)
            if current is None or sort_key >= current["sort_key"]:
                latest_by_key[key] = {
                    "company": normalize_text(company),
                    "bank_name": normalize_text(bank_name),
                    "account_name": normalize_text(account_name),
                    "balance": round(balance, 2),
                    "balance_status": "MANUAL",
                    "as_of": balance_dt.date().isoformat() if balance_dt else "",
                    "sort_key": sort_key,
                    "source_file": first_present(row, ["source_file", "arquivo", "file_name"]),
                }

        balances = [
            {
                "company": item["company"],
                "bank_name": item["bank_name"],
                "account_name": item["account_name"],
                "balance": item["balance"],
                "balance_status": item["balance_status"],
                "as_of": item["as_of"],
                "source_file": item["source_file"],
            }
            for item in latest_by_key.values()
        ]
        balances.sort(key=lambda item: (item["company"], item["bank_name"], item["account_name"]))
        total_balance = round(sum(float(item["balance"]) for item in balances), 2)
        as_of = max((item["as_of"] for item in balances if item["as_of"]), default="")
        return {
            "available": bool(balances),
            "source": str(staging_path),
            "as_of": as_of,
            "total_balance": total_balance,
            "bank_count": len(balances),
            "balances": balances,
        }

    bank_cache_rows: list[dict[str, Any]] = []
    caixa_rows_for_aggregation: list[dict[str, Any]] = []
    for filename in ["contas_financeiras_cache.jsonl", "contas_financeiras_cache_cr.jsonl"]:
        for path in resolve_cache_candidates(clear_os_root, filename):
            company = "CR" if path.name.endswith("_cr.jsonl") else "CZ"
            for row in load_jsonl_rows(path):
                caixa_rows_for_aggregation.append({"company": company, "source_file": str(path), "row": row})
                bank_name = first_value(
                    row,
                    ["descricao", "nome", "descricaoConta", "descricao_conta", "banco.nome", "banco"],
                ) or "Banco"
                account_name = first_value(
                    row,
                    ["descricao", "nome", "conta", "numero", "numeroConta", "numero_conta"],
                ) or bank_name
                balance = None
                for key in [
                    "saldo",
                    "saldoAtual",
                    "saldo_atual",
                    "saldoDisponivel",
                    "saldo_disponivel",
                    "valorSaldo",
                    "valor_saldo",
                    "saldoDerivadoMovimentos",
                ]:
                    try:
                        raw = row.get(key)
                        if raw not in (None, ""):
                            balance = float(raw)
                            break
                    except Exception:
                        continue
                if balance is None:
                    continue
                bank_cache_rows.append(
                    {
                        "company": company,
                        "bank_name": normalize_text(bank_name),
                        "account_name": normalize_text(account_name),
                        "balance": round(balance, 2),
                        "balance_status": "API",
                        "as_of": first_value(row, ["dataSaldo", "data_saldo", "updated_at", "snapshot_at"]),
                        "source_file": str(path),
                    }
                )

    if bank_cache_rows:
        as_of = max((str(item["as_of"]) for item in bank_cache_rows if item["as_of"]), default="")
        total_balance = round(sum(float(item["balance"]) for item in bank_cache_rows), 2)
        return {
            "available": True,
            "source": "bling_api/contas_financeiras_cache*.jsonl",
            "as_of": as_of,
            "total_balance": total_balance,
            "bank_count": len(bank_cache_rows),
            "balances": sorted(bank_cache_rows, key=lambda item: (item["company"], item["bank_name"], item["account_name"])),
        }

    if caixa_rows_for_aggregation:
        latest_by_account: dict[tuple[str, str, str], dict[str, Any]] = {}
        for item in caixa_rows_for_aggregation:
            company = item["company"]
            source_file = item["source_file"]
            row = item["row"]
            conta = row.get("contaFinanceira") or {}
            if not isinstance(conta, dict):
                continue
            account_id = str(conta.get("id") or "").strip()
            account_name = str(conta.get("descricao") or conta.get("nome") or "").strip() or "Conta principal"
            if not account_id:
                continue
            movement_kind = str(row.get("tipoLancamento") or "").strip()
            movement_desc = normalize_text(
                f"{row.get('descricao') or ''} {row.get('observacoes') or ''}"
            )
            try:
                value = float(row.get("valor") or 0.0)
            except Exception:
                continue
            date_value = first_value(row, ["data", "competencia"])
            row_dt = parse_date(date_value) if date_value else None
            sort_key = row_dt.isoformat() if row_dt else ""
            key = (company, account_id, account_name)
            current = latest_by_account.get(key)
            if current is None:
                balance_status = "DERIVADO_MOVIMENTOS"
                if movement_kind == "1" or abs(value) <= 1.0:
                    balance_status = "SEM_SALDO_API"
                latest_by_account[key] = {
                    "company": company,
                    "bank_name": normalize_text(account_name),
                    "account_name": normalize_text(account_name),
                    "balance": round(value, 2),
                    "balance_status": balance_status,
                    "as_of": row_dt.date().isoformat() if row_dt else "",
                    "sort_key": sort_key,
                    "source_file": source_file,
                }
            else:
                current["balance"] = round(float(current["balance"]) + value, 2)
                if current.get("balance_status") == "SEM_SALDO_API" and abs(float(current["balance"])) > 1.0:
                    current["balance_status"] = "DERIVADO_MOVIMENTOS"
                if sort_key >= current["sort_key"]:
                    current["as_of"] = row_dt.date().isoformat() if row_dt else current["as_of"]
                    current["sort_key"] = sort_key

        balances = [
            {
                "company": item["company"],
                "bank_name": item["bank_name"],
                "account_name": item["account_name"],
                "balance": item["balance"],
                "balance_status": item.get("balance_status", "DERIVADO_MOVIMENTOS"),
                "as_of": item["as_of"],
                "source_file": item["source_file"],
            }
            for item in latest_by_account.values()
        ]
        balances.sort(key=lambda item: (item["company"], item["bank_name"], item["account_name"]))
        total_balance = round(sum(float(item["balance"]) for item in balances), 2)
        as_of = max((item["as_of"] for item in balances if item["as_of"]), default="")
        return {
            "available": bool(balances),
            "source": "bling_api/contas_financeiras_cache*.jsonl (derived from /caixas movements)",
            "as_of": as_of,
            "total_balance": total_balance,
            "bank_count": len(balances),
            "balances": balances,
        }

    return {
        "available": False,
        "source": "",
        "as_of": "",
        "total_balance": 0.0,
        "bank_count": 0,
        "balances": [],
    }


def unique_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    out: list[Path] = []
    for path in paths:
        key = str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def resolve_cache_candidates(clear_os_root: Path, filename: str) -> list[Path]:
    candidates = [
        ROOT / "bling_api" / filename,
        clear_os_root / "bling_api" / filename,
    ]
    if resolve_bling_file is not None:
        for mode in ["pipeline", "app"]:
            try:
                candidates.append(resolve_bling_file(filename, mode=mode))
            except Exception:
                continue
    compatibility_roots = [
        clear_os_root / "11_agentes_automacoes" / "11_dev_codex_agent" / "repos" / "CRM_Clear_Agro" / "bling_api",
        Path.home() / "Documents" / "Clear_OS" / "bling_api",
        Path.home() / "projects" / "CRM_Clear_Agro" / "bling_api",
    ]
    for root in compatibility_roots:
        candidates.append(root / filename)
    return unique_paths([path for path in candidates if path is not None])


def resolve_cache_glob_candidates(clear_os_root: Path, pattern: str) -> list[Path]:
    roots = [
        ROOT / "bling_api",
        clear_os_root / "bling_api",
        clear_os_root / "11_agentes_automacoes" / "11_dev_codex_agent" / "repos" / "CRM_Clear_Agro" / "bling_api",
        Path.home() / "Documents" / "Clear_OS" / "bling_api",
        Path.home() / "projects" / "CRM_Clear_Agro" / "bling_api",
    ]
    candidates: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        candidates.extend(root.glob(pattern))
    return unique_paths(candidates)


def contato_nome(row: dict) -> str:
    contato = row.get("contato") or {}
    if isinstance(contato, dict):
        nome = str(contato.get("nome") or "").strip()
        if nome:
            return nome
        contato_id = contato.get("id")
        if contato_id:
            return f"Contato {contato_id}"
    return "N/D"


def build_contact_name_maps(clear_os_root: Path) -> dict[tuple[str, str], str]:
    names_by_id: dict[tuple[str, str], str] = {}
    for filename in ["contatos_cache.jsonl", "contatos_cache_cr.jsonl"]:
        for path in resolve_cache_candidates(clear_os_root, filename):
            company = "CR" if path.name.endswith("_cr.jsonl") else "CZ"
            for row in load_jsonl_rows(path):
                contact_id = str(row.get("id") or "").strip()
                contact_name = str(row.get("nome") or row.get("fantasia") or "").strip()
                if not contact_id or not contact_name:
                    continue
                names_by_id[(company, contact_id)] = contact_name
                names_by_id[("", contact_id)] = contact_name
    return names_by_id


def build_contact_doc_type_maps(clear_os_root: Path) -> dict[tuple[str, str], str]:
    doc_types_by_id: dict[tuple[str, str], str] = {}
    if detect_document_type is None:
        return doc_types_by_id
    for filename in ["contatos_cache.jsonl", "contatos_cache_cr.jsonl"]:
        for path in resolve_cache_candidates(clear_os_root, filename):
            company = "CR" if path.name.endswith("_cr.jsonl") else "CZ"
            for row in load_jsonl_rows(path):
                contact_id = str(row.get("id") or "").strip()
                if not contact_id:
                    continue
                doc_type = detect_document_type(str(row.get("numeroDocumento") or ""))
                doc_types_by_id[(company, contact_id)] = doc_type
                doc_types_by_id[("", contact_id)] = doc_type
    return doc_types_by_id


def resolved_contact_name(row: dict, company: str, contact_names: dict[tuple[str, str], str]) -> str:
    contato = row.get("contato") or {}
    if isinstance(contato, dict):
        nome = str(contato.get("nome") or "").strip()
        if nome:
            return nome
        contato_id = str(contato.get("id") or "").strip()
        if contato_id:
            resolved = contact_names.get((company, contato_id)) or contact_names.get(("", contato_id))
            if resolved:
                return resolved
            return f"Contato {contato_id}"
    return "N/D"


def classify_ap_monthly(clear_os_root: Path, min_year: int = 2023) -> dict[str, list[dict[str, Any]]]:
    if classify_supplier is None:
        return {"category_monthly": [], "subcategory_monthly": []}

    contact_names = build_contact_name_maps(clear_os_root)
    contact_doc_types = build_contact_doc_type_maps(clear_os_root)
    category_monthly: dict[tuple[str, str, str], float] = defaultdict(float)
    subcategory_monthly: dict[tuple[str, str, str], float] = defaultdict(float)

    cache_paths = [
        path
        for filename in ["contas_pagar_cache.jsonl", "contas_pagar_cache_cr.jsonl"]
        for path in resolve_cache_candidates(clear_os_root, filename)
    ]
    for path in cache_paths:
        company = "CR" if path.name.endswith("_cr.jsonl") else "CZ"
        for row in load_jsonl_rows(path):
            month_key = ""
            for field in ["vencimento", "dataEmissao", "data_emissao", "competencia"]:
                dt = parse_date(str(row.get(field) or "").strip())
                if dt is not None and dt.year >= min_year:
                    month_key = dt.strftime("%Y-%m")
                    break
            if not month_key:
                continue
            try:
                value = float(row.get("valor") or 0.0)
            except Exception:
                value = 0.0
            if value == 0:
                continue
            supplier_name = resolved_contact_name(row, company, contact_names)
            contato = row.get("contato") or {}
            contact_id = str(contato.get("id") or "").strip() if isinstance(contato, dict) else ""
            doc_type = (
                contact_doc_types.get((company, contact_id))
                or contact_doc_types.get(("", contact_id))
                or "unknown"
            )
            classified = classify_supplier(supplier_name, doc_type)
            category = str(classified.get("categoria_mckinsey") or "").strip() or "A revisar - Governanca"
            subcategory = str(classified.get("subcategoria_mckinsey") or "").strip() or "Classificacao ambigua"
            category_monthly[(month_key, company, category)] += value
            subcategory_monthly[(month_key, company, subcategory)] += value

    category_rows = [
        {"mes": mes, "company": company, "label": label, "valor": round(valor, 2)}
        for (mes, company, label), valor in sorted(category_monthly.items())
    ]
    subcategory_rows = [
        {"mes": mes, "company": company, "label": label, "valor": round(valor, 2)}
        for (mes, company, label), valor in sorted(subcategory_monthly.items())
    ]
    return {"category_monthly": category_rows, "subcategory_monthly": subcategory_rows}


def first_value(payload: dict, candidates: list[str]) -> str:
    for key in candidates:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def parse_date(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for candidate in [text, text.replace("Z", "+00:00")]:
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue
    return None


def normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().upper().split())


def row_is_open(row: dict[str, Any]) -> bool:
    situacao = row.get("situacao")
    situacao_txt = normalize_text(situacao)
    if situacao_txt and "CANCEL" in situacao_txt:
        return False

    saldo = row.get("saldo")
    try:
        if saldo not in (None, ""):
            return float(saldo) > 0
    except Exception:
        pass

    try:
        if situacao is not None and int(situacao) in {3, 4, 6}:
            return True
        if situacao is not None and int(situacao) in {1, 2, 5}:
            return False
    except Exception:
        pass

    if situacao_txt:
        if any(token in situacao_txt for token in ["ABERTO", "EM ABERTO", "PENDENTE", "PARCIAL"]):
            return True
        if any(token in situacao_txt for token in ["PAGO", "PAGA", "BAIXADO", "LIQUIDADO", "QUITADO", "RECEBIDO"]):
            return False
    return True


def row_is_cancelled(row: dict[str, Any]) -> bool:
    situacao = row.get("situacao")
    situacao_txt = normalize_text(situacao)
    if situacao_txt and "CANCEL" in situacao_txt:
        return True
    try:
        if situacao is not None and int(situacao) == 5:
            return True
    except Exception:
        pass
    return False


def row_matches_min_year(row: dict[str, Any], min_year: int = 2023) -> bool:
    for field in ["vencimento", "dataEmissao", "data_emissao", "competencia"]:
        dt = parse_date(str(row.get(field) or "").strip())
        if dt is not None:
            return dt.year >= min_year
    return False


def normalize_contas(paths: list[Path], tipo: str, *, open_only: bool = True, min_year: int = 2023) -> list[dict]:
    items: list[dict] = []
    today_dt = datetime.now()
    contact_names = build_contact_name_maps(ROOT)
    for path in paths:
        for row in load_jsonl_rows(path):
            if row_is_cancelled(row):
                continue
            if open_only and not row_is_open(row):
                continue
            if min_year and not row_matches_min_year(row, min_year=min_year):
                continue
            try:
                valor = float(row.get("valor") or 0)
            except Exception:
                valor = 0.0
            saldo = row.get("saldo")
            try:
                saldo_val = float(saldo) if saldo not in (None, "") else None
            except Exception:
                saldo_val = None
            effective_value = saldo_val if (open_only and saldo_val is not None and saldo_val > 0) else valor
            vencimento = str(row.get("vencimento") or "").strip()
            due_dt = parse_date(vencimento)
            data_emissao = first_value(
                row,
                [
                    "dataEmissao",
                    "data_emissao",
                    "emissao",
                    "origem.dataEmissao",
                ],
            )
            origem = row.get("origem")
            if not data_emissao and isinstance(origem, dict):
                data_emissao = first_value(origem, ["dataEmissao", "data_emissao", "emissao"])
            dias_atraso = 0
            if due_dt is not None:
                dias_atraso = max((today_dt.date() - due_dt.date()).days, 0)
            juros = 0.0
            for key in ["juros", "valorJuros", "valor_juros", "mora", "multa"]:
                try:
                    raw_value = row.get(key)
                    if raw_value not in (None, ""):
                        juros += float(raw_value)
                except Exception:
                    continue
            company = first_value(row, ["empresa", "company"]) or ("CR" if path.name.endswith("_cr.jsonl") else "CZ")
            entity_name = resolved_contact_name(row, company, contact_names)
            items.append(
                {
                    "tipo": tipo,
                    "valor": effective_value,
                    "valor_original": valor,
                    "saldo": saldo_val if saldo_val is not None else effective_value,
                    "data_emissao": data_emissao,
                    "competencia": first_value(row, ["competencia"]),
                    "vencimento": vencimento,
                    "situacao": str(row.get("situacao") or ""),
                    "contato": entity_name,
                    "documento": first_value(row, ["numeroDocumento", "numero_documento", "numero", "id"])
                    or (first_value(origem, ["numero"]) if isinstance(origem, dict) else ""),
                    "company": company,
                    "cliente_fornecedor": entity_name,
                    "fornecedor": entity_name if tipo == "pagar" else "",
                    "cliente": entity_name if tipo == "receber" else "",
                    "cultura": first_value(row, ["cultura", "cultura_nome", "crop", "safra_cultura"]),
                    "zafra": first_value(row, ["zafra", "safra", "season"]),
                    "juros": round(juros, 2),
                    "dias_atraso": int(dias_atraso),
                    "vencido": bool(dias_atraso > 0),
                }
            )
    return items


def _parse_month_key(row: dict[str, Any], today_dt: datetime, year: int | None = None) -> str | None:
    for field in ["data_emissao", "competencia", "vencimento"]:
        value = row.get(field)
        if not value:
            continue
        dt = parse_date(str(value))
        if dt is None:
            continue
        if (year is not None and dt.year != year) or dt.date() > today_dt.date():
            continue
        return dt.strftime("%Y-%m")
    return None


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def build_product_cost_maps(clear_os_root: Path) -> tuple[dict[tuple[str, str], float], dict[tuple[str, str], float], dict[tuple[str, str], float]]:
    costs_by_id: dict[tuple[str, str], float] = {}
    costs_by_code: dict[tuple[str, str], float] = {}
    costs_by_name: dict[tuple[str, str], float] = {}

    for filename in ["produtos_cache.jsonl", "produtos_cache_cr.jsonl"]:
        for path in resolve_cache_candidates(clear_os_root, filename):
            company = "CR" if path.name.endswith("_cr.jsonl") else "CZ"
            for row in load_jsonl_rows(path):
                cost = _to_float(row.get("precoCusto"))
                if cost is None:
                    continue
                product_id = str(row.get("id") or "").strip()
                product_code = str(row.get("codigo") or "").strip().upper()
                product_name = normalize_text(row.get("nome"))
                if product_id:
                    costs_by_id[(company, product_id)] = cost
                    costs_by_id[("", product_id)] = cost
                if product_code:
                    costs_by_code[(company, product_code)] = cost
                    costs_by_code[("", product_code)] = cost
                if product_name:
                    costs_by_name[(company, product_name)] = cost
                    costs_by_name[("", product_name)] = cost

    return costs_by_id, costs_by_code, costs_by_name


def build_composition_map(clear_os_root: Path) -> dict[tuple[str, str], list[dict[str, Any]]]:
    composition_by_product: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for filename in ["produtos_composicao_cache.jsonl", "produtos_composicao_cache_cr.jsonl"]:
        for path in resolve_cache_candidates(clear_os_root, filename):
            company = "CR" if path.name.endswith("_cr.jsonl") else "CZ"
            for row in load_jsonl_rows(path):
                product_id = str(row.get("id") or row.get("produto.id") or "").strip()
                if not product_id:
                    continue
                items = row.get("composicao_itens") or []
                if isinstance(items, list) and items:
                    composition_by_product[(company, product_id)] = items
    return composition_by_product


def manual_cost_overrides() -> dict[tuple[str, str], float]:
    # Fallbacks only for SKUs that exist in sales but remain uncosted in Bling.
    return {
        ("CZ", "16598009702"): 3.32464648,   # CLEAR COAT 250mL -> nearest 250mL ClearCoat variant
        ("CZ", "16598009703"): 9.77047565,   # CLEAR COAT 1L -> nearest 1L ClearCoat variant
        ("CZ", "16126477344"): 32.84726295 * 4,  # TOPAIR 20L -> 4x TOPAIR 5L unit cost
    }


def resolve_product_unit_cost(
    *,
    company: str,
    product_id: str,
    product_code: str,
    product_name: str,
    costs_by_id: dict[tuple[str, str], float],
    costs_by_code: dict[tuple[str, str], float],
    costs_by_name: dict[tuple[str, str], float],
    composition_by_product: dict[tuple[str, str], list[dict[str, Any]]],
    cost_overrides: dict[tuple[str, str], float] | None = None,
    _visited: set[tuple[str, str]] | None = None,
) -> tuple[float | None, str]:
    overrides = cost_overrides or {}
    if product_id:
        override_cost = overrides.get((company, product_id)) or overrides.get(("", product_id))
        if override_cost is not None and override_cost > 0:
            return override_cost, "manual_fallback"

    if product_id:
        cost = costs_by_id.get((company, product_id))
        if cost is None:
            cost = costs_by_id.get(("", product_id))
        if cost is not None and cost > 0:
            return cost, "product_id"
        if cost == 0:
            return None, "product_id_zero_cost"

    if product_code:
        cost = costs_by_code.get((company, product_code))
        if cost is None:
            cost = costs_by_code.get(("", product_code))
        if cost is not None and cost > 0:
            return cost, "product_code"
        if cost == 0:
            return None, "product_code_zero_cost"

    if product_name:
        cost = costs_by_name.get((company, product_name))
        if cost is None:
            cost = costs_by_name.get(("", product_name))
        if cost is not None and cost > 0:
            return cost, "product_name"
        if cost == 0:
            return None, "product_name_zero_cost"

    if not product_id:
        return None, "missing_product_ref"

    visit_key = (company, product_id)
    visited = set() if _visited is None else set(_visited)
    if visit_key in visited:
        return None, "composition_cycle"
    visited.add(visit_key)

    components = composition_by_product.get((company, product_id)) or []
    if not components:
        return None, "product_not_found"

    total_cost = 0.0
    matched_components = 0
    for component in components:
        if not isinstance(component, dict):
            continue
        component_id = str(
            component.get("idProduto")
            or component.get("produtoId")
            or component.get("id_produto")
            or component.get("id")
            or ""
        ).strip()
        component_code = str(component.get("codigo") or "").strip().upper()
        component_name = normalize_text(component.get("produto") or component.get("nome") or component.get("descricao"))
        component_qty = _to_float(
            component.get("quantidade")
            or component.get("qtd")
            or component.get("qtde")
            or component.get("quantidadeUtilizada")
            or component.get("quantidadeComponente")
        )
        if component_qty is None:
            continue
        component_cost, _ = resolve_product_unit_cost(
            company=company,
            product_id=component_id,
            product_code=component_code,
            product_name=component_name,
            costs_by_id=costs_by_id,
            costs_by_code=costs_by_code,
            costs_by_name=costs_by_name,
            composition_by_product=composition_by_product,
            cost_overrides=overrides,
            _visited=visited,
        )
        if component_cost is None or component_cost <= 0:
            continue
        total_cost += component_qty * component_cost
        matched_components += 1

    if matched_components > 0 and total_cost > 0:
        return total_cost, "composition"
    return None, "composition_unpriced"


def build_bling_dre_from_erp(clear_os_root: Path, ap_rows: list[dict]) -> tuple[list[dict], dict]:
    revenue_by_month: dict[str, float] = defaultdict(float)
    sales_cmv_by_month: dict[str, float] = defaultdict(float)
    expense_by_month: dict[str, float] = defaultdict(float)
    today_dt = datetime.now()
    ap_rows_total = 0
    ap_rows_exact_date = 0
    ap_rows_fallback_due = 0
    cmv_item_total = 0
    cmv_item_matched = 0
    cmv_item_missing = 0
    cmv_source_counter: dict[str, int] = defaultdict(int)
    cmv_months_sales_coverage: dict[str, int] = defaultdict(int)
    cmv_months_item_count: dict[str, int] = defaultdict(int)
    missing_item_summary: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}

    costs_by_id, costs_by_code, costs_by_name = build_product_cost_maps(clear_os_root)
    composition_by_product = build_composition_map(clear_os_root)
    cost_overrides = manual_cost_overrides()

    nfe_paths = resolve_cache_glob_candidates(clear_os_root, "nfe_*_cache*.jsonl")
    for path in nfe_paths:
        for row in load_jsonl_rows(path):
            issue_dt = parse_date(first_value(row, ["dataEmissao", "dataOperacao"]))
            if issue_dt is None or issue_dt.year < 2023 or issue_dt.date() > today_dt.date():
                continue
            try:
                valor = float(row.get("valorNota") or row.get("valor") or 0)
            except Exception:
                valor = 0.0
            month_key = issue_dt.strftime("%Y-%m")
            revenue_by_month[month_key] += valor

    sale_paths = resolve_cache_glob_candidates(clear_os_root, "vendas_*_cache*.jsonl")
    for path in sale_paths:
        company = "CR" if path.name.endswith("_cr.jsonl") else "CZ"
        for row in load_jsonl_rows(path):
            sale_dt = parse_date(first_value(row, ["data", "dataSaida", "dataPrevista"]))
            if sale_dt is None or sale_dt.year < 2023 or sale_dt.date() > today_dt.date():
                continue
            month_key = sale_dt.strftime("%Y-%m")
            for item in row.get("itens") or []:
                if not isinstance(item, dict):
                    continue
                cmv_item_total += 1
                cmv_months_item_count[month_key] += 1
                produto = item.get("produto") or {}
                product_id = str(produto.get("id") if isinstance(produto, dict) else "" or "").strip()
                product_code = str(
                    item.get("codigo")
                    or (produto.get("codigo") if isinstance(produto, dict) else "")
                    or ""
                ).strip().upper()
                product_name = normalize_text(
                    item.get("descricao")
                    or item.get("descricaoDetalhada")
                    or (produto.get("nome") if isinstance(produto, dict) else "")
                )
                cost, source = resolve_product_unit_cost(
                    company=company,
                    product_id=product_id,
                    product_code=product_code,
                    product_name=product_name,
                    costs_by_id=costs_by_id,
                    costs_by_code=costs_by_code,
                    costs_by_name=costs_by_name,
                    composition_by_product=composition_by_product,
                    cost_overrides=cost_overrides,
                )
                if cost is None or cost <= 0:
                    cmv_item_missing += 1
                    cmv_source_counter[source] += 1
                    missing_key = (company, month_key, product_id, product_code, product_name)
                    summary = missing_item_summary.setdefault(
                        missing_key,
                        {
                            "company": company,
                            "mes": month_key,
                            "produto_id": product_id,
                            "produto_codigo": product_code,
                            "produto": product_name,
                            "motivo": source,
                            "quantidade_total": 0.0,
                            "itens": 0,
                        },
                    )
                    try:
                        qty = float(item.get("quantidade") or 0.0)
                    except Exception:
                        qty = 0.0
                    summary["quantidade_total"] += qty
                    summary["itens"] += 1
                    continue
                try:
                    qty = float(item.get("quantidade") or 0.0)
                except Exception:
                    qty = 0.0
                sales_cmv_by_month[month_key] += qty * float(cost)
                cmv_item_matched += 1
                cmv_months_sales_coverage[month_key] += 1
                cmv_source_counter[source] += 1

    for row in ap_rows:
        base_dt = parse_date(str(row.get("data_emissao") or "").strip())
        used_fallback = False
        if base_dt is None:
            base_dt = parse_date(str(row.get("competencia") or "").strip())
        if base_dt is None:
            base_dt = parse_date(str(row.get("vencimento") or "").strip())
            used_fallback = base_dt is not None
        if base_dt is None or base_dt.year < 2023 or base_dt.date() > today_dt.date():
            continue
        ap_rows_total += 1
        if used_fallback:
            ap_rows_fallback_due += 1
        else:
            ap_rows_exact_date += 1
        month_key = base_dt.strftime("%Y-%m")
        expense_by_month[month_key] += float(row.get("valor") or 0.0)

    months = sorted(set(revenue_by_month) | set(expense_by_month))
    monthly_rows: list[dict] = []
    for month_key in months:
        receita = round(revenue_by_month.get(month_key, 0.0), 2)
        sales_cmv = round(sales_cmv_by_month.get(month_key, 0.0), 2)
        cmv_proxy = sales_cmv
        cmv_method = "sales_cost"
        despesa_ap = round(expense_by_month.get(month_key, 0.0), 2)
        monthly_rows.append(
            {
                "mes": month_key,
                "mes_num": int(month_key[-2:]),
                "receita_liquida": receita,
                "custos_variaveis_total": cmv_proxy,
                "cmv_proxy": cmv_proxy,
                "cmv_sales_cost": sales_cmv,
                "cmv_purchase_fallback": 0.0,
                "cmv_method": cmv_method,
                "custo_fixo_base": despesa_ap,
                "margem_contribuicao": round(receita - cmv_proxy, 2),
                "ebitda": round(receita - cmv_proxy - despesa_ap, 2),
                "dre_model": "bling_erp",
                "dre_source": "nfe_emitida + vendas_itens_custo + contas_pagar_total",
                "despesas_ap_proxy": despesa_ap,
            }
        )

    info = {
        "model": "bling_erp",
        "years": sorted({int(month[:4]) for month in months}),
        "revenue_source": "nfe_emitida",
        "cmv_source": "vendas_itens x custo_produto_hierarquico",
        "cmv_priority": "sales_cost_only",
        "cmv_purchase_source": "",
        "purchase_months": [],
        "purchase_month_count": 0,
        "sales_cmv_months": sorted(sales_cmv_by_month),
        "sales_cmv_month_count": len(sales_cmv_by_month),
        "expense_source": "contas_pagar_total_sem_cancelados",
        "expense_date_rule": "data_emissao -> competencia -> vencimento",
        "cmv_item_total": cmv_item_total,
        "cmv_item_matched": cmv_item_matched,
        "cmv_item_missing": cmv_item_missing,
        "cmv_match_rate": round((cmv_item_matched / cmv_item_total), 4) if cmv_item_total else 0.0,
        "cmv_match_sources": dict(sorted(cmv_source_counter.items())),
        "cmv_sales_coverage_by_month": {
            month: {
                "matched_items": cmv_months_sales_coverage.get(month, 0),
                "total_items": cmv_months_item_count.get(month, 0),
            }
            for month in sorted(cmv_months_item_count)
        },
        "cmv_missing_items_top": sorted(
            (
                {
                    **item,
                    "quantidade_total": round(float(item["quantidade_total"]), 2),
                }
                for item in missing_item_summary.values()
            ),
            key=lambda item: (item["itens"], item["quantidade_total"]),
            reverse=True,
        )[:100],
        "ap_rows_total": ap_rows_total,
        "ap_rows_exact_date": ap_rows_exact_date,
        "ap_rows_fallback_due": ap_rows_fallback_due,
        "nfe_cache_count": len(nfe_paths),
        "sales_cache_count": len(sale_paths),
    }
    return monthly_rows, info


def aging_bucket(delta_days: int) -> str:
    if delta_days < -60:
        return "Vencido >60"
    if delta_days < -30:
        return "Vencido 31-60"
    if delta_days < 0:
        return "Vencido 1-30"
    if delta_days <= 30:
        return "A vencer 0-30"
    if delta_days <= 60:
        return "A vencer 31-60"
    return "A vencer >60"


def build_aging(rows: list[dict], today_dt: datetime) -> list[dict]:
    buckets = {
        "Vencido >60": 0.0,
        "Vencido 31-60": 0.0,
        "Vencido 1-30": 0.0,
        "A vencer 0-30": 0.0,
        "A vencer 31-60": 0.0,
        "A vencer >60": 0.0,
    }
    for row in rows:
        vencimento = row.get("vencimento") or ""
        if not vencimento:
            continue
        try:
            due = datetime.fromisoformat(vencimento)
        except ValueError:
            continue
        delta = (due.date() - today_dt.date()).days
        buckets[aging_bucket(delta)] += float(row.get("valor") or 0.0)
    return [{"bucket": key, "valor": round(value, 2)} for key, value in buckets.items()]


def build_cash_projection(ap_rows: list[dict], ar_rows: list[dict], today_dt: datetime, days: int = 30) -> dict:
    horizon = today_dt.date() + timedelta(days=days)
    daily: dict[str, dict[str, float]] = {}

    def ensure_day(day: str) -> dict[str, float]:
        if day not in daily:
            daily[day] = {"inflow": 0.0, "outflow": 0.0}
        return daily[day]

    def process(rows: list[dict], kind: str) -> list[dict]:
        upcoming: list[dict] = []
        for row in rows:
            vencimento = row.get("vencimento") or ""
            if not vencimento:
                continue
            try:
                due = datetime.fromisoformat(vencimento).date()
            except ValueError:
                continue
            if not (today_dt.date() <= due <= horizon):
                continue
            bucket = ensure_day(due.isoformat())
            value = float(row.get("valor") or 0.0)
            if kind == "inflow":
                bucket["inflow"] += value
            else:
                bucket["outflow"] += value
            upcoming.append(
                {
                    "data": due.isoformat(),
                    "contato": row.get("contato", "N/D"),
                    "valor": round(value, 2),
                    "tipo": kind,
                }
            )
        upcoming.sort(key=lambda item: item["valor"], reverse=True)
        return upcoming

    upcoming_in = process(ar_rows, "inflow")
    upcoming_out = process(ap_rows, "outflow")

    ordered_days = []
    cumulative = 0.0
    for offset in range(days + 1):
        day = (today_dt.date() + timedelta(days=offset)).isoformat()
        values = ensure_day(day)
        net = values["inflow"] - values["outflow"]
        cumulative += net
        ordered_days.append(
            {
                "data": day,
                "inflow": round(values["inflow"], 2),
                "outflow": round(values["outflow"], 2),
                "net": round(net, 2),
                "cumulative_net": round(cumulative, 2),
            }
        )

    negative_days = sum(1 for row in ordered_days if row["cumulative_net"] < 0)
    return {
        "days": ordered_days,
        "inflow_30d": round(sum(row["inflow"] for row in ordered_days), 2),
        "outflow_30d": round(sum(row["outflow"] for row in ordered_days), 2),
        "net_30d": round(sum(row["net"] for row in ordered_days), 2),
        "min_cumulative_30d": round(min(row["cumulative_net"] for row in ordered_days), 2),
        "negative_cumulative_days": negative_days,
        "top_inflows": upcoming_in[:12],
        "top_outflows": upcoming_out[:12],
        "opening_balance_available": False,
    }


def aggregate_sum(rows: list[dict[str, str]], key: str, value_key: str) -> list[dict]:
    bucket: dict[str, float] = {}
    for row in rows:
        name = str(row.get(key) or "N/D").strip() or "N/D"
        bucket[name] = bucket.get(name, 0.0) + to_float(row.get(value_key))
    items = [{"label": label, "valor": round(valor, 2)} for label, valor in bucket.items()]
    items.sort(key=lambda item: item["valor"], reverse=True)
    return items


def aggregate_count(rows: list[dict[str, str]], key: str) -> list[dict]:
    bucket: dict[str, int] = {}
    for row in rows:
        name = str(row.get(key) or "N/D").strip() or "N/D"
        bucket[name] = bucket.get(name, 0) + 1
    items = [{"label": label, "qtd": qtd} for label, qtd in bucket.items()]
    items.sort(key=lambda item: item["qtd"], reverse=True)
    return items


def latest_file(paths: list[Path], pattern: str) -> Path | None:
    matches: list[Path] = []
    for base in paths:
        if base.exists():
            matches.extend([p for p in base.glob(pattern) if p.is_file()])
    if not matches:
        return None
    return sorted(matches, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def load_status_summary(path: Path | None) -> dict:
    if path is None or not path.exists():
        return {"exists": False}
    payload = load_json(path)
    return {
        "exists": True,
        "file": str(path),
        "name": path.name,
        "generated_at": payload.get("generated_at", ""),
        "status": payload.get("status", ""),
        "run_id": payload.get("run_id", ""),
        "checks_summary": payload.get("checks_summary", {}),
        "company_scope": payload.get("company_scope", ""),
        "warnings": payload.get("warnings", []),
    }


def merge_monthly_rows(base_rows: list[dict], proxy_rows: list[dict]) -> list[dict]:
    monthly_by_key = {
        str(row.get("mes") or ""): row
        for row in base_rows
        if row.get("mes") and str(row.get("dre_model") or "") != "bling_proxy"
    }
    for row in proxy_rows:
        monthly_by_key[str(row.get("mes") or "")] = row
    return [monthly_by_key[key] for key in sorted(monthly_by_key) if key]


def build_ap_ar_snapshot(clear_os_root: Path) -> dict:
    today_dt = datetime.now()
    bank_balances = load_bank_balance_snapshot(clear_os_root)
    ap_classification = classify_ap_monthly(clear_os_root, min_year=2023)
    ap_rows = normalize_contas(
        [
            path
            for filename in ["contas_pagar_cache.jsonl", "contas_pagar_cache_cr.jsonl"]
            for path in resolve_cache_candidates(clear_os_root, filename)
        ],
        "pagar",
        open_only=True,
        min_year=2023,
    )
    ap_rows_dre = normalize_contas(
        [
            path
            for filename in ["contas_pagar_cache.jsonl", "contas_pagar_cache_cr.jsonl"]
            for path in resolve_cache_candidates(clear_os_root, filename)
        ],
        "pagar",
        open_only=False,
        min_year=2023,
    )
    ar_rows = normalize_contas(
        [
            path
            for filename in ["contas_receber_cache.jsonl", "contas_receber_cache_cr.jsonl"]
            for path in resolve_cache_candidates(clear_os_root, filename)
        ],
        "receber",
        open_only=True,
        min_year=2023,
    )
    horizon = today_dt + timedelta(days=30)
    ap_aberto = round(sum(float(row["valor"]) for row in ap_rows), 2)
    ar_aberto = round(sum(float(row["valor"]) for row in ar_rows), 2)
    ap_vencido = round(sum(float(row["valor"]) for row in ap_rows if row["dias_atraso"] > 0), 2)
    ar_vencido = round(sum(float(row["valor"]) for row in ar_rows if row["dias_atraso"] > 0), 2)
    ap_30 = round(
        sum(
            float(row["valor"])
            for row in ap_rows
            if row["vencimento"] and parse_date(row["vencimento"]) and today_dt.date() <= parse_date(row["vencimento"]).date() <= horizon.date()
        ),
        2,
    )
    ar_30 = round(
        sum(
            float(row["valor"])
            for row in ar_rows
            if row["vencimento"] and parse_date(row["vencimento"]) and today_dt.date() <= parse_date(row["vencimento"]).date() <= horizon.date()
        ),
        2,
    )
    bling_monthly_rows, bling_info = build_bling_dre_from_erp(clear_os_root, ap_rows_dre)
    return {
        "ap_rows": ap_rows,
        "ar_rows": ar_rows,
        "monthly_bling": bling_monthly_rows,
        "dre_bling_info": bling_info,
        "classic_kpis": {
            "ap_aberto": ap_aberto,
            "ar_aberto": ar_aberto,
            "ap_vencido": ap_vencido,
            "ar_vencido": ar_vencido,
            "fluxo_liquido_previsto_30d": round(ar_30 - ap_30, 2),
            "aging_ap": build_aging(ap_rows, today_dt),
            "aging_ar": build_aging(ar_rows, today_dt),
        },
        "cash_projection": build_cash_projection(ap_rows, ar_rows, today_dt, days=30),
        "bank_balances": bank_balances,
        "ap_details": ap_rows,
        "ar_details": ar_rows,
        "ap_classification": ap_classification,
    }


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    clear_os_root = root.parent
    output_dir = root / "dashboard_online" / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "latest_snapshot.json"
    integration_status_roots = [
        clear_os_root / "logs" / "integration" / "status",
        clear_os_root / "11_agentes_automacoes" / "12_integracoes_agent" / "pipeline" / "out" / "status",
    ]
    latest_path = root / "dre" / "finance-recon-hub" / "out" / "aios" / "monthly-fin-close" / "latest.json"
    if not latest_path.exists():
        if out_path.exists():
            snapshot = load_json(out_path)
            ap_ar_payload = build_ap_ar_snapshot(clear_os_root)
            snapshot.setdefault("classic_kpis", {})
            snapshot["classic_kpis"].update(ap_ar_payload["classic_kpis"])
            snapshot["cash_projection"] = ap_ar_payload["cash_projection"]
            snapshot["bank_balances"] = ap_ar_payload["bank_balances"]
            snapshot["ap_details"] = ap_ar_payload["ap_details"]
            snapshot["ar_details"] = ap_ar_payload["ar_details"]
            snapshot["ap_classification"] = ap_ar_payload["ap_classification"]
            snapshot["monthly_bling"] = ap_ar_payload["monthly_bling"]
            snapshot["dre_bling_info"] = ap_ar_payload["dre_bling_info"]
            snapshot["generated_at"] = datetime.now().isoformat()
            out_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            print(str(out_path))
            return
        raise FileNotFoundError(f"Missing financial close pointer: {latest_path}")

    latest = load_json(latest_path)
    run_dir = Path(str(latest.get("path") or "")).resolve()
    if not run_dir.exists():
        if out_path.exists():
            snapshot = load_json(out_path)
            ap_ar_payload = build_ap_ar_snapshot(clear_os_root)
            snapshot.setdefault("classic_kpis", {})
            snapshot["classic_kpis"].update(ap_ar_payload["classic_kpis"])
            snapshot["cash_projection"] = ap_ar_payload["cash_projection"]
            snapshot["bank_balances"] = ap_ar_payload["bank_balances"]
            snapshot["ap_details"] = ap_ar_payload["ap_details"]
            snapshot["ar_details"] = ap_ar_payload["ar_details"]
            snapshot["ap_classification"] = ap_ar_payload["ap_classification"]
            snapshot["monthly_bling"] = ap_ar_payload["monthly_bling"]
            snapshot["dre_bling_info"] = ap_ar_payload["dre_bling_info"]
            snapshot["generated_at"] = datetime.now().isoformat()
            out_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            print(str(out_path))
            return
        raise FileNotFoundError(f"Missing financial close run dir: {run_dir}")

    exports_dir = run_dir / "control_tower" / "data" / "exports"
    out_dir = run_dir / "control_tower" / "out"

    mensal_rows = load_csv_rows(exports_dir / "dre_mckinsey_mensal.csv")
    resumo_rows = load_csv_rows(exports_dir / "dre_mckinsey_resumo.csv")
    qa_rows = load_csv_rows(exports_dir / "qa_finance_report.csv")
    review_rows = load_csv_rows(exports_dir / "fornecedores_revisao_governanca.csv")
    pending_rows = load_csv_rows(exports_dir / "fornecedores_pendentes_nome.csv")
    ap_rows_classificados = load_csv_rows(exports_dir / "ap_bling_classificado.csv")
    health = load_json(out_dir / "dashboard_healthcheck.json")
    today_dt = datetime.now()

    ap_rows = normalize_contas(
        [
            path
            for filename in ["contas_pagar_cache.jsonl", "contas_pagar_cache_cr.jsonl"]
            for path in resolve_cache_candidates(clear_os_root, filename)
        ],
        "pagar",
        open_only=True,
        min_year=2023,
    )
    ap_rows_dre = normalize_contas(
        [
            path
            for filename in ["contas_pagar_cache.jsonl", "contas_pagar_cache_cr.jsonl"]
            for path in resolve_cache_candidates(clear_os_root, filename)
        ],
        "pagar",
        open_only=False,
        min_year=2023,
    )
    ar_rows = normalize_contas(
        [
            path
            for filename in ["contas_receber_cache.jsonl", "contas_receber_cache_cr.jsonl"]
            for path in resolve_cache_candidates(clear_os_root, filename)
        ],
        "receber",
        open_only=True,
        min_year=2023,
    )

    resumo = resumo_rows[0] if resumo_rows else {}
    warn_count = sum(1 for row in qa_rows if (row.get("status") or "").upper() == "WARN")
    fail_count = sum(1 for row in qa_rows if (row.get("status") or "").upper() == "FAIL")

    negative_months = 0
    for row in qa_rows:
        if row.get("check") == "meses_ebitda_negativo_monitorado":
            details = row.get("details") or ""
            for part in details.split(";"):
                part = part.strip()
                if part.startswith("meses_negativos="):
                    negative_months = int(part.split("=", 1)[1])
                    break

    horizon = today_dt + timedelta(days=30)
    ap_aberto = round(sum(float(row["valor"]) for row in ap_rows), 2)
    ar_aberto = round(sum(float(row["valor"]) for row in ar_rows), 2)
    ap_vencido = round(
        sum(float(row["valor"]) for row in ap_rows if row["vencimento"] and datetime.fromisoformat(row["vencimento"]).date() < today_dt.date()),
        2,
    )
    ar_vencido = round(
        sum(float(row["valor"]) for row in ar_rows if row["vencimento"] and datetime.fromisoformat(row["vencimento"]).date() < today_dt.date()),
        2,
    )
    ap_30 = round(
        sum(
            float(row["valor"])
            for row in ap_rows
            if row["vencimento"] and today_dt.date() <= datetime.fromisoformat(row["vencimento"]).date() <= horizon.date()
        ),
        2,
    )
    ar_30 = round(
        sum(
            float(row["valor"])
            for row in ar_rows
            if row["vencimento"] and today_dt.date() <= datetime.fromisoformat(row["vencimento"]).date() <= horizon.date()
        ),
        2,
    )

    latest_recon_all = latest_file(integration_status_roots, "bling_supabase_reconciliation*_status.json")
    latest_recon_cz = latest_file(integration_status_roots, "bling_supabase_reconciliation*cz*_status.json")
    latest_recon_cr = latest_file(integration_status_roots, "bling_supabase_reconciliation*cr*_status.json")
    latest_ingest = latest_file(integration_status_roots, "finance_ingest_hub*_status.json")
    latest_import = latest_file(integration_status_roots, "bling_import_generator*_status.json")
    latest_cutover = latest_file(integration_status_roots, "check_bling_cutover_health*_status.json")

    quality_checks = health.get("checks", [])
    quality_ok = sum(1 for item in quality_checks if item.get("ok") is True)
    quality_fail = sum(1 for item in quality_checks if item.get("ok") is False)

    bling_monthly_rows, bling_info = build_bling_dre_from_erp(clear_os_root, ap_rows_dre)
    ap_classification = classify_ap_monthly(clear_os_root, min_year=2023)
    legacy_monthly = [
        {
            "mes": row["mes"],
            "mes_num": int(row["mes_num"]),
            "receita_liquida": to_float(row.get("receita_liquida")),
            "custos_variaveis_total": to_float(row.get("custos_variaveis_total")),
            "custo_fixo_base": to_float(row.get("custo_fixo_base")),
            "margem_contribuicao": to_float(row.get("margem_contribuicao")),
            "ebitda": to_float(row.get("ebitda")),
            "dre_model": "legacy_run",
            "dre_source": "finance_recon_hub",
            "despesas_ap_proxy": 0.0,
        }
        for row in mensal_rows
    ]
    snapshot = {
        "generated_at": datetime.now().isoformat(),
        "run_id": run_dir.name,
        "source_run_dir": str(run_dir),
        "health": health,
        "classic_kpis": {
            "ap_aberto": ap_aberto,
            "ar_aberto": ar_aberto,
            "ap_vencido": ap_vencido,
            "ar_vencido": ar_vencido,
            "fluxo_liquido_previsto_30d": round(ar_30 - ap_30, 2),
            "aging_ap": build_aging(ap_rows, today_dt),
            "aging_ar": build_aging(ar_rows, today_dt),
            "quality_status": "PASS" if health.get("ready") else "FAIL",
        },
        "summary": {
            "receita_liquida_total": to_float(resumo.get("receita_liquida_total")),
            "cmv_proxy_total": to_float(resumo.get("cmv_proxy_total")),
            "lucro_bruto_total": to_float(resumo.get("lucro_bruto_total")),
            "custos_variaveis_total": to_float(resumo.get("custos_variaveis_total")),
            "margem_contribuicao_total": to_float(resumo.get("margem_contribuicao_total")),
            "custo_fixo_total": to_float(resumo.get("custo_fixo_total")),
            "ebitda_total": to_float(resumo.get("ebitda_total")),
            "margem_bruta_pct_total": to_float(resumo.get("margem_bruta_pct_total")),
        },
        "monthly": legacy_monthly,
        "monthly_bling": bling_monthly_rows,
        "dre_bling_info": bling_info,
        "qa": {
            "warn_count": warn_count,
            "fail_count": fail_count,
            "negative_ebitda_months": negative_months,
            "checks": qa_rows,
        },
        "governance": {
            "review_count": len(review_rows),
            "review_total_value": round(sum(to_float(row.get("valor_total")) for row in review_rows), 2),
            "pending_count": len(pending_rows),
            "pending_total_value": round(sum(to_float(row.get("valor_total")) for row in pending_rows), 2),
            "top_review": [
                {
                    "fornecedor": row.get("fornecedor", ""),
                    "valor_total": to_float(row.get("valor_total")),
                    "qtd_lancamentos": int(float(row.get("qtd_lancamentos", "0"))),
                    "motivo_governanca": row.get("motivo_governanca", ""),
                }
                for row in review_rows[:10]
            ],
            "top_pending": [
                {
                    "fornecedor": row.get("fornecedor", ""),
                    "valor_total": to_float(row.get("valor_total")),
                    "qtd_lancamentos": int(float(row.get("qtd_lancamentos", "0"))),
                    "motivo_governanca": row.get("motivo_governanca", ""),
                }
                for row in pending_rows[:10]
            ],
        },
        "ap_governance": {
            "ap_total_value": round(sum(to_float(row.get("valor")) for row in ap_rows_classificados), 2),
            "ap_total_rows": len(ap_rows_classificados),
            "mapped_status_value": aggregate_sum(ap_rows_classificados, "status_mapeamento", "valor")[:10],
            "mapped_status_count": aggregate_count(ap_rows_classificados, "status_mapeamento")[:10],
            "category_value": aggregate_sum(ap_rows_classificados, "categoria_mckinsey", "valor")[:12],
            "subcategory_value": aggregate_sum(ap_rows_classificados, "subcategoria_mckinsey", "valor")[:12],
            "top_suppliers": aggregate_sum(ap_rows_classificados, "fornecedor", "valor")[:15],
            "confidence_count": aggregate_count(ap_rows_classificados, "confianca_classificacao")[:10],
        },
        "ap_classification": ap_classification,
        "quality_reconciliation": {
            "quality_check_total": len(quality_checks),
            "quality_check_ok": quality_ok,
            "quality_check_fail": quality_fail,
            "health_ready": bool(health.get("ready")),
            "gate_detail": health.get("quality_gate", {}).get("detail", ""),
            "latest_reconciliation": load_status_summary(latest_recon_all),
            "latest_reconciliation_cz": load_status_summary(latest_recon_cz),
            "latest_reconciliation_cr": load_status_summary(latest_recon_cr),
            "latest_ingest": load_status_summary(latest_ingest),
            "latest_import_generator": load_status_summary(latest_import),
            "latest_cutover_health": load_status_summary(latest_cutover),
        },
        "cash_projection": build_cash_projection(ap_rows, ar_rows, today_dt, days=30),
        "bank_balances": load_bank_balance_snapshot(clear_os_root),
        "ap_details": ap_rows,
        "ar_details": ar_rows,
    }

    out_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(out_path))


if __name__ == "__main__":
    main()
