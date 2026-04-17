from __future__ import annotations

import argparse
import csv
import json
import os
import time
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from client import BlingClient

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT
REPORT_FILE = ROOT / "bling_sync_report.json"
ACCOUNT_ALIASES = {"cz": "CZ", "cr": "CR"}
BANK_ACCOUNT_ENDPOINT_CANDIDATES = [
    "/caixas",
]
BANK_BALANCE_DETAIL_SUFFIXES = [
    "",
    "/saldo",
]


def _company_tag(company: str) -> str:
    tag = ACCOUNT_ALIASES.get((company or "").strip().lower())
    if not tag:
        raise ValueError(f"Unsupported company: {company}")
    return tag


def _cache_path(base_name: str, company: str) -> Path:
    tag = _company_tag(company).lower()
    # Backward compatibility: keep CZ on legacy cache names.
    if tag == "cz":
        return OUT_DIR / base_name
    stem, ext = base_name.rsplit(".", 1)
    return OUT_DIR / f"{stem}_{tag}.{ext}"


def _report_path(company: str) -> Path:
    tag = _company_tag(company).lower()
    if tag == "cz":
        return REPORT_FILE
    return ROOT / f"bling_sync_report_{tag}.json"


def _env_csv_list(name: str) -> list[str]:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _vendor_map_path(company: str) -> Path:
    return _cache_path("vendedores_map.csv", company)


def _bank_accounts_cache_path(company: str) -> Path:
    return _cache_path("contas_financeiras_cache.jsonl", company)


def _read_existing_ids(path: Path) -> set[str]:
    out: set[str] = set()
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                obj = json.loads(ln)
            except Exception:
                continue
            if obj.get("id") is not None:
                out.add(str(obj["id"]))
    return out


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _extract_first_number(payload: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except Exception:
            continue
    return None


def _normalize_text(value: Any) -> str:
    txt = str(value or "").strip().upper()
    if not txt:
        return ""
    txt = "".join(ch for ch in unicodedata.normalize("NFKD", txt) if not unicodedata.combining(ch))
    return " ".join(txt.split())


def _extract_vendedor_info(obj: dict[str, Any]) -> tuple[str | None, str | None]:
    candidates = [
        obj.get("vendedor"),
        obj.get("vendedorResponsavel"),
        obj.get("responsavel"),
        obj.get("representante"),
    ]
    for item in candidates:
        if isinstance(item, dict):
            vid = item.get("id")
            vname = item.get("nome") or item.get("name")
            if vid is not None or vname:
                return (str(vid) if vid is not None else None, str(vname) if vname else None)
    for id_key in ["vendedor_id", "vendedor.id", "vendedorResponsavel.id", "responsavel.id", "representante.id"]:
        if obj.get(id_key) is not None:
            vid = str(obj.get(id_key))
            break
    else:
        vid = None
    for name_key in ["vendedor", "vendedor.nome", "vendedorResponsavel.nome", "responsavel.nome", "representante.nome"]:
        val = obj.get(name_key)
        if isinstance(val, str) and val.strip():
            return vid, val.strip()
    return vid, None


def _merge_nested_objects(base: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in detail.items():
        if key not in out or out.get(key) in (None, "", [], {}):
            out[key] = value
    return out


def _collect_component_dicts(value: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if isinstance(value, list):
        for sub in value:
            items.extend(_collect_component_dicts(sub))
        return items
    if not isinstance(value, dict):
        return items

    # Component records usually carry product/item id and quantity fields.
    id_keys = {"id", "idProduto", "produtoId", "id_produto", "codigo", "produto"}
    qty_keys = {"quantidade", "qtd", "qtde", "quantidadeUtilizada", "quantidadeComponente"}
    has_id = any(k in value for k in id_keys)
    has_qty = any(k in value for k in qty_keys)
    if has_id and has_qty:
        items.append(value)

    for sub in value.values():
        if isinstance(sub, (list, dict)):
            items.extend(_collect_component_dicts(sub))
    return items


def _extract_produto_composicao(detail: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    roots: list[dict[str, Any]] = [detail]
    produto = detail.get("produto")
    if isinstance(produto, dict):
        roots.append(produto)

    candidate_fields = [
        "estrutura",
        "composicao",
        "composição",
        "componentes",
        "itensComposicao",
        "itens_composicao",
        "itensEstrutura",
        "itens_estrutura",
        "listaComponentes",
        "componentesProduto",
    ]
    picked: dict[str, Any] = {}
    for root in roots:
        for field in candidate_fields:
            if field in root and root[field] not in (None, [], {}):
                picked[field] = root[field]

    raw_items: list[dict[str, Any]] = []
    for value in picked.values():
        raw_items.extend(_collect_component_dicts(value))

    # Deduplicate nested items that can appear in more than one branch.
    unique_items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw_items:
        token = json.dumps(item, ensure_ascii=False, sort_keys=True)
        if token in seen:
            continue
        seen.add(token)
        unique_items.append(item)

    return unique_items, sorted(picked.keys())


def _enrich_venda_with_detail(client: BlingClient, row: dict[str, Any]) -> dict[str, Any]:
    rid = row.get("id")
    if rid is None:
        return row
    detail = client.get_detail(f"/pedidos/vendas/{rid}")
    out = _merge_nested_objects(row, detail)
    vid, vname = _extract_vendedor_info(detail)
    if vid is not None:
        out["vendedor_id"] = vid
    if vname:
        out["vendedor"] = vname
    return out


def _enrich_nfe_with_detail(client: BlingClient, row: dict[str, Any]) -> dict[str, Any]:
    rid = row.get("id")
    if rid is None:
        return row
    detail = client.get_detail(f"/nfe/{rid}")
    return _merge_nested_objects(row, detail)


def _enrich_conta_receber_with_detail(client: BlingClient, row: dict[str, Any]) -> dict[str, Any]:
    rid = row.get("id")
    if rid is None:
        return row
    detail = client.get_detail(f"/contas/receber/{rid}")
    return _merge_nested_objects(row, detail)


def _enrich_conta_pagar_with_detail(client: BlingClient, row: dict[str, Any]) -> dict[str, Any]:
    rid = row.get("id")
    if rid is None:
        return row
    detail = client.get_detail(f"/contas/pagar/{rid}")
    return _merge_nested_objects(row, detail)


def _needs_conta_detail_backfill(row: dict[str, Any]) -> bool:
    origem = row.get("origem")
    origem_data = origem.get("dataEmissao") if isinstance(origem, dict) else None
    return not any(
        [
            row.get("dataEmissao"),
            row.get("data_emissao"),
            row.get("emissao"),
            origem_data,
        ]
    )


def _row_year_matches(row: dict[str, Any], years: set[int] | None) -> bool:
    if not years:
        return True
    candidates = [
        row.get("dataEmissao"),
        row.get("data_emissao"),
        row.get("emissao"),
        row.get("competencia"),
        row.get("vencimento"),
        row.get("vencimentoOriginal"),
    ]
    origem = row.get("origem")
    if isinstance(origem, dict):
        candidates.append(origem.get("dataEmissao"))
    for value in candidates:
        if not value:
            continue
        text = str(value).strip()
        if len(text) >= 4 and text[:4].isdigit() and int(text[:4]) in years:
            return True
    return False


def _row_is_open(row: dict[str, Any]) -> bool:
    situacao = row.get("situacao")
    situacao_txt = _normalize_text(situacao)
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


def _conta_backfill_predicate(open_only: bool, years: set[int] | None) -> Any:
    def predicate(row: dict[str, Any]) -> bool:
        if not _needs_conta_detail_backfill(row):
            return False
        if open_only and not _row_is_open(row):
            return False
        if years and not _row_year_matches(row, years):
            return False
        return True

    return predicate


def _backfill_cache_details(
    client: BlingClient,
    cache: Path,
    enrich_row: Any,
    should_refresh: Any,
    limit: int | None = None,
    sleep_s: float = 0.05,
    progress_every: int = 25,
) -> dict[str, int]:
    rows = _read_jsonl(cache)
    if not rows:
        return {"updated": 0, "errors": 0}

    updated = 0
    errors = 0
    rewritten = False
    for idx, row in enumerate(rows):
        if not should_refresh(row):
            continue
        if limit is not None and updated >= limit:
            break
        try:
            enriched = enrich_row(row)
            if isinstance(enriched, dict):
                enriched["empresa"] = row.get("empresa") or _company_tag(client.account)
                rows[idx] = enriched
                updated += 1
                rewritten = True
        except Exception as exc:
            errors += 1
            rows[idx] = dict(row)
            rows[idx]["enrich_error"] = str(exc)[:180]
            rewritten = True
        if progress_every and (updated + errors) % progress_every == 0 and (updated + errors) > 0:
            print(
                f"[INFO] backfill {cache.name} {client.account}: "
                f"processados={updated + errors} updated={updated} errors={errors}"
            )
        time.sleep(sleep_s)

    if rewritten:
        _rewrite_jsonl(cache, rows)
    return {"updated": updated, "errors": errors}


def _build_vendedores_map_rows(rows: list[dict[str, Any]], company: str) -> list[dict[str, str]]:
    by_id: dict[str, dict[str, str]] = {}
    for row in rows:
        vid, vname = _extract_vendedor_info(row)
        if vid is None:
            continue
        key = str(vid).strip()
        if not key:
            continue
        name = str(vname or "").strip()
        current = by_id.get(key)
        if current is None or (not current["vendedor"] and name):
            by_id[key] = {
                "vendedor_id": key,
                "vendedor": name,
                "empresa": _company_tag(company),
            }
    return sorted(by_id.values(), key=lambda item: item["vendedor_id"])


def _write_vendedores_map(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["vendedor_id", "vendedor", "empresa"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                rows.append(json.loads(ln))
            except Exception:
                continue
    return rows


def _rewrite_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _sync_paginated(
    client: BlingClient,
    endpoint: str,
    cache_file: Path,
    company: str,
    params: dict[str, Any] | None = None,
    enrich_row: Any | None = None,
    max_pages: int | None = None,
    sleep_s: float = 0.15,
    progress_every_pages: int = 1,
) -> dict[str, Any]:
    base_params = dict(params or {})
    seen = _read_existing_ids(cache_file)
    page = 1
    total_new = 0
    total_fetched = 0
    enriched_ok = 0
    enriched_err = 0
    started = time.time()

    while True:
        query = dict(base_params)
        query["pagina"] = page
        query["limite"] = 100
        rows = client.get_data(endpoint, query)
        if not rows:
            break
        total_fetched += len(rows)
        batch: list[dict[str, Any]] = []
        for r in rows:
            rid = r.get("id")
            if rid is None:
                continue
            key = str(rid)
            if key in seen:
                continue
            seen.add(key)
            candidate = r
            if enrich_row is not None:
                try:
                    maybe = enrich_row(r)
                    if isinstance(maybe, dict):
                        candidate = maybe
                    enriched_ok += 1
                except Exception as exc:
                    enriched_err += 1
                    candidate = dict(r)
                    candidate["enrich_error"] = str(exc)[:180]
            candidate["empresa"] = _company_tag(company)
            batch.append(candidate)
        _append_jsonl(cache_file, batch)
        total_new += len(batch)
        if progress_every_pages and page % progress_every_pages == 0:
            print(
                f"[INFO] {endpoint} {company}: pagina={page} "
                f"lidos={total_fetched} novos={total_new} "
                f"enrich_ok={enriched_ok} enrich_err={enriched_err}"
            )
        page += 1
        if max_pages and page > max_pages:
            break
        time.sleep(sleep_s)

    return {
        "endpoint": endpoint,
        "cache_file": str(cache_file),
        "pages": page - 1,
        "fetched": total_fetched,
        "new_records": total_new,
        "enriched_ok": enriched_ok,
        "enriched_err": enriched_err,
        "elapsed_s": round(time.time() - started, 2),
    }


def _sync_paginated_snapshot(
    client: BlingClient,
    endpoint: str,
    cache_file: Path,
    company: str,
    params: dict[str, Any] | None = None,
    enrich_row: Any | None = None,
    row_filter: Any | None = None,
    append: bool = False,
    max_pages: int | None = None,
    sleep_s: float = 0.15,
    progress_every_pages: int = 1,
) -> dict[str, Any]:
    base_params = dict(params or {})
    page = 1
    total_fetched = 0
    enriched_ok = 0
    enriched_err = 0
    started = time.time()
    snapshot_rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    while True:
        query = dict(base_params)
        query["pagina"] = page
        query["limite"] = 100
        rows = client.get_data(endpoint, query)
        if not rows:
            break
        total_fetched += len(rows)
        for r in rows:
            rid = r.get("id")
            if rid is None:
                continue
            if row_filter is not None:
                try:
                    if not row_filter(r):
                        continue
                except Exception:
                    continue
            key = str(rid)
            if key in seen_ids:
                continue
            seen_ids.add(key)
            candidate = r
            if enrich_row is not None:
                try:
                    maybe = enrich_row(r)
                    if isinstance(maybe, dict):
                        candidate = maybe
                    enriched_ok += 1
                except Exception as exc:
                    enriched_err += 1
                    candidate = dict(r)
                    candidate["enrich_error"] = str(exc)[:180]
            candidate["empresa"] = _company_tag(company)
            snapshot_rows.append(candidate)
        if progress_every_pages and page % progress_every_pages == 0:
            print(
                f"[INFO] {endpoint} {company}: pagina={page} "
                f"lidos={total_fetched} snapshot={len(snapshot_rows)} "
                f"enrich_ok={enriched_ok} enrich_err={enriched_err}"
            )
        page += 1
        if max_pages and page > max_pages:
            break
        time.sleep(sleep_s)

    if append:
        _append_jsonl(cache_file, snapshot_rows)
    else:
        _rewrite_jsonl(cache_file, snapshot_rows)
    return {
        "endpoint": endpoint,
        "cache_file": str(cache_file),
        "pages": page - 1,
        "fetched": total_fetched,
        "new_records": len(snapshot_rows),
        "enriched_ok": enriched_ok,
        "enriched_err": enriched_err,
        "elapsed_s": round(time.time() - started, 2),
    }


def _year_range(year: int) -> tuple[str, str]:
    return f"{year}-01-01", f"{year}-12-31"


def _date_window_params(start_date: str | None, end_date: str | None) -> dict[str, str]:
    params: dict[str, str] = {}
    if start_date:
        params["dataEmissaoInicial"] = start_date
    if end_date:
        params["dataEmissaoFinal"] = end_date
    return params


def sync_vendas(client: BlingClient, year: int, max_pages: int | None) -> dict[str, Any]:
    start_date, end_date = _year_range(year)
    cache = _cache_path(f"vendas_{year}_cache.jsonl", client.account)
    params = {"dataEmissaoInicial": start_date, "dataEmissaoFinal": end_date}
    result = _sync_paginated(
        client,
        "/pedidos/vendas",
        cache,
        company=client.account,
        params=params,
        enrich_row=lambda r: _enrich_venda_with_detail(client, r),
        max_pages=max_pages,
    )
    vendor_rows = _build_vendedores_map_rows(_read_jsonl(cache), client.account)
    vendor_map = _vendor_map_path(client.account)
    _write_vendedores_map(vendor_map, vendor_rows)
    result["vendor_map_file"] = str(vendor_map)
    result["vendor_map_rows"] = len(vendor_rows)
    return result


def backfill_vendas_vendedor(client: BlingClient, year: int, limit: int | None = None) -> dict[str, Any]:
    cache = _cache_path(f"vendas_{year}_cache.jsonl", client.account)
    rows = _read_jsonl(cache)
    if not rows:
        return {"cache_file": str(cache), "records": 0, "updated": 0, "errors": 0, "elapsed_s": 0}

    started = time.time()
    updated = 0
    errors = 0
    touched = 0

    for i, row in enumerate(rows):
        has_name = bool(_normalize_text(row.get("vendedor")))
        has_id = row.get("vendedor_id") not in (None, "", 0, "0")
        has_items = isinstance(row.get("itens"), list) and len(row.get("itens")) > 0
        if has_name and has_id and has_items:
            continue
        try:
            rows[i] = _enrich_venda_with_detail(client, row)
            updated += 1
        except Exception:
            errors += 1
        touched += 1
        if limit and touched >= limit:
            break
        time.sleep(0.03)

    if updated:
        _rewrite_jsonl(cache, rows)

    vendor_rows = _build_vendedores_map_rows(rows, client.account)
    vendor_map = _vendor_map_path(client.account)
    _write_vendedores_map(vendor_map, vendor_rows)

    return {
        "cache_file": str(cache),
        "vendor_map_file": str(vendor_map),
        "vendor_map_rows": len(vendor_rows),
        "records": len(rows),
        "updated": updated,
        "errors": errors,
        "elapsed_s": round(time.time() - started, 2),
    }


def sync_nfe(client: BlingClient, year: int, max_pages: int | None) -> dict[str, Any]:
    start_date, end_date = _year_range(year)
    cache = _cache_path(f"nfe_{year}_cache.jsonl", client.account)
    params = {"dataEmissaoInicial": start_date, "dataEmissaoFinal": end_date}
    return _sync_paginated(
        client,
        "/nfe",
        cache,
        company=client.account,
        params=params,
        enrich_row=lambda r: _enrich_nfe_with_detail(client, r),
        max_pages=max_pages,
    )


def backfill_nfe_detail(client: BlingClient, year: int, limit: int | None = None) -> dict[str, Any]:
    cache = _cache_path(f"nfe_{year}_cache.jsonl", client.account)
    rows = _read_jsonl(cache)
    if not rows:
        return {"cache_file": str(cache), "records": 0, "updated": 0, "errors": 0, "elapsed_s": 0}

    started = time.time()
    updated = 0
    errors = 0
    touched = 0

    for i, row in enumerate(rows):
        has_value = row.get("valorNota") not in (None, "", 0, "0")
        has_contact = isinstance(row.get("contato"), dict) and bool(row.get("contato"))
        if has_value and has_contact:
            continue
        try:
            rows[i] = _enrich_nfe_with_detail(client, row)
            updated += 1
        except Exception:
            errors += 1
        touched += 1
        if limit and touched >= limit:
            break
        time.sleep(0.03)

    if updated:
        _rewrite_jsonl(cache, rows)

    return {
        "cache_file": str(cache),
        "records": len(rows),
        "updated": updated,
        "errors": errors,
        "elapsed_s": round(time.time() - started, 2),
    }


def sync_contatos(client: BlingClient, max_pages: int | None) -> dict[str, Any]:
    cache = _cache_path("contatos_cache.jsonl", client.account)
    return _sync_paginated(client, "/contatos", cache, company=client.account, params={}, max_pages=max_pages)


def sync_produtos(client: BlingClient, max_pages: int | None) -> dict[str, Any]:
    cache = _cache_path("produtos_cache.jsonl", client.account)
    return _sync_paginated(client, "/produtos", cache, company=client.account, params={}, max_pages=max_pages)


def sync_produtos_composicao(client: BlingClient, max_pages: int | None) -> dict[str, Any]:
    cache = _cache_path("produtos_composicao_cache.jsonl", client.account)
    started = time.time()
    page = 1
    total_products = 0
    detail_errors = 0
    rows: list[dict[str, Any]] = []
    processed = 0

    while True:
        produtos = client.get_data("/produtos", {"pagina": page, "limite": 100})
        if not produtos:
            break
        total_products += len(produtos)
        for prod in produtos:
            pid = prod.get("id")
            if pid is None:
                continue
            row: dict[str, Any] = {
                "id": pid,
                "produto.id": pid,
                "produto.nome": prod.get("nome") or prod.get("descricao") or "",
                "produto.codigo": prod.get("codigo") or "",
                "produto.tipo": prod.get("tipo") or "",
                "produto.formato": prod.get("formato") or "",
                "empresa": client.account,
                "snapshot_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            try:
                detail = client.get_detail(f"/produtos/{pid}")
                componentes, campos = _extract_produto_composicao(detail)
                row["tem_composicao"] = bool(componentes)
                row["composicao_campos"] = campos
                row["composicao_itens_qtd"] = len(componentes)
                row["composicao_itens"] = componentes
            except Exception as exc:
                detail_errors += 1
                row["tem_composicao"] = False
                row["composicao_campos"] = []
                row["composicao_itens_qtd"] = 0
                row["composicao_itens"] = []
                row["detail_error"] = str(exc)[:180]
            rows.append(row)
            processed += 1
            if processed % 25 == 0:
                with_composicao_partial = sum(1 for r in rows if r.get("tem_composicao"))
                print(
                    f"[INFO] produtos_composicao {client.account}: processados={processed} "
                    f"composicao={with_composicao_partial} erros={detail_errors}"
                )
            if processed % 100 == 0:
                _rewrite_jsonl(cache, rows)
        page += 1
        if max_pages and page > max_pages:
            break
        time.sleep(0.12)

    _rewrite_jsonl(cache, rows)
    with_composicao = sum(1 for r in rows if r.get("tem_composicao"))
    return {
        "endpoint": "/produtos + /produtos/{id}",
        "cache_file": str(cache),
        "pages": page - 1,
        "fetched": total_products,
        "new_records": len(rows),
        "with_composition": with_composicao,
        "detail_errors": detail_errors,
        "elapsed_s": round(time.time() - started, 2),
    }


def _bank_account_endpoint_candidates() -> list[str]:
    custom = _env_csv_list("BLING_BANK_ACCOUNT_ENDPOINTS")
    return custom or list(BANK_ACCOUNT_ENDPOINT_CANDIDATES)


def _fetch_bank_account_detail(client: BlingClient, base_endpoint: str, row: dict[str, Any]) -> dict[str, Any]:
    rid = row.get("id")
    if rid is None:
        return row
    out = dict(row)
    detail_errors: list[str] = []
    for suffix in BANK_BALANCE_DETAIL_SUFFIXES:
        try:
            detail = client.get_detail(f"{base_endpoint}/{rid}{suffix}")
        except Exception as exc:
            detail_errors.append(str(exc)[:180])
            continue
        if isinstance(detail, dict):
            out = _merge_nested_objects(out, detail)
            balance = _extract_first_number(
                detail,
                [
                    "saldo",
                    "saldoAtual",
                    "saldo_atual",
                    "saldoDisponivel",
                    "saldo_disponivel",
                    "valorSaldo",
                    "valor_saldo",
                ],
            )
            if balance is not None:
                out["saldo"] = balance
            bank_info = detail.get("banco")
            if isinstance(bank_info, dict):
                if bank_info.get("nome") and not out.get("banco"):
                    out["banco"] = bank_info.get("nome")
                if bank_info.get("nome"):
                    out["banco.nome"] = bank_info.get("nome")
            for source_key, target_key in [
                ("descricao", "descricaoConta"),
                ("nome", "descricaoConta"),
            ]:
                if out.get(source_key) and not out.get(target_key):
                    out[target_key] = out.get(source_key)
            if out.get("saldo") is not None and not out.get("saldoAtual"):
                out["saldoAtual"] = out["saldo"]
    if detail_errors and "saldo" not in out:
        out["detail_error"] = detail_errors[-1]
    return out


def sync_contas_financeiras(client: BlingClient, max_pages: int | None) -> dict[str, Any]:
    cache = _bank_accounts_cache_path(client.account)
    started = time.time()
    last_error = ""
    date_start = str(os.getenv("BLING_CAIXAS_DATE_START") or "2024-01-01").strip()
    date_end = date.today().isoformat()
    for endpoint in _bank_account_endpoint_candidates():
        try:
            page = 1
            total_fetched = 0
            movement_rows: list[dict[str, Any]] = []
            while True:
                rows = client.get_data(endpoint, {"pagina": page, "limite": 100})
                if not rows:
                    break
                total_fetched += len(rows)
                movement_rows.extend(rows)
                print(f"[INFO] {endpoint} {client.account}: pagina={page} lidos={total_fetched}")
                page += 1
                if max_pages and page > max_pages:
                    break
                time.sleep(0.35)

            accounts: dict[str, dict[str, Any]] = {}
            for row in movement_rows:
                conta = row.get("contaFinanceira") or {}
                if not isinstance(conta, dict):
                    continue
                account_id = str(conta.get("id") or "").strip()
                if not account_id:
                    continue
                account_name = str(conta.get("descricao") or conta.get("nome") or "").strip()
                current = accounts.get(account_id)
                if current is None:
                    current = {
                        "id": account_id,
                        "contaFinanceira": conta,
                        "descricaoConta": account_name,
                        "nome": account_name,
                        "saldoDerivadoMovimentos": 0.0,
                        "movimentos": 0,
                        "empresa": client.account,
                        "snapshot_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    accounts[account_id] = current
                try:
                    current["saldoDerivadoMovimentos"] = round(
                        float(current["saldoDerivadoMovimentos"]) + float(row.get("valor") or 0.0), 2
                    )
                except Exception:
                    pass
                current["movimentos"] = int(current["movimentos"]) + 1
                current["data_ultimo_movimento"] = str(row.get("data") or current.get("data_ultimo_movimento") or "")

            for account_id, current in accounts.items():
                saldo = 0.0
                movimentos = 0
                latest_date = str(current.get("data_ultimo_movimento") or "")
                start_dt = datetime.fromisoformat(date_start).date()
                end_dt = datetime.fromisoformat(date_end).date()
                window_start = start_dt
                while window_start <= end_dt:
                    window_end = min(window_start + timedelta(days=365), end_dt)
                    page = 1
                    while True:
                        params = {
                            "idContaFinanceira": account_id,
                            "dataInicial": window_start.isoformat(),
                            "dataFinal": window_end.isoformat(),
                            "pagina": page,
                            "limite": 100,
                        }
                        rows = client.get_data(endpoint, params)
                        if not rows:
                            break
                        for row in rows:
                            try:
                                saldo += float(row.get("valor") or 0.0)
                            except Exception:
                                continue
                            movimentos += 1
                            row_date = str(row.get("data") or "")
                            if row_date and row_date >= latest_date:
                                latest_date = row_date
                        page += 1
                        if max_pages and page > max_pages:
                            break
                        time.sleep(0.2)
                    window_start = window_end + timedelta(days=1)
                current["saldoDerivadoMovimentos"] = round(saldo, 2)
                current["movimentos"] = movimentos
                current["data_ultimo_movimento"] = latest_date
                current["periodo_consulta_inicial"] = date_start
                current["periodo_consulta_final"] = date_end

            snapshot_rows: list[dict[str, Any]] = []
            enriched_ok = 0
            enriched_err = 0
            for account in accounts.values():
                candidate = dict(account)
                try:
                    maybe = _fetch_bank_account_detail(client, endpoint, candidate)
                    if isinstance(maybe, dict):
                        candidate = maybe
                    enriched_ok += 1
                except Exception as exc:
                    enriched_err += 1
                    candidate["enrich_error"] = str(exc)[:180]
                snapshot_rows.append(candidate)

            _rewrite_jsonl(cache, snapshot_rows)
            result = {
                "endpoint": endpoint,
                "cache_file": str(cache),
                "pages": page - 1,
                "fetched": total_fetched,
                "new_records": len(snapshot_rows),
                "enriched_ok": enriched_ok,
                "enriched_err": enriched_err,
                "elapsed_s": round(time.time() - started, 2),
            }
            return {
                "endpoint": endpoint,
                "cache_file": str(cache),
                "pages": result["pages"],
                "fetched": result["fetched"],
                "new_records": result["new_records"],
                "enriched_ok": result["enriched_ok"],
                "enriched_err": result["enriched_err"],
                "elapsed_s": round(time.time() - started, 2),
            }
        except Exception as exc:
            last_error = str(exc)
            continue
    raise RuntimeError(
        "Nenhum endpoint de caixas e bancos respondeu. "
        f"Tentados: {', '.join(_bank_account_endpoint_candidates())}. Ultimo erro: {last_error}"
    )


def sync_contas_receber(
    client: BlingClient,
    max_pages: int | None,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    backfill_only: bool = False,
    backfill_open_only: bool = False,
    backfill_years: set[int] | None = None,
    backfill_limit: int | None = None,
) -> dict[str, Any]:
    cache = _cache_path("contas_receber_cache.jsonl", client.account)
    if backfill_only:
        fetched = 0
        new_records = 0
    else:
        params = _date_window_params(start_date, end_date)
        result = _sync_paginated_snapshot(
            client,
            "/contas/receber",
            cache,
            company=client.account,
            params=params,
            enrich_row=lambda r: _enrich_conta_receber_with_detail(client, r),
            max_pages=max_pages,
            sleep_s=0.4,
        )
        fetched = result["fetched"]
        new_records = result["new_records"]
    backfill = _backfill_cache_details(
        client,
        cache,
        enrich_row=lambda r: _enrich_conta_receber_with_detail(client, r),
        should_refresh=_conta_backfill_predicate(backfill_open_only, backfill_years),
        limit=backfill_limit,
        sleep_s=0.4,
    )
    return {
        "endpoint": "/contas/receber",
        "cache_file": str(cache),
        "pages": 0 if backfill_only else result["pages"],
        "fetched": fetched,
        "new_records": new_records,
        "enriched_ok": 0 if backfill_only else result["enriched_ok"],
        "enriched_err": 0 if backfill_only else result["enriched_err"],
        "backfill_updated": backfill["updated"],
        "backfill_errors": backfill["errors"],
        "elapsed_s": 0 if backfill_only else result["elapsed_s"],
    }


def sync_contas_pagar(
    client: BlingClient,
    max_pages: int | None,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    backfill_only: bool = False,
    backfill_open_only: bool = False,
    backfill_years: set[int] | None = None,
    backfill_limit: int | None = None,
) -> dict[str, Any]:
    cache = _cache_path("contas_pagar_cache.jsonl", client.account)
    if backfill_only:
        fetched = 0
        new_records = 0
    else:
        params = _date_window_params(start_date, end_date)
        result = _sync_paginated_snapshot(
            client,
            "/contas/pagar",
            cache,
            company=client.account,
            params=params,
            enrich_row=lambda r: _enrich_conta_pagar_with_detail(client, r),
            max_pages=max_pages,
            sleep_s=0.4,
        )
        fetched = result["fetched"]
        new_records = result["new_records"]
    backfill = _backfill_cache_details(
        client,
        cache,
        enrich_row=lambda r: _enrich_conta_pagar_with_detail(client, r),
        should_refresh=_conta_backfill_predicate(backfill_open_only, backfill_years),
        limit=backfill_limit,
        sleep_s=0.4,
    )
    return {
        "endpoint": "/contas/pagar",
        "cache_file": str(cache),
        "pages": 0 if backfill_only else result["pages"],
        "fetched": fetched,
        "new_records": new_records,
        "enriched_ok": 0 if backfill_only else result["enriched_ok"],
        "enriched_err": 0 if backfill_only else result["enriched_err"],
        "backfill_updated": backfill["updated"],
        "backfill_errors": backfill["errors"],
        "elapsed_s": 0 if backfill_only else result["elapsed_s"],
    }


def sync_estoque(client: BlingClient, max_pages: int | None) -> dict[str, Any]:
    cache = _cache_path("estoque_cache.jsonl", client.account)
    started = time.time()
    page = 1
    total_products = 0
    snapshot_rows: list[dict[str, Any]] = []

    while True:
        produtos = client.get_data("/produtos", {"pagina": page, "limite": 100})
        if not produtos:
            break
        total_products += len(produtos)
        for prod in produtos:
            pid = prod.get("id")
            if pid is None:
                continue
            try:
                saldos = client.get_data("/estoques/saldos", {"idProduto": pid})
            except Exception:
                saldos = []

            saldo_total = 0.0
            for s in saldos:
                for k in ["saldoFisicoTotal", "saldoVirtualTotal", "saldo", "estoque", "quantidade"]:
                    if k in s:
                        try:
                            saldo_total += float(s.get(k) or 0)
                        except Exception:
                            pass
                        break

            snapshot_rows.append(
                {
                    "id": pid,
                    "produto.id": pid,
                    "produto.nome": prod.get("nome") or prod.get("descricao") or "",
                    "produto.codigo": prod.get("codigo") or "",
                    "saldoFisicoTotal": saldo_total,
                    "saldos_raw": saldos,
                    "empresa": client.account,
                    "snapshot_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
        page += 1
        if max_pages and page > max_pages:
            break
        time.sleep(0.12)

    # snapshot completo (sobrescreve) para refletir estoque atual
    with cache.open("w", encoding="utf-8") as f:
        for r in snapshot_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    return {
        "endpoint": "/estoques/saldos?idProduto",
        "cache_file": str(cache),
        "pages": page - 1,
        "fetched": total_products,
        "new_records": len(snapshot_rows),
        "elapsed_s": round(time.time() - started, 2),
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sincronizacao Bling ERP (multimodulo).")
    p.add_argument(
        "--company",
        default="cz",
        choices=["cz", "cr", "CZ", "CR"],
        help="Empresa/conta Bling: cz (padrao) ou cr.",
    )
    p.add_argument("--year", type=int, default=date.today().year)
    p.add_argument(
        "--modules",
        default="vendas,nfe,contatos,produtos,contas_receber,contas_pagar,estoque",
        help="Lista separada por virgula: vendas,nfe,contatos,produtos,produtos_composicao,contas_receber,contas_pagar,estoque,contas_financeiras",
    )
    p.add_argument("--max-pages", type=int, default=None, help="Limite de paginas por modulo (teste rapido).")
    p.add_argument(
        "--backfill-vendas-vendedor",
        action="store_true",
        help="Preenche vendedor_id/vendedor no cache de vendas via detalhe do pedido.",
    )
    p.add_argument(
        "--backfill-nfe-detail",
        action="store_true",
        help="Preenche valorNota e demais campos no cache de NF-e via detalhe da nota.",
    )
    p.add_argument(
        "--backfill-limit",
        type=int,
        default=None,
        help="Limite de registros pendentes para backfill (opcional).",
    )
    p.add_argument(
        "--contas-backfill-only",
        action="store_true",
        help="Nao pagina as listas de contas; apenas enriquece o cache local existente.",
    )
    p.add_argument(
        "--contas-open-only",
        action="store_true",
        help="Em contas, faz backfill somente de titulos em aberto.",
    )
    p.add_argument(
        "--contas-years",
        default="",
        help="Lista separada por virgula dos anos para o backfill de contas, ex.: 2025,2026",
    )
    p.add_argument(
        "--contas-date-start",
        default="",
        help="Data inicial para sync de contas, formato YYYY-MM-DD.",
    )
    p.add_argument(
        "--contas-date-end",
        default="",
        help="Data final para sync de contas, formato YYYY-MM-DD.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    company = _company_tag(args.company)
    selected = {m.strip().lower() for m in args.modules.split(",") if m.strip()}
    conta_years = {
        int(part.strip())
        for part in str(args.contas_years or "").split(",")
        if part.strip().isdigit()
    } or None
    contas_date_start = str(args.contas_date_start or "").strip() or None
    contas_date_end = str(args.contas_date_end or "").strip() or None
    client = BlingClient(account=company)
    report: dict[str, Any] = {
        "company": company,
        "year": args.year,
        "modules": sorted(selected),
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "results": [],
        "errors": [],
    }

    steps = []
    if "vendas" in selected:
        steps.append(("vendas", lambda: sync_vendas(client, args.year, args.max_pages)))
    if "nfe" in selected:
        steps.append(("nfe", lambda: sync_nfe(client, args.year, args.max_pages)))
    if "contatos" in selected:
        steps.append(("contatos", lambda: sync_contatos(client, args.max_pages)))
    if "produtos" in selected:
        steps.append(("produtos", lambda: sync_produtos(client, args.max_pages)))
    if "produtos_composicao" in selected:
        steps.append(("produtos_composicao", lambda: sync_produtos_composicao(client, args.max_pages)))
    if "contas_receber" in selected:
        steps.append(
            (
                "contas_receber",
                lambda: sync_contas_receber(
                    client,
                    args.max_pages,
                    start_date=contas_date_start,
                    end_date=contas_date_end,
                    backfill_only=args.contas_backfill_only,
                    backfill_open_only=args.contas_open_only,
                    backfill_years=conta_years,
                    backfill_limit=args.backfill_limit,
                ),
            )
        )
    if "contas_pagar" in selected:
        steps.append(
            (
                "contas_pagar",
                lambda: sync_contas_pagar(
                    client,
                    args.max_pages,
                    start_date=contas_date_start,
                    end_date=contas_date_end,
                    backfill_only=args.contas_backfill_only,
                    backfill_open_only=args.contas_open_only,
                    backfill_years=conta_years,
                    backfill_limit=args.backfill_limit,
                ),
            )
        )
    if "estoque" in selected:
        steps.append(("estoque", lambda: sync_estoque(client, args.max_pages)))
    if "contas_financeiras" in selected:
        steps.append(("contas_financeiras", lambda: sync_contas_financeiras(client, args.max_pages)))

    for name, fn in steps:
        try:
            result = fn()
            report["results"].append({"module": name, **result})
            print(f"[OK] {name}: +{result['new_records']} novos ({result['fetched']} lidos)")
        except Exception as exc:
            report["errors"].append({"module": name, "error": str(exc)})
            print(f"[ERR] {name}: {exc}")

    if args.backfill_vendas_vendedor:
        try:
            bf = backfill_vendas_vendedor(client, args.year, args.backfill_limit)
            report["results"].append({"module": "vendas_backfill", **bf})
            print(f"[OK] vendas_backfill: {bf['updated']} atualizados ({bf['errors']} erros)")
        except Exception as exc:
            report["errors"].append({"module": "vendas_backfill", "error": str(exc)})
            print(f"[ERR] vendas_backfill: {exc}")

    if args.backfill_nfe_detail:
        try:
            bf = backfill_nfe_detail(client, args.year, args.backfill_limit)
            report["results"].append({"module": "nfe_backfill", **bf})
            print(f"[OK] nfe_backfill: {bf['updated']} atualizados ({bf['errors']} erros)")
        except Exception as exc:
            report["errors"].append({"module": "nfe_backfill", "error": str(exc)})
            print(f"[ERR] nfe_backfill: {exc}")

    report["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        report_file = _report_path(company)
        report_file.parent.mkdir(parents=True, exist_ok=True)
        report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Relatorio: {report_file}")
    except Exception as exc:
        # Nao interrompe o sync quando apenas o persist de relatorio falha.
        print(f"[WARN] Falha ao gravar relatorio: {exc}")
    return 0 if not report["errors"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
