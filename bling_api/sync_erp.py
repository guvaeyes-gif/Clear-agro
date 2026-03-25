from __future__ import annotations

import argparse
import csv
import json
import time
import unicodedata
from datetime import date
from pathlib import Path
from typing import Any

from client import BlingClient

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT
REPORT_FILE = ROOT / "bling_sync_report.json"
ACCOUNT_ALIASES = {"cz": "CZ", "cr": "CR"}


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


def _vendor_map_path(company: str) -> Path:
    return _cache_path("vendedores_map.csv", company)


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


def _year_range(year: int) -> tuple[str, str]:
    return f"{year}-01-01", f"{year}-12-31"


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


def sync_contas_receber(client: BlingClient, max_pages: int | None) -> dict[str, Any]:
    cache = _cache_path("contas_receber_cache.jsonl", client.account)
    return _sync_paginated(client, "/contas/receber", cache, company=client.account, params={}, max_pages=max_pages)


def sync_contas_pagar(client: BlingClient, max_pages: int | None) -> dict[str, Any]:
    cache = _cache_path("contas_pagar_cache.jsonl", client.account)
    return _sync_paginated(client, "/contas/pagar", cache, company=client.account, params={}, max_pages=max_pages)


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
        help="Lista separada por virgula: vendas,nfe,contatos,produtos,produtos_composicao,contas_receber,contas_pagar,estoque",
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
    return p.parse_args()


def main() -> int:
    args = parse_args()
    company = _company_tag(args.company)
    selected = {m.strip().lower() for m in args.modules.split(",") if m.strip()}
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
        steps.append(("contas_receber", lambda: sync_contas_receber(client, args.max_pages)))
    if "contas_pagar" in selected:
        steps.append(("contas_pagar", lambda: sync_contas_pagar(client, args.max_pages)))
    if "estoque" in selected:
        steps.append(("estoque", lambda: sync_estoque(client, args.max_pages)))

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
