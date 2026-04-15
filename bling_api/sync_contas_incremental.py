"""
Sync incremental de Contas a Pagar e Receber do Bling.

Este script busca apenas os registros modificados/apartir da última sincronização.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sync_erp import _cache_path, _company_tag  # noqa: E402

STATUS_DIR = ROOT.parent / "logs" / "integration" / "status"
FORCE_FULL_HORIZON = "2035-12-31"


def get_last_sync_date(company: str) -> str:
    """Obter a data da última sincronização a partir dos arquivos de status."""
    tag = _company_tag(company).lower()
    
    # Padrão de busca
    if tag == "cz":
        pattern = "*sync_bling_cache_roots*status.json"
    else:
        pattern = f"*sync_bling_cache_roots*{tag}*status.json"
    
    # Buscar o arquivo de status mais recente
    status_files = list(STATUS_DIR.glob(pattern))
    if not status_files:
        # Fallback: 7 dias atrás
        return (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    status_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    latest = status_files[0]
    
    try:
        with open(latest, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        generated_at = data.get("generated_at")
        if generated_at:
            # Parse ISO format e converter para date
            dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            # Subtrair 1 dia para garantir que pegamos tudo desde a última sync
            sync_date = dt.date() - timedelta(days=1)
            return sync_date.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"[WARN] Erro ao ler status file {latest}: {e}")
    
    # Fallback
    return (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")


def get_last_record_date(cache_file: Path) -> str | None:
    """Obter a data do registro mais recente no cache local."""
    if not cache_file.exists():
        return None
    
    latest_date = None
    with open(cache_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                # Tentar obter data de modificação ou vencimento
                for date_field in ["dataModificacao", "dataEmissao", "vencimento", "updatedAt", "createdAt"]:
                    if date_field in obj and obj[date_field]:
                        date_str = obj[date_field][:10]  # YYYY-MM-DD
                        if latest_date is None or date_str > latest_date:
                            latest_date = date_str
                        break
            except json.JSONDecodeError:
                continue
    
    return latest_date


def split_date_ranges(date_from: str, date_to: str, max_span_days: int = 365) -> list[tuple[str, str]]:
    start = datetime.strptime(date_from, "%Y-%m-%d").date()
    end = datetime.strptime(date_to, "%Y-%m-%d").date()
    if end < start:
        return []
    ranges: list[tuple[str, str]] = []
    current = start
    step = timedelta(days=max_span_days)
    while current <= end:
        window_end = min(current + step, end)
        ranges.append((current.isoformat(), window_end.isoformat()))
        current = window_end + timedelta(days=1)
    return ranges


def _row_matches_window(row: dict, chunk_start: str, chunk_end: str) -> bool:
    vencimento = str(row.get("vencimento") or "").strip()
    if not vencimento:
        return False
    if vencimento < chunk_start or vencimento > chunk_end:
        return False
    try:
        situacao = row.get("situacao")
        if situacao is not None and int(float(situacao)) != 1:
            return False
    except Exception:
        return False
    situacao_txt = str(row.get("situacao") or "").strip().upper()
    if "CANCEL" in situacao_txt:
        return False
    return True


def sync_contas_incremental(
    client,
    company: str,
    module: str,  # "contas_pagar" ou "contas_receber"
    date_from: str,
    date_to: str | None = None,
    force_full: bool = False,
    max_pages: int | None = None,
) -> dict:
    """
    Sincronizar contas de forma incremental.
    
    Args:
        client: BlingClient instance
        company: Empresa (CZ ou CR)
        module: "contas_pagar" ou "contas_receber"
        date_from: Data inicial (YYYY-MM-DD)
        date_to: Data final (opcional, default hoje)
        max_pages: Limite de páginas (opcional)
    """
    from sync_erp import _sync_paginated_snapshot, _enrich_conta_pagar_with_detail, _enrich_conta_receber_with_detail  # noqa: E402
    
    if module == "contas_pagar":
        endpoint = "/contas/pagar"
        cache = _cache_path("contas_pagar_cache.jsonl", company)
        enrich_fn = _enrich_conta_pagar_with_detail
    else:
        endpoint = "/contas/receber"
        cache = _cache_path("contas_receber_cache.jsonl", company)
        enrich_fn = _enrich_conta_receber_with_detail
    
    final_date_to = date_to or datetime.now().strftime("%Y-%m-%d")
    ranges = split_date_ranges(date_from, final_date_to)
    if not ranges:
        raise ValueError(f"Invalid date range: {date_from}..{final_date_to}")

    print(f"[INFO] Sync incremental: {endpoint} de {date_from} até {final_date_to}")

    total_pages = 0
    total_fetched = 0
    total_new = 0
    total_enriched_ok = 0
    total_enriched_err = 0
    total_elapsed = 0.0

    for chunk_start, chunk_end in ranges:
        print(f"[INFO] Janela AP/AR: {chunk_start} até {chunk_end}")
        params = {
            "situacao": [1],
            "dataVencimentoInicial": chunk_start,
            "dataVencimentoFinal": chunk_end,
        }
        result = _sync_paginated_snapshot(
            client,
            endpoint,
            cache,
            company=company,
            params=params,
            enrich_row=enrich_fn,
            row_filter=lambda row, start=chunk_start, end=chunk_end: _row_matches_window(row, start, end),
            max_pages=max_pages,
            sleep_s=0.4,
        )
        total_pages += int(result["pages"])
        total_fetched += int(result["fetched"])
        total_new += int(result["new_records"])
        total_enriched_ok += int(result["enriched_ok"])
        total_enriched_err += int(result["enriched_err"])
        total_elapsed += float(result["elapsed_s"])

    return {
        "module": module,
        "endpoint": endpoint,
        "date_from": date_from,
        "date_to": final_date_to,
        "pages": total_pages,
        "fetched": total_fetched,
        "new_records": total_new,
        "enriched_ok": total_enriched_ok,
        "enriched_err": total_enriched_err,
        "elapsed_s": total_elapsed,
    }


def load_bling_credentials(secrets_file: str | None = None) -> dict:
    """Carregar credenciais do Bling do arquivo de secrets."""
    if secrets_file is None:
        secrets_file = os.getenv("BLING_SECRETS_FILE")
        if not secrets_file:
            # Caminho padrão
            secrets_file = str(Path.home() / "Documents" / "bling id.txt")
    
    credentials = {}
    secrets_path = Path(secrets_file)
    
    if not secrets_path.exists():
        raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {secrets_path}")
    
    with open(secrets_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=", 1)
            credentials[key.strip()] = value.strip()
    
    return credentials


def main() -> int:
    from client import BlingClient  # noqa: E402
    
    parser = argparse.ArgumentParser(
        description="Sync incremental de Contas a Pagar/Receber a partir da última sync."
    )
    parser.add_argument(
        "--company",
        required=True,
        choices=["CZ", "CR", "cz", "cr"],
        help="Empresa (CZ ou CR)",
    )
    parser.add_argument(
        "--module",
        required=True,
        choices=["contas_pagar", "contas_receber", "ambos"],
        help="Módulo para sincronizar",
    )
    parser.add_argument(
        "--date-from",
        dest="date_from",
        default=None,
        help="Data inicial (YYYY-MM-DD). Se omitido, usa a data da última sync.",
    )
    parser.add_argument(
        "--date-to",
        dest="date_to",
        default=None,
        help="Data final (YYYY-MM-DD). Default: hoje.",
    )
    parser.add_argument(
        "--max-pages",
        dest="max_pages",
        type=int,
        default=None,
        help="Limite de páginas para teste.",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Apenas planejar, não executar.",
    )
    parser.add_argument(
        "--force-full",
        dest="force_full",
        action="store_true",
        help="Forçar sync completo (ignora última sync).",
    )
    parser.add_argument(
        "--secrets-file",
        dest="secrets_file",
        default=None,
        help="Caminho para o arquivo de credenciais do Bling.",
    )
    
    args = parser.parse_args()
    
    company = args.company.upper()
    
    # Determinar data inicial
    if args.force_full:
        date_from = "2024-01-01"
        print(f"[INFO] Forçando sync completo a partir de {date_from}")
    elif args.date_from:
        date_from = args.date_from
        print(f"[INFO] Usando date-from explicito: {date_from}")
    else:
        date_from = get_last_sync_date(company)
        print(f"[INFO] Data da ultima sync: {date_from}")
    
    if args.date_to:
        date_to = args.date_to
    elif args.force_full:
        date_to = FORCE_FULL_HORIZON
    else:
        date_to = datetime.now().strftime("%Y-%m-%d")
    
    print(f"[INFO] Empresa: {company}")
    print(f"[INFO] Período: {date_from} até {date_to}")
    
    if args.dry_run:
        print("[DRY RUN] Nenhuma operação será executada.")
        return 0
    
    # Carregar credenciais e inicializar cliente Bling
    try:
        creds = load_bling_credentials(args.secrets_file)
        os.environ["BLING_SECRETS_FILE"] = args.secrets_file or str(Path.home() / "Documents" / "bling id.txt")
        
        client = BlingClient(company)
    except Exception as e:
        print(f"[ERRO] Falha ao inicializar BlingClient: {e}")
        return 1
    
    results = []
    
    modules = ["contas_pagar", "contas_receber"] if args.module == "ambos" else [args.module]
    
    for module in modules:
        print(f"\n=== Sync: {module} ===")
        try:
            result = sync_contas_incremental(
                client=client,
                company=company,
                module=module,
                date_from=date_from,
                date_to=date_to,
                force_full=args.force_full,
                max_pages=args.max_pages,
            )
            results.append(result)
            print(f"[OK] {module}: {result['fetched']} registros, {result['new_records']} novos")
        except Exception as e:
            print(f"[ERRO] {module}: {e}")
            return 1
    
    # Resumo
    print("\n=== Resumo ===")
    for r in results:
        print(f"  {r['module']}: {r['fetched']} fetched, {r['new_records']} novos, {r['pages']} páginas")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
