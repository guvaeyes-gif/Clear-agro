Bling API v3 setup (Python)

1) Install dependencies:
   pip install requests

2) Generate authorization URL:
   python authorize_url.py

3) Open URL, login, authorize. Copy the 'code' from redirect URL.

4) Exchange code for tokens:
   # CZ (default)
   python token_exchange.py --code YOUR_CODE --company cz
   # CR
   python token_exchange.py --code YOUR_CODE --company cr

5) Refresh tokens (when needed):
   # CZ (default)
   python refresh_token.py --company cz
   # CR
   python refresh_token.py --company cr

6) Test API:
   python fetch_example.py

Notes:
- Env vars required: BLING_CLIENT_ID, BLING_CLIENT_SECRET, BLING_REDIRECT_URI
- Optional: BLING_SCOPES (space-separated)
- Tokens saved to bling_tokens.json

7) Sync unificado ERP (vendas, nfe, contatos, produtos, contas e estoque):
   # CZ (default)
   python sync_erp.py --year 2026 --company cz
   # CR
   python sync_erp.py --year 2026 --company cr

8) Teste rapido (1 pagina por modulo):
   python sync_erp.py --year 2026 --company cz --max-pages 1

9) Modulos especificos:
   python sync_erp.py --year 2026 --company cz --modules vendas,nfe

10) Incluir financeiro e estoque em modulos especificos:
    python sync_erp.py --year 2026 --company cz --modules vendas,nfe,contas_receber,contas_pagar,estoque

10.1) Sincronizar composicao/montagem de produtos (formula/BOM):
    # CZ
    python sync_erp.py --year 2026 --company cz --modules produtos_composicao
    # CR
    python sync_erp.py --year 2026 --company cr --modules produtos_composicao
    # saida: produtos_composicao_cache.jsonl (ou _cr)

11) Vendas com vendedor (enriquecimento por detalhe do pedido):
    # ja ocorre no modulo vendas (novos registros)
    python sync_erp.py --year 2026 --company cz --modules vendas

12) Backfill de vendedor no cache historico de vendas:
    python sync_erp.py --year 2026 --company cz --modules vendas --backfill-vendas-vendedor
    # opcional: limitar quantidade por execucao
    python sync_erp.py --year 2026 --company cz --modules vendas --backfill-vendas-vendedor --backfill-limit 300

13) Agendamento diario no Windows (06:00):
    powershell -ExecutionPolicy Bypass -File ..\scripts\register_bling_daily_task.ps1 -Company "CZ" -StartTime "06:00"

Credenciais por empresa (arquivo bling id.txt):
- BLING_CLIENT_ID_CZ=...
- BLING_CLIENT_SECRET_CZ=...
- BLING_REDIRECT_URI_CZ=...
- BLING_CLIENT_ID_CR=...
- BLING_CLIENT_SECRET_CR=...
- BLING_REDIRECT_URI_CR=...
