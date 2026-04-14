# Runbook de Metas Comerciais via Google Sheets

## Objetivo
Manter uma planilha compartilhada como fonte operacional de metas comerciais por vendedor, mes e trimestre, com sincronizacao para o Supabase e consumo pelo dashboard.

## Arquivos prontos
- `templates/metas_comerciais_modelo_google_sheets.xlsx`
- `templates/metas_comerciais_modelo_google_sheets.csv`
- `scripts/build_sales_targets_google_sheet_model.py`
- `scripts/import_sales_targets.py`
- `run_sales_targets_sync.cmd`

## Estrutura da planilha
Use a aba `metas` com estas colunas:
- `ano`
- `periodo_tipo`
- `mes`
- `quarter`
- `estado`
- `vendedor_id`
- `empresa`
- `canal`
- `cultura`
- `meta_valor`
- `meta_volume`
- `realizado_valor`
- `realizado_volume`
- `status`
- `observacoes`

## Regra operacional
- Preencha uma linha por meta mensal por vendedor.
- Deixe `quarter` em branco quando quiser que o importador derive o trimestre automaticamente.
- Se houver meta trimestral manual, use `periodo_tipo = QUARTER`.

## Configuracao minima
Defina no ambiente:
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SHEETS_TARGETS_SHEET=metas`
- `GOOGLE_SHEETS_TARGETS_RANGE=A:O`
- `GOOGLE_SERVICE_ACCOUNT_JSON` ou `GOOGLE_SERVICE_ACCOUNT_JSON_B64`
- opcionalmente `GOOGLE_SERVICE_ACCOUNT_FILE` quando rodar localmente com arquivo

## No Render
- use `GOOGLE_SERVICE_ACCOUNT_JSON` ou `GOOGLE_SERVICE_ACCOUNT_JSON_B64`
- compartilhe a planilha com o e-mail da service account
- depois de salvar as variaveis no Render, faça `Redeploy`

## Fluxo no dashboard
1. Abrir `Metas Comerciais`.
2. Clicar em `Validar planilha compartilhada`.
3. Revisar a saida do comando.
4. Clicar em `Sincronizar agora`.

## Fluxo via linha de comando
- Rodar `run_sales_targets_sync.cmd` ou:
  - `python scripts/import_sales_targets.py --source google-sheet --default-company CZ`

## Modelo pronto
O arquivo `templates/metas_comerciais_modelo_google_sheets.xlsx` ja vem com:
- `metas`
- `metas_trimestrais`
- `metas_atual`
- `referencia_vendas`
- `vendedores`
- `instrucoes`

## Observacao
O dashboard consome as metas sincronizadas via Supabase. A planilha compartilhada e apenas a fonte operacional de entrada.
