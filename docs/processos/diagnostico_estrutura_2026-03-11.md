# Diagnostico da Estrutura Atual (2026-03-11)

## Pastas existentes
- Estrutura legacy numerada (`00_...` ate `14_...`) ja em uso operacional.
- Workspace `supabase/` ativo com migrations e config do projeto remoto.
- Scripts de integracao e automacao em `11_agentes_automacoes/12_integracoes_agent/pipeline`.

## Scripts ativos identificados
- `run_bling_supabase_daily_cz.cmd`
- `run_bling_supabase_daily_cr.cmd`
- `06_generate_bling_supabase_import.py`
- `07_run_bling_supabase_daily.ps1`
- `08_register_bling_supabase_daily_task.ps1`
- `09_reconcile_bling_supabase.py`

## Integracoes e configs
- Supabase remoto operacional (`supabase/config.toml` + migrations aplicadas).
- Integracao Bling ativa para `CZ` e `CR`.
- Reconciliacao Bling x Supabase ativa com status/qa.
- Config principal de ingestao: `bling_ingest_hub_v1.json`.

## O que ja esta bom
- Pipeline diario funcional (sync, ingest, push, reconciliacao).
- Rastreabilidade por logs e status.
- Migrations versionadas e historico consistente.
- Gate de qualidade para dados financeiros ja implantado.

## O que precisa reorganizar
- Consolidar estrutura para padrao corporativo (`docs/database/integrations/...`).
- Reduzir dispersao de artefatos entre pastas legacy.
- Formalizar governanca de mudanca e estrategia de branches.
- Definir contratos de modulo para futura operacao multiagentes.

## Riscos de reorganizacao
- Quebra de scheduler por mudanca de caminho de script.
- Divergencia entre scripts duplicados em legado e estrutura nova.
- Commit acidental de secrets/logs sem `.gitignore` corporativo.
- Renomeacoes agressivas em arquivos de producao sem rollback.

## Diretriz aplicada
- Preservar caminhos produtivos atuais.
- Reorganizar por copia segura para estrutura nova.
- Registrar transicao em documento especifico.

