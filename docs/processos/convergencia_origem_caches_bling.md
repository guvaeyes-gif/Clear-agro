# Convergencia da Origem dos Caches Bling

## Objetivo
Reduzir o risco de divergencia entre app, relatorios e pipeline ao centralizar a resolucao dos caminhos dos caches Bling, sem mover arquivos nem alterar o scheduler produtivo nesta etapa.

## Problema encontrado
Hoje existem duas raizes relevantes de cache:
- `bling_api/` na raiz do workspace
- `11_agentes_automacoes/11_dev_codex_agent/repos/CRM_Clear_Agro/bling_api/`

Na pratica:
- o app CRM e parte dos relatorios leem a raiz `bling_api/`
- o pipeline financeiro produtivo usa majoritariamente o repo aninhado

Isso cria risco de:
- dashboard e pipeline enxergarem conjuntos diferentes de dados
- suporte operacional ler o arquivo errado
- agentes tomarem decisoes diferentes para a mesma entidade

## Decisao da etapa anterior
Foi criada uma camada de resolucao centralizada em:
- `integrations/shared/bling_paths.py`

Essa camada:
- resolve o root preferencial por contexto (`app` ou `pipeline`)
- preserva fallback para o caminho alternativo quando o arquivo nao existe na raiz preferida
- permite override por ambiente sem alterar codigo produtivo

## Overrides suportados
- `CLEAR_OS_BLING_ROOT`
- `CLEAR_OS_BLING_APP_ROOT`
- `CLEAR_OS_BLING_PIPELINE_ROOT`

## Politica de resolucao
### Contexto `app`
Preferencia:
1. `CLEAR_OS_BLING_APP_ROOT`
2. `CLEAR_OS_BLING_ROOT`
3. `bling_api/` na raiz
4. repo aninhado em `11_agentes_automacoes/.../CRM_Clear_Agro/bling_api/`

### Contexto `pipeline`
Preferencia:
1. `CLEAR_OS_BLING_PIPELINE_ROOT`
2. `CLEAR_OS_BLING_ROOT`
3. repo aninhado em `11_agentes_automacoes/.../CRM_Clear_Agro/bling_api/`
4. `bling_api/` na raiz

## Consumidores ajustados nesta etapa
- `src/data.py`
- `src/data_loader.py`
- `src/reports/build_finance_pack.py`
- `scripts/send_ar_weekly.py`
- defaults Python de:
  - `integrations/bling/load/generate_bling_supabase_import.py`
  - `integrations/bling/reconciliation/reconcile_bling_supabase.py`

## O que nao foi alterado nesta etapa
- `integrations/bling/runners/run_bling_supabase_daily.ps1`
- configs JSON de ingestao em `integrations/bling/config/`
- scheduler
- localizacao fisica dos caches

Esses itens continuam preservados para nao quebrar a operacao atual.

## Resultado pratico
- o app deixa de depender rigidamente de uma unica raiz fisica
- scripts Python passam a usar uma regra unica de resolucao
- o pipeline continua funcionando como hoje, com um caminho claro para convergencia futura

## Decisao da etapa atual
Foi homologada uma raiz fisica oficial para os caches Bling:
- `bling_api/` na raiz do workspace

Artefatos adicionados:
- `config/paths/bling_cache_roots.json`
- `scripts/sync_bling_cache_roots.py`

Regras desta etapa:
- a raiz oficial passa a ser `bling_api/`
- a raiz aninhada continua como compatibilidade e fonte de sincronizacao
- o codigo passa a priorizar a raiz homologada, com fallback automatico enquanto a convergencia nao estiver completa

## Cutover operacional aplicado
- `integrations/bling/runners/run_bling_supabase_daily.ps1` passa a sincronizar o ERP na raiz homologada `bling_api/`
- os configs:
  - `integrations/bling/config/bling_ingest_hub_v1.json`
  - `integrations/bling/config/bling_ingest_hub_v1_cz.json`
  - `integrations/bling/config/bling_ingest_hub_v1_cr.json`
  passam a ler os caches da raiz homologada
- apos o sync, o runner executa um espelho opcional da raiz homologada para a raiz legada para manter compatibilidade temporaria

## Fallback temporario mantido
- leitores Python ainda possuem fallback automatico para a raiz legada se algum arquivo faltar
- o espelho reverso do runner evita quebra imediata de consumidores antigos
- o scheduler e os wrappers nao precisaram ser alterados nesta etapa

## Proxima etapa recomendada
1. monitorar 2 a 5 execucoes do pipeline com a raiz homologada
2. identificar consumidores residuais da raiz legada
3. congelar a raiz antiga como somente leitura
4. remover o espelho reverso e o fallback apenas depois da validacao operacional
