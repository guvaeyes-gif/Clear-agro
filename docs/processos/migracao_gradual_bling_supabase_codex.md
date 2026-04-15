# Migracao Gradual - Bling -> Supabase -> Codex

## Objetivo
Evoluir o stack atual para um fluxo canonico `Bling -> Supabase -> Codex` sem interromper a operacao atual nem fazer cutover imediato.

## Principio de transicao
- manter o fluxo produtivo atual operando
- construir o fluxo novo em paralelo
- validar paridade funcional e numerica
- trocar o consumo apenas quando a leitura nova estiver comprovada

## Regra de arquitetura alvo
- `Bling`: sistema de origem
- `Supabase`: banco canonico unico
- `Codex`: app, agentes, dashboards e automacoes lendo do Supabase

## Regra operacional durante a migracao
- nenhuma remocao de `jsonl`, `xlsx` ou `metas.db` antes da validacao
- nenhuma troca direta em telas criticas sem flag
- toda leitura nova deve poder ser desligada por configuracao
- todo cutover deve ser reversivel

## Flags iniciais
- `CRM_READ_SOURCE=auto`
  - comportamento padrao
  - usa Supabase quando backend estiver configurado
- `CRM_READ_SOURCE=legacy`
  - desliga a leitura CRM via Supabase
  - preserva o comportamento atual do restante do app
- `CRM_READ_SOURCE=supabase`
  - forca a leitura CRM via Supabase
- `USE_SUPABASE_CRM_READ=1`
  - alias simples para forcar leitura CRM via Supabase

## Fases recomendadas
1. Consolidar o modelo canonico no Supabase.
2. Migrar leitura CRM para views Supabase com flags.
3. Migrar escrita operacional de CRM para Supabase.
4. Migrar financeiro de caches locais para raw/core no Supabase.
5. Comparar KPIs, totais e filas entre legado e Supabase.
6. Fazer cutover controlado e manter fallback por janela curta.

## Escopo imediato no repositorio
- CRM:
  - `src/metas_db.py`
  - `src/data.py`
  - `app/main.py`
- Financeiro:
  - `integrations/bling/*`
  - `src/reports/build_finance_pack.py`
  - leituras locais em `app/main.py`

## Criterios minimos para cutover
- views CRM respondendo sem erro no Supabase
- paridade de metas, pipeline e fila prioritaria
- reconciliacao financeira com `PASS`
- dashboards consumindo apenas Supabase nas areas migradas
- rollback documentado e testado
