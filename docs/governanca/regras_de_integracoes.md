# Regras de Integracoes

## Objetivo
Definir como uma integracao deve ser estruturada, nomeada, observada e alterada dentro do Clear OS.

## Estrutura padrao
- `integrations/<sistema>/config`
- `integrations/<sistema>/load`
- `integrations/<sistema>/reconciliation`
- `integrations/<sistema>/runners`
- `integrations/shared`

## Regras obrigatorias
- Toda integracao deve declarar origem, destino, owner e criticidade.
- Toda integracao deve ter ao menos um ponto de status estruturado.
- Toda integracao critica deve ser idempotente na escrita.
- Toda integracao com scheduler deve ter runner canonico e wrapper de job separados.
- Toda dependencia de credencial deve ser documentada, mesmo quando o segredo ficar fora do repositorio.

## Regras de implementacao
- Runners orquestram; nao carregam regra de negocio complexa.
- Scripts de `load` transformam e escrevem; devem publicar status.
- Scripts de `reconciliation` validam origem versus destino e devem produzir QA.
- `config` deve separar configuracao global de configuracao por empresa.

## Regras de compatibilidade
- Se houver caminho legado, documentar o motivo e o plano de convergencia.
- Nao mover integracao produtiva sem wrapper de compatibilidade ou janela controlada.
- Nao alterar integracao de producao apenas para alinhar estrutura estetica.

## Regras de seguranca
- Credenciais nao devem ser commitadas.
- Tokens locais devem ser referenciados por caminho ou variavel de ambiente.
- Mudancas em integracao critica exigem avaliacao de impacto e rollback.

## Regras de observabilidade
- Publicar `execution_id` ou `run_id`.
- Publicar `job_name`, `module_name`, `source_system`, `target_system`, `company_code`, `status`.
- Publicar contagens de leitura, escrita e falha quando houver processamento de volume.
- Publicar QA CSV quando houver reconciliacao ou gate tecnico.

## Regra pratica para Bling
`integrations/bling` e a referencia operacional atual. Scripts em `automation/scripts` ou em caminho legacy nao devem ser promovidos a canonicos sem revisao.
