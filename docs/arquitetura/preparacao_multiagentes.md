# Preparacao Multiagentes

## Objetivo
Preparar o Clear OS para crescimento com mais agentes sem colisao de ownership, concorrencia operacional ou perda de rastreabilidade.

## Modulos que podem ser trabalhados em paralelo
- `docs/arquitetura`, `docs/governanca`, `docs/processos`, `docs/runbooks` quando os arquivos forem distintos.
- `dashboards/specs` e consultas de BI sem alterar runtime operacional.
- Leitura diagnostica de `database`, `agents`, `dashboards` e `security`.

## Modulos que exigem worktree ou branch isolada
- `database/migrations`
- `integrations/bling/runners`
- `automation/scheduler`
- `security`
- contratos compartilhados em `integrations/shared`

## Modulos que exigem lock de execucao
- runner diario Bling
- `supabase db push`
- reconciliacao por empresa
- alteracao de scheduler

## Modulos que exigem aprovacao antes de mudar
- `security`
- `database/migrations`
- `automation/scheduler`
- jobs diarios `CZ` e `CR`
- qualquer alteracao que altere o destino da reconciliacao ou do quality gate

## Pastas compartilhadas e sensiveis
- `logs`
- `automation`
- `config`
- `database`
- `security`

## Areas de maior risco de conflito
- Convivencia entre trilha nova e artefatos legados.
- Scripts que compartilham o mesmo token, credencial ou migration dir.
- Alteracao simultanea de docs de ownership e scripts operacionais.
- Jobs concorrentes que escrevem em Supabase sem lock.

## Boas praticas de worktree
- Um worktree por modulo sensivel ou por iniciativa critica.
- Nao compartilhar worktree para duas frentes que alteram `database/migrations`.
- Sincronizar documentacao de ownership antes de merge em modulo compartilhado.

## Escopo recomendado por agente
| Agente | Escopo recomendado |
|---|---|
| CRM | `agents/crm`, `integrations/crm`, docs do dominio |
| Financeiro | `agents/financeiro`, views e artefatos financeiros acordados |
| Operacoes | `docs`, `logs`, `automation`, observabilidade, runbooks |
| Plataforma | `integrations/shared`, scheduler, contratos tecnicos |

## Ordem recomendada de atuacao
1. Documentar ownership e risco.
2. Definir fonte canonica do modulo.
3. Aplicar lock ou branch isolada se houver escrita critica.
4. Executar mudanca.
5. Publicar status, inventario e proximo passo.

## Necessidade estrutural
Logs, idempotencia e rastreabilidade nao sao opcionais em ambiente multiagentes. Todo modulo compartilhado deve ser tratado como contrato, nao como pasta livre para escrita.
