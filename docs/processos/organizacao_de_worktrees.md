# Organizacao de Worktrees

## Objetivo
Separar as frentes `comercial`, `financeiro` e `governanca` em worktrees dedicados, reduzindo conflito entre agentes e preservando a trilha canonica do Clear OS.

## Estrutura ativa
| Papel | Caminho | Branch |
|---|---|---|
| Base de validacao e integracao final | `C:\Users\cesar.zarovski\Documents\Clear_OS` | `main` |
| Comercial | `C:\Users\cesar.zarovski\Documents\Clear_OS_comercial` | `codex/comercial` |
| Financeiro | `C:\Users\cesar.zarovski\Documents\Clear_OS_financeiro` | `codex/financeiro` |
| Governanca | `C:\Users\cesar.zarovski\Documents\Clear_OS_governanca` | `codex/governanca` |

## Papel de cada worktree
### Base
- leitura operacional
- validacao final
- consolidacao de merge
- verificacao de impacto cruzado

### Comercial
- CRM
- metas
- oportunidades
- atividades comerciais
- views e app do dominio comercial

### Financeiro
- pipeline Bling -> Supabase
- reconciliacao
- dashboard financeiro
- publisher, healthcheck e observabilidade financeira

### Governanca
- docs
- runbooks
- logs
- checks
- guardrails
- ownership e padroes operacionais

## Regras de uso
- `supabase/migrations`, `integrations/bling/runners`, `automation/scheduler` e `security` nao devem ser editados em paralelo em worktrees diferentes.
- O worktree `main` nao deve virar area de desenvolvimento corrente; ele e a referencia de integracao final.
- Mudancas de `comercial` e `financeiro` devem chegar ao `main` so depois de validacao no proprio worktree.
- Mudancas de governanca devem refletir o estado real dos worktrees e da operacao.

## Regra pratica por frente
| Frente | Pode editar livremente | Precisa coordenar antes |
|---|---|---|
| Comercial | `app/`, `src/`, `integrations/crm`, seeds e views CRM | migrations, scheduler, runners Bling |
| Financeiro | `integrations/bling`, `dashboards/metabase`, publisher e status financeiros | migrations, scheduler, contratos compartilhados |
| Governanca | `docs/`, `config/`, runbooks, checks e monitoramento | qualquer mudanca que altere runtime produtivo |

## Fluxo recomendado
1. Trabalhar no worktree do dominio.
2. Validar localmente no proprio worktree.
3. Atualizar runbook ou governanca se houver impacto operacional.
4. Integrar no worktree base.
5. So depois abrir nova frente paralela no mesmo modulo sensivel.

## Observacao
Essa estrutura foi criada para aprofundar `comercial` e `financeiro` antes de expandir agentes. O objetivo nao e multiplicar frentes, e sim isolar risco.
