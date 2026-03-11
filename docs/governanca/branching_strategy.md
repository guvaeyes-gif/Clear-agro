# Branching Strategy

## Branches
- `main`: estado estavel para release.
- `dev`: integracao continua de mudancas aprovadas.
- `feature/*`: nova funcionalidade.
- `fix/*`: correcao de bug.
- `agent/*`: evolucao especifica de agentes/automacoes.

## Regras
- Nao commitar direto em `main`.
- `main` recebe merge apenas de `dev` validado.
- `feature/*`, `fix/*` e `agent/*` devem abrir PR para `dev`.
- PR precisa conter risco, impacto e rollback.

## Naming examples
- `feature/dashboard-financeiro-v1`
- `fix/bling-cr-taxid-conflict`
- `agent/orchestrator-lock-strategy`

