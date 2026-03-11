# Contributing - Clear OS

## Fluxo de contribuicao
1. Criar branch a partir de `dev`.
2. Implementar mudanca com foco em seguranca operacional.
3. Atualizar docs e runbook quando houver impacto.
4. Validar testes e checks de qualidade.
5. Abrir PR para `dev` com resumo tecnico e risco.

## Regras obrigatorias
- Nao sobrescrever scripts de producao sem backup ou plano de rollback.
- Nao commitar secrets, tokens, arquivos de credenciais ou `.env` real.
- Nao remover estrutura legacy sem aprovacao explicita.
- Toda mudanca de schema deve ser via migration versionada.
- Toda automacao precisa log e evidencias de execucao.

## Padrao de commit
- `feat: ...` nova capacidade
- `fix: ...` correcao
- `chore: ...` manutencao
- `docs: ...` documentacao
- `refactor: ...` refatoracao sem mudanca funcional

## Pull request checklist
- [ ] Objetivo e contexto claros
- [ ] Impactos em producao avaliados
- [ ] Migration (se aplicavel) testada
- [ ] Runbook atualizado (se aplicavel)
- [ ] Risco e plano de rollback descritos

