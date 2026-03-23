# Regras de Mudanca

## Objetivo
Controlar mudancas em modulos criticos com rastreabilidade, rollback claro e preservacao do que ja esta operando.

## Principios obrigatorios
- Preservar operacao em producao.
- Explicitar impacto, risco e rollback antes de alterar modulo critico.
- Alterar de forma incremental e auditavel.
- Preferir compatibilidade por copia, wrapper ou documentacao antes de renomeacao destrutiva.

## Fluxo minimo
1. Inventariar o estado atual e identificar a fonte canonica do artefato.
2. Classificar o risco da mudanca: baixo, medio ou alto.
3. Definir validacao minima e rollback executavel.
4. Implementar em branch ou worktree dedicada quando a mudanca tocar area sensivel.
5. Validar com checks, leitura dos logs gerados e evidencias.
6. Atualizar documentacao de arquitetura, governanca ou runbook quando houver impacto operacional.
7. Registrar a mudanca em inventario, changelog ou trilha de auditoria.

## Mudancas que exigem aprovacao explicita
- `database/migrations`
- `automation/scheduler`
- `integrations/bling/runners`
- `security`
- Credenciais, cofres, tokens e arquivos de acesso
- Qualquer alteracao em jobs diarios `CZ` e `CR`

## Mudancas proibidas sem analise previa
- Apagar scripts de producao.
- Reapontar scheduler diretamente para caminho novo sem janela controlada.
- Alterar integracao Bling funcional por suposicao.
- Mudar logs ou status de modo que quebre rastreabilidade ja usada por operacao.
- Sobrescrever artefato de outro agente sem ownership claro.

## Politica para legado e duplicidade
- Se um artefato legado ainda participa do fluxo real, ele deve ser mantido ate a transicao ser comprovadamente segura.
- Se houver duplicidade entre caminho novo e antigo, documentar:
  - qual e a fonte canonica
  - qual e compatibilidade temporaria
  - qual e o risco de divergencia
- Renomeacao so deve ocorrer depois que wrappers, docs e scheduler estiverem convergentes.

## Evidencias minimas por tipo de mudanca
| Tipo | Evidencia minima |
|---|---|
| Documentacao | diff claro e referencias a caminhos reais |
| Integracao | log de execucao, status final e impacto esperado |
| Scheduler | comando de registro, nome da task e rollback |
| Banco | migration versionada, leitura de impacto e rollback |
| Dashboard | healthcheck, runbook e gate de qualidade |

## Rollback minimo esperado
- Documentacao: restaurar arquivo anterior.
- Wrappers e jobs: voltar para wrapper anterior conhecido.
- Scheduler: re-registrar task com alvo anterior.
- Banco: usar migration corretiva versionada; nunca editar migration aplicada.

## Registro final obrigatorio
Toda alteracao com impacto operacional deve registrar:
- o que foi encontrado
- o que mudou
- por que mudou
- riscos
- proximos passos
