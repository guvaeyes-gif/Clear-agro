# Regras de Mudanca

## Principios
- Preservar operacao em producao.
- Alterar com rastreabilidade.
- Documentar decisao e impacto.

## Fluxo minimo
1. Inventariar estado atual.
2. Definir risco e plano de rollback.
3. Implementar em branch dedicada.
4. Validar testes/checks.
5. Registrar no changelog/roadmap.

## Mudancas criticas
Mudancas em integracoes, migrations, scheduler ou seguranca exigem:
- avaliacao de impacto operacional
- janela controlada
- evidencias de validacao
- rollback executavel

## O que nao pode
- editar script critico sem backup
- apagar artefato sem registro
- executar migration manual fora de trilha versionada
- publicar dashboard com gate de qualidade em `FAIL`

