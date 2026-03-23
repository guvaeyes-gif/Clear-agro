# Bank Schema Matrix (MVP)

Colunas mínimas esperadas por extrato:
- `data`
- `descricao` (ou `historico`)
- `valor` (ou `credito` + `debito`)
- `documento` (opcional, recomendado)

## Mapeamentos aceitos no parser
- Data: `data`, `data lançamento`, `data lancamento`, `date`
- Descrição: `historico`, `histórico`, `descricao`, `descrição`, `memo`
- Valor único: `valor`, `valor (r$)`, `amount`
- Crédito/Débito: `credito`/`debito` (`credit`/`debit`)

## Recomendação operacional por banco
- Santander: exportar CSV com `data`, `historico`, `valor`.
- Itaú: manter formato com valor único quando possível.
- Bradesco/Sicredi: se vier crédito/débito separado, não editar manualmente.

## Boas práticas
- Não alterar separador decimal no arquivo original.
- Salvar CSV em UTF-8.
- Não remover cabeçalho.
