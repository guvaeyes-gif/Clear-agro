## Importacao De Metas

Arquivo padrao:

- `01_crm_comercial\metas_comerciais_template.csv`

Campos obrigatorios:

- `ano`
- `mes`
- `empresa`
- `estado`
- `vendedor_id`
- `meta_valor`

Campos opcionais:

- `vendedor`
- `status`
- `canal`
- `cultura`
- `meta_volume`
- `observacoes`

Regras:

- uma linha por meta mensal
- quarter e YTD sao calculados no dashboard
- use `empresa` = `CZ` ou `CR`
- `status` deve ser `ATIVO`, `PAUSADO`, `DESLIGADO` ou `TRANSFERIDO`

Operacao simples:

1. editar `metas_comerciais_template.csv`
2. executar `Importar metas comerciais.cmd`
3. abrir o dashboard

Teste sem gravar:

```powershell
python importar_metas_supabase.py --input .\metas_comerciais_template.csv --dry-run
```
