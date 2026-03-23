# Monitoramento Pos-Cutover Bling

## Objetivo
Verificar, sem executar o pipeline produtivo, se a raiz canonica `bling_api/` permanece integra e se a compatibilidade com a raiz legada continua preservada.

## Check operacional
Executar:

```powershell
python scripts/check_bling_cutover_health.py --run-id monitoramento_manual
```

Saida:
- `logs/integration/status/check_bling_cutover_health_<RUN_ID>_status.json`

## O que o check valida
- existencia da raiz canonica e da raiz legada
- presenca dos arquivos monitorados nas duas raizes
- sincronizacao por tamanho e timestamp
- presenca dos ultimos status de:
  - ingestao
  - geracao de import
  - reconciliacao
  - sync para a raiz canonica
  - dry-run do espelho para a raiz legada

## Interpretacao
- `post_cutover_pipeline_seen = false`
  significa que ainda nao houve execucao real observada do pipeline apos o cutover
- `files_missing_in_canonical > 0`
  indica que a raiz homologada ainda nao esta completa
- `files_mismatched > 0`
  indica divergencia entre raiz canonica e raiz legada

## Regra operacional
- Nao congelar a raiz legada enquanto houver divergencia material nos arquivos monitorados.
- Nao remover fallback enquanto nao houver execucoes reais validadas apos o cutover.
