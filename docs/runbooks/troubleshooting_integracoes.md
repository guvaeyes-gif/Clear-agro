# Troubleshooting de Integracoes

## Quando usar
Quando um job de integracao falhar, ficar parcial ou nao publicar status.

## Passos
1. Identificar qual wrapper ou task foi executado.
2. Abrir o log do scheduler correspondente.
3. Localizar o `execution_id` ou `run_id`.
4. Ler o status JSON e o QA CSV da mesma rodada.
5. Confirmar se a falha ocorreu em sync, ingest, geracao de migration, push ou reconciliacao.
6. Verificar credenciais locais e paths de cache.
7. Se o erro envolver Supabase, confirmar token, project ref e acesso da CLI.
8. Se o erro envolver Bling, confirmar cache JSONL e arquivo de credencial.
9. Se necessario, rerodar manualmente a empresa afetada em janela controlada.

## Diagnosticos comuns
| Sintoma | Causa provavel | Acao |
|---|---|---|
| task executou mas nao gerou status | falha no runner antes de publicar artefato | ler log tecnico e confirmar dependencia ausente |
| reconciliacao falhou | divergencia entre cache e Supabase | inspecionar QA CSV e ultima migration aplicada |
| `db push` falhou | token, CLI ou migration invalida | validar token e revisar migration mais recente |
| dashboard healthcheck falhou | quality gate ou arquivo obrigatorio ausente | corrigir status de reconciliacao ou artefato faltante |

## Escalonamento
- Incidente de credencial: Governanca/Plataforma.
- Divergencia de dados financeiros: Financeiro.
- Falha em scheduler ou runner: Plataforma/Operacoes.
