# Diagnostico Operacional do Clear OS (2026-03-11)

## 1. Estado atual
- O workspace `Clear_OS` ja possui a estrutura alvo de plataforma: `docs`, `integrations`, `automation`, `logs`, `config`, `security`, `database`, `dashboards`, `tests`.
- O pipeline Bling x Supabase esta operacional para `CZ` e `CR`.
- Existem wrappers em `automation/jobs` e tambem wrappers legados na raiz do repositorio.
- A integracao canonica mais atual esta em `integrations/bling/runners`.
- Parte da documentacao de governanca ja existia, mas ainda estava resumida e sem cobertura operacional completa.

## 2. O que foi encontrado nas pastas-alvo
### `integrations`
- `integrations/bling` contem configs por empresa, runner diario, registrador de scheduler, gerador de migration e reconciliacao.
- `integrations/crm` e `integrations/sheets` existem como placeholder documental.
- `integrations/shared` existe, mas ainda sem contrato operacional detalhado.

### `automation`
- `automation/jobs` contem wrappers para Bling diario e publicacao do dashboard financeiro.
- `automation/scheduler` contem registrador de task diario Bling.
- `automation/scripts` contem um espelho do runner de Bling com caminhos legados e menor convergencia com a estrutura nova.

### `logs`
- Estrutura principal existe: `logs/integration`, `logs/agents`, `logs/audit`.
- Em `logs/integration` existe hoje apenas `scheduler/` como subpasta criada fisicamente.
- Os demais subdiretorios sao criados sob demanda pelos scripts atuais.

### `config`
- Ja existem templates e arquivos de configuracao para ambiente, entidades, overrides e mapping rules.
- Nao havia templates padronizados para status, auditoria e log operacional.

### `security`
- A pasta existe e representa area sensivel de governanca.
- Nao foi feito ajuste funcional nessa area; ela foi tratada apenas como dependencia de ownership e aprovacao.

### `tests/integration`
- Existe apenas um `README.md`.
- Ainda nao ha bateria formal de testes de integracao documentando o pipeline Bling ou checagens de scheduler.

## 3. Dependencias operacionais criticas
- Credencial Bling em arquivo legado fora da trilha nova.
- Token de acesso do Supabase em arquivo local do usuario.
- `npx supabase db push --linked --include-all --yes` no runner diario.
- CLI do Supabase para recuperar API key na reconciliacao.
- Cache JSONL do `bling_api` no repositorio legado de automacoes.

## 4. Pontos fortes
- Runner canonico novo ja publica logs em `logs/integration`.
- Reconciliacao gera status JSON e QA CSV.
- Config de ingestao por empresa ja aponta para `logs/integration/status`.
- Scheduler e wrappers de `CZ` e `CR` ja foram espelhados para a estrutura nova.

## 5. Pontos frageis
- Convivencia de duas trilhas operacionais: nova em `integrations/bling` e legado em `11_agentes_automacoes/...`.
- `automation/scripts/run_bling_supabase_daily.ps1` ainda referencia arquivos antigos e uma config antiga.
- `scripts/finance_dashboard_publisher.py` ainda depende do status legado para o quality gate.
- Falta padrao unico formal para logs de agentes, auditoria e status final de jobs nao financeiros.
- Falta runbook operacional unico para jobs, troubleshooting e verificacao de logs.

## 6. Riscos principais
- Divergencia entre wrappers novos e antigos.
- Alteracao acidental do scheduler para um alvo nao homologado.
- Falha de rastreabilidade se cada job continuar escrevendo em local diferente.
- Concorrencia indevida entre agentes em `logs`, `automation`, `database/migrations` e `security`.

## 7. Diretriz aplicada nesta rodada
- Preservar caminhos produtivos existentes.
- Consolidar a camada documental e os templates operacionais.
- Tratar duplicidade e legado como risco documentado, nao como refactor imediato.
