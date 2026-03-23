# Padroes de Nomenclatura

## Regras gerais
- Usar minusculas.
- Evitar espacos.
- Preferir `snake_case`.
- Nomes devem refletir papel operacional e nao contexto temporario.

## Documentacao
- Arquitetura: `<tema>.md`
- Governanca: `<regra_ou_padrao>.md`
- Runbook: `<acao_ou_troubleshooting>.md`
- Inventario datado: `<tema>_<yyyy-mm-dd>.md`

## Scripts e runners
- Script utilitario: `<acao>_<alvo>_<contexto>.py|.ps1`
- Runner de pipeline: `run_<pipeline>.ps1`
- Registro de scheduler: `register_<pipeline>_task.ps1`
- Wrapper de job: `run_<pipeline>_<empresa>.cmd`

## Jobs e scheduler
- Nome interno do job: `run_<dominio>_<pipeline>_<frequencia>`
- Nome da task do Windows Scheduler: `ClearOS-<Fluxo>-<Frequencia>-<Escopo>`
- Exemplo atual valido:
  - `ClearOS-Bling-Supabase-Daily-CZ`
  - `ClearOS-Bling-Supabase-Daily-CR`

## Logs e status
- Log tecnico: `<job_name>_<execution_id>.log`
- Status final: `<job_name>_<execution_id>_status.json`
- QA: `<job_name>_<execution_id>_qa.csv`
- Arquivo de auditoria: `<change_type>_<execution_id>.json`

## Configuracoes
- Config principal por integracao: `<integracao>_<pipeline>_<versao>.json`
- Config por empresa: `<integracao>_<pipeline>_<versao>_<empresa>.json`
- Template: `<tipo>_template.<ext>`

## Banco e SQL
- Migration: `<yyyymmddhhmmss>_<descricao>.sql`
- View: `vw_<dominio>_<tema>`
- Query de dashboard: `<ordem>_<tema>.sql`

## Sufixos e escopos
- Empresa: `cz`, `cr`, `all`
- Frequencia: `daily`, `hourly`, `manual`
- Ambiente: `local`, `dev`, `prod`
- Status de artefato: usar no conteudo do JSON, nao no nome do arquivo

## Regras de compatibilidade
- Nomes legados com numeracao sequencial so devem ser mantidos quando fizerem parte do fluxo real.
- Novos arquivos nao devem adotar prefixos numericos fora de migrations e SQL de dashboard.
