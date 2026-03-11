# Changelog

## 2026-03-11
- Criada estrutura padrao corporativa (`docs`, `database`, `integrations`, `agents`, `dashboards`, `automation`, `logs`, `config`, `security`, `tests`, `archive`).
- Inicializado Git no workspace `Clear_OS`.
- Adicionado `.gitignore` corporativo com foco em secrets, logs e artefatos locais.
- Publicada documentacao base de governanca, branching e contribuicao.
- Publicada arquitetura multiagentes e READMEs por agente.
- Reorganizados artefatos por copia segura para novo padrao (sem quebrar legado):
  - roadmap em `docs/roadmap`
  - runbooks em `docs/runbooks`
  - migrations em `database/migrations`
  - scripts Bling em `integrations/bling/*`
  - jobs/scheduler em `automation/*`

