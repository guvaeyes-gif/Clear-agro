# Render CRM Public Review

## Objetivo
Publicar o CRM em URL HTTPS estavel para revisao externa, sem usar tunel temporario.

## Escopo recomendado
- usar o servico definido em `render.yaml`
- manter `CRM_PUBLIC_REVIEW=1`
- expor apenas as paginas CRM que dependem do banco: `Pipeline Manager` e `Insights & Alertas`

## Variaveis obrigatorias no Render
- `CRM_DATABASE_URL`
  - opcao preferida; usar a connection string Postgres do Supabase com `sslmode=require`

## Variaveis opcionais
- `SUPABASE_DB_URL`
  - alternativa se nao quiser usar `CRM_DATABASE_URL`
- `SUPABASE_PROJECT_REF`
- `SUPABASE_ACCESS_TOKEN`
  - usar apenas se quiser fallback via REST
- `APP_BUILD`
  - identificador visivel na tela publica

## Passo a passo
1. Subir este repositorio para GitHub.
2. No Render, criar um novo `Blueprint` apontando para o repo.
3. Confirmar a leitura do arquivo `render.yaml`.
4. Preencher `CRM_DATABASE_URL` com a string do banco Supabase.
5. Disparar o primeiro deploy.
6. Abrir a URL gerada pelo Render e validar as paginas `Pipeline Manager` e `Insights & Alertas`.

## Validacao minima
- a home abre sem bloqueio de seguranca do navegador
- `Pipeline Manager` mostra oportunidades e fila prioritaria
- `Insights & Alertas` mostra itens da fila CRM
- se o banco falhar, a UI mostra warning explicito em vez de parecer vazia

## Observacoes
- o modo publico nao deve depender de arquivos locais em `out/` ou `bling_api/`
- `Metas Comerciais` fica fora do modo publico
- se quiser URL fixa corporativa, conectar dominio proprio ao servico Render apos o primeiro deploy
