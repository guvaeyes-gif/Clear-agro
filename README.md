# McKinsey Agro CRM

## Rodar local

```powershell
pip install -r requirements.txt
python scripts\validate_kpis.py
python -m streamlit run app\main.py
```

## Startup estavel (Windows)

Use este fluxo para evitar travamentos de abertura do CRM:

```powershell
cd C:\Users\cesar.zarovski\CRM_Clear_Agro
.\start_crm_stable.cmd
```

Diagnostico rapido de localhost (quando "conexao recusada"):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\diagnose_localhost.ps1
```

## URL publica temporaria (review sem login)

Para revisar rapido com o time sem depender de localhost:

1. Suba este repo para o GitHub.
2. No Render, clique em `New +` > `Blueprint`.
3. Selecione o repo `clear-agro`.
4. O Render vai ler [render.yaml](C:\Users\cesar.zarovski\CRM_Clear_Agro\render.yaml) e publicar automaticamente.
5. Abra a URL gerada (ex.: `https://crm-clear-agro-review.onrender.com`).

Observacao:
- Este modo e publico e sem senha (somente para review rapido).
- Depois da revisao, habilite login/senha antes do uso operacional.

## Control Tower Financeira (CLARA CFO)

Estrutura criada para fluxo `raw -> staging -> marts -> exports`:

- `data/raw/{dre,bling,banks}`
- `data/staging`
- `data/marts`
- `data/quality`
- `data/exports`
- `config/{accounts,mapping_rules,overrides,entities}`
- `src/{ingest,transform,model,reconcile,reports,forecast,utils}`
- `docs/{OPERATING_MANUAL.md,DATA_DICTIONARY.md,CFO_PACK_SPEC.md}`

### Comandos (dev/test/lint/build)

```powershell
python -m streamlit run app\main.py     # dashboard (dev)
python -m ruff check .                  # lint
python -m pytest                        # test
python scripts\validate_kpis.py         # build atual do app
```

### Comandos da Control Tower

```powershell
python src\ingest\ingest_dre.py
python src\ingest\ingest_bling.py
python src\ingest\ingest_banks.py
python src\reports\build_cfo_pack.py
python src\reports\build_finance_pack.py
```

Saidas principais:
- `data/staging/stg_dre.csv`
- `data/staging/stg_bling.csv`
- `data/staging/stg_banks.csv`
- `data/marts/fact_dre.csv`
- `data/marts/fact_cashflow.csv`
- `data/marts/fact_reconciliation.csv`
- `data/exports/cfo_pack.md`
- `data/exports/finance_pack.md`

## Integracao Telegram

1. Crie um bot no Telegram com o `@BotFather` e copie o token.
2. Obtenha seu `chat_id` (ex.: com `@userinfobot` ou via getUpdates).
3. Configure variaveis de ambiente:

```powershell
$env:TELEGRAM_BOT_TOKEN="SEU_TOKEN"
$env:TELEGRAM_CHAT_ID="SEU_CHAT_ID"
```

4. Envie alertas:

- Pelo app: pagina `Insights & Alertas` > `Enviar alertas para Telegram`.
- Pelo terminal/agendador:

```powershell
python scripts\send_telegram_alerts.py
```

## Conversar com a Clara no Telegram (2 vias)

1. Configure tambem sua chave da OpenAI:

```powershell
$env:OPENAI_API_KEY="SUA_OPENAI_API_KEY"
# opcional:
$env:OPENAI_MODEL="gpt-4.1-mini"
```

2. Inicie o bot em polling:

```powershell
python scripts\telegram_clara_bot.py
```

3. No Telegram, abra seu bot e envie `/start`.
4. Envie mensagens normalmente; a Clara responde no mesmo chat.

Observacao:
- O modo atual esta sem whitelist/autorizacao por usuario.
- Se `OPENAI_API_KEY` nao estiver configurada, o bot responde com aviso de configuracao.

## Integracao Google (Gmail, Calendar, Drive, Sheets)

1. No Google Cloud Console:
- crie/seleciona um projeto.
- habilite APIs: Gmail API, Google Calendar API, Google Drive API, Google Sheets API.
- em `APIs e servicos > Tela de consentimento OAuth`, configure o app (External) e adicione seu usuario de teste.
- em `Credenciais`, crie `OAuth client ID` do tipo `Desktop app`.

2. Baixe o JSON da credencial e salve em:

```text
data/google/client_secret.json
```

3. Instale dependencias:

```powershell
pip install -r requirements.txt
```

4. Rode a autorizacao (abre navegador para login/consentimento):

```powershell
python scripts\google_workspace_auth.py
```

5. Valide com smoke test:

```powershell
python scripts\google_workspace_smoke.py
```

6. (Opcional) testar leitura de planilha especifica:

```powershell
$env:GOOGLE_SHEETS_SMOKE_SPREADSHEET_ID="SEU_SPREADSHEET_ID"
$env:GOOGLE_SHEETS_SMOKE_RANGE="A1:C10"
python scripts\google_workspace_smoke.py
```

Arquivos de segredo/token:
- `data/google/client_secret.json`
- `data/google/token.json`

### Operar aba especifica do Sheets

Ler:

```powershell
python scripts\google_sheets_ops.py --spreadsheet-id "SEU_ID" --sheet "Aba1" --range "A1:C10" --mode read
```

Gravar:

```powershell
python scripts\google_sheets_ops.py --spreadsheet-id "SEU_ID" --sheet "Aba1" --range "A1:B2" --mode write --set "[[\"Status\",\"OK\"],[\"Data\",\"2026-02-21\"]]" --confirm-write
```

### Atualizacao automatica de KPI diario

```powershell
python scripts\atualizar_status_relatorio.py --kpi "Pipeline ponderado" --valor "R$ 120.000" --status "Em linha" --confirm-write
```
