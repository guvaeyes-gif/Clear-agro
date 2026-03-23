# Clear OS - Supabase Database Architecture (Phase 1)

## Bloco A - Visao geral da arquitetura

### Objetivo
Criar um nucleo transacional unico no Supabase para sustentar:
- Morgan Financeiro
- CRM Agent
- Compras Agent
- Dashboards iniciais
- Visao executiva do dono

### Estrategia por fases
- Phase 1A: tabelas centrais transacionais (financeiro, CRM minimo, compras minimo, usuarios).
- Phase 1B: tabelas operacionais complementares (contatos, atividades, aprovacoes detalhadas, cotacoes, pedidos).
- Phase 2: camada analitica, integracoes expandidas e historico/auditoria avancada.

### Convencoes tecnicas
- Tabelas e colunas em ingles e snake_case.
- PK em uuid com default gen_random_uuid().
- Campos base: id, created_at, updated_at.
- Campos opcionais padrao: created_by, status, notes, metadata.
- Tipos de data em timestamptz para rastreabilidade.
- Estrutura pronta para RLS no Supabase (auth.uid + app role).

### Camadas sugeridas no Postgres/Supabase
- public: tabelas transacionais do app.
- raw (phase 2): dados brutos de integracoes (Bling, Sheets, Notion, email, OpenClaw).
- mart (phase 2): visoes materializadas para dashboard.
- audit (phase 2): trilha de mudanca detalhada.

---

## Bloco B - Tabelas por modulo

## CRM / Comercial

### app_users (phase 1A)
- Objective: cadastro de usuarios internos, role e vinculo com auth.users.
- Main fields:
  - id uuid
  - auth_user_id uuid
  - full_name text
  - email text
  - role app_role
  - department text
  - is_active boolean
  - status record_status
  - notes text
  - metadata jsonb
  - created_by uuid
  - created_at timestamptz
  - updated_at timestamptz
- PK: id
- FKs: auth_user_id -> auth.users.id
- Required: full_name, email, role
- Status fields: status, is_active
- Timestamps: created_at, updated_at
- Notes: base de autorizacao para RLS em todas as areas.

### sales_reps (phase 1A)
- Objective: cadastro de vendedores vinculados a usuarios.
- Main fields: id, user_id, rep_code, region, commission_rate, status, notes, metadata, created_by, created_at, updated_at
- PK: id
- FKs: user_id -> app_users.id, created_by -> app_users.id
- Required: user_id
- Status fields: status
- Timestamps: created_at, updated_at
- Notes: 1 usuario pode ser 1 vendedor; use unique(user_id).

### customers (phase 1A)
- Objective: cadastro mestre de clientes para CRM e financeiro.
- Main fields: id, legal_name, trade_name, tax_id, segment, city, state, country, owner_sales_rep_id, risk_level, status, notes, metadata, created_by, created_at, updated_at
- PK: id
- FKs: owner_sales_rep_id -> sales_reps.id, created_by -> app_users.id
- Required: legal_name
- Status fields: status, risk_level
- Timestamps: created_at, updated_at
- Notes: tax_id unico quando informado; reutilizar para contas a receber.

### customer_contacts (phase 1B)
- Objective: contatos por cliente (comercial e cobranca).
- Main fields: id, customer_id, full_name, email, phone, role_title, is_primary, status, notes, metadata, created_by, created_at, updated_at
- PK: id
- FKs: customer_id -> customers.id, created_by -> app_users.id
- Required: customer_id, full_name
- Status fields: status, is_primary
- Timestamps: created_at, updated_at
- Notes: permite multiplos contatos por cliente.

### sales_opportunities (phase 1A)
- Objective: pipeline comercial e previsao de receita.
- Main fields: id, customer_id, owner_sales_rep_id, title, stage, expected_value, probability, expected_close_date, source, status, notes, metadata, created_by, created_at, updated_at
- PK: id
- FKs: customer_id -> customers.id, owner_sales_rep_id -> sales_reps.id, created_by -> app_users.id
- Required: customer_id, title, expected_value
- Status fields: stage, status
- Timestamps: created_at, updated_at
- Notes: base para dashboard de funil e alertas do CRM Agent.

### sales_activities (phase 1B)
- Objective: historico de atividades comerciais (ligacao, visita, follow-up).
- Main fields: id, opportunity_id, customer_id, owner_sales_rep_id, activity_type, due_at, completed_at, priority, status, notes, metadata, created_by, created_at, updated_at
- PK: id
- FKs: opportunity_id -> sales_opportunities.id, customer_id -> customers.id, owner_sales_rep_id -> sales_reps.id, created_by -> app_users.id
- Required: activity_type, due_at
- Status fields: priority, status
- Timestamps: created_at, updated_at
- Notes: permite alerta de carteira parada.

## Financeiro

### financial_categories (phase 1A)
- Objective: plano de categorias financeiras para DRE e fluxo de caixa.
- Main fields: id, code, name, category_group, is_cash_flow, status, notes, metadata, created_by, created_at, updated_at
- PK: id
- FKs: created_by -> app_users.id
- Required: code, name, category_group
- Status fields: status
- Timestamps: created_at, updated_at
- Notes: category_group padroniza receita/despesa/transferencia etc.

### accounts_payable (phase 1A)
- Objective: contas a pagar com vencimento, aprovacao e pagamento.
- Main fields: id, supplier_id, category_id, description, document_number, issue_date, due_date, amount, currency_code, status, approved_by, approved_at, paid_at, notes, metadata, created_by, created_at, updated_at
- PK: id
- FKs: supplier_id -> suppliers.id, category_id -> financial_categories.id, approved_by -> app_users.id, created_by -> app_users.id
- Required: supplier_id, category_id, description, issue_date, due_date, amount
- Status fields: status
- Timestamps: created_at, updated_at
- Notes: insumo principal do Morgan para risco de caixa e vencimentos.

### accounts_receivable (phase 1A)
- Objective: contas a receber com risco de inadimplencia.
- Main fields: id, customer_id, category_id, description, invoice_number, issue_date, due_date, amount, currency_code, status, received_at, notes, metadata, created_by, created_at, updated_at
- PK: id
- FKs: customer_id -> customers.id, category_id -> financial_categories.id, created_by -> app_users.id
- Required: customer_id, category_id, description, issue_date, due_date, amount
- Status fields: status
- Timestamps: created_at, updated_at
- Notes: base para aging e alertas de cobranca.

### financial_transactions (phase 1A)
- Objective: livro caixa consolidado de entradas e saidas.
- Main fields: id, transaction_date, direction, amount, currency_code, category_id, payable_id, receivable_id, source_system, external_ref, status, notes, metadata, created_by, created_at, updated_at
- PK: id
- FKs: category_id -> financial_categories.id, payable_id -> accounts_payable.id, receivable_id -> accounts_receivable.id, created_by -> app_users.id
- Required: transaction_date, direction, amount, category_id
- Status fields: status
- Timestamps: created_at, updated_at
- Notes: tabela central para dashboards de caixa e DRE caixa.

### approval_requests (phase 1B)
- Objective: trilha unica de aprovacao (financeiro e compras).
- Main fields: id, module_name, record_id, requested_by, approver_id, approval_type, status, requested_at, decided_at, notes, metadata, created_at, updated_at
- PK: id
- FKs: requested_by -> app_users.id, approver_id -> app_users.id
- Required: module_name, record_id, requested_by, approval_type
- Status fields: status
- Timestamps: created_at, updated_at
- Notes: evita aprovacao fora de trilha auditavel.

## Compras

### suppliers (phase 1A)
- Objective: cadastro de fornecedores com dados operacionais.
- Main fields: id, legal_name, trade_name, tax_id, email, phone, city, state, country, payment_terms_days, status, notes, metadata, created_by, created_at, updated_at
- PK: id
- FKs: created_by -> app_users.id
- Required: legal_name
- Status fields: status
- Timestamps: created_at, updated_at
- Notes: usado em contas a pagar e pedidos de compra.

### purchase_requests (phase 1A)
- Objective: solicitacoes internas de compra.
- Main fields: id, requester_user_id, supplier_id, title, description, needed_by, estimated_amount, currency_code, status, priority, approved_by, approved_at, notes, metadata, created_at, updated_at
- PK: id
- FKs: requester_user_id -> app_users.id, supplier_id -> suppliers.id, approved_by -> app_users.id
- Required: requester_user_id, title, needed_by, estimated_amount
- Status fields: status, priority
- Timestamps: created_at, updated_at
- Notes: ponto inicial do fluxo de compras.

### supplier_quotes (phase 1B)
- Objective: cotacoes de fornecedores por solicitacao.
- Main fields: id, purchase_request_id, supplier_id, quote_number, quoted_amount, delivery_days, valid_until, status, notes, metadata, created_by, created_at, updated_at
- PK: id
- FKs: purchase_request_id -> purchase_requests.id, supplier_id -> suppliers.id, created_by -> app_users.id
- Required: purchase_request_id, supplier_id, quoted_amount
- Status fields: status
- Timestamps: created_at, updated_at
- Notes: permite ranking e comparativo de fornecedores.

### purchase_orders (phase 1B)
- Objective: pedidos de compra aprovados.
- Main fields: id, purchase_request_id, supplier_id, order_number, order_date, expected_delivery_date, total_amount, currency_code, status, notes, metadata, created_by, created_at, updated_at
- PK: id
- FKs: purchase_request_id -> purchase_requests.id, supplier_id -> suppliers.id, created_by -> app_users.id
- Required: supplier_id, order_date, total_amount
- Status fields: status
- Timestamps: created_at, updated_at
- Notes: gera compromisso financeiro para contas a pagar.

## Sistema / IA

### agent_logs (phase 1B)
- Objective: logs padronizados dos agentes de IA.
- Main fields: id, agent_name, module_name, severity, message, payload, execution_id, status, occurred_at, created_at, updated_at
- PK: id
- FKs: none
- Required: agent_name, module_name, severity, message, occurred_at
- Status fields: status, severity
- Timestamps: created_at, updated_at
- Notes: fundamental para monitoracao e RCA.

### system_alerts (phase 1B)
- Objective: alertas operacionais e de negocio.
- Main fields: id, alert_type, module_name, severity, title, description, related_record_id, status, opened_at, closed_at, owner_user_id, metadata, created_at, updated_at
- PK: id
- FKs: owner_user_id -> app_users.id
- Required: alert_type, module_name, severity, title, opened_at
- Status fields: status, severity
- Timestamps: created_at, updated_at
- Notes: base de painel de saude operacional.

### tasks (phase 1B)
- Objective: tarefas acionaveis originadas por agentes/usuarios.
- Main fields: id, module_name, title, description, assigned_to, due_at, priority, status, related_record_id, created_by, metadata, created_at, updated_at
- PK: id
- FKs: assigned_to -> app_users.id, created_by -> app_users.id
- Required: module_name, title
- Status fields: status, priority
- Timestamps: created_at, updated_at
- Notes: fecha ciclo de execucao da recomendacao.

### attachments (phase 1B)
- Objective: metadados de anexos ligados a registros (link com Storage Supabase).
- Main fields: id, module_name, record_id, file_name, file_path, mime_type, file_size, uploaded_by, status, metadata, created_at, updated_at
- PK: id
- FKs: uploaded_by -> app_users.id
- Required: module_name, record_id, file_name, file_path
- Status fields: status
- Timestamps: created_at, updated_at
- Notes: usar bucket privado por modulo.

### event_history (phase 1B)
- Objective: trilha de eventos de negocio e sistema.
- Main fields: id, event_type, module_name, record_id, actor_user_id, event_payload, occurred_at, status, created_at, updated_at
- PK: id
- FKs: actor_user_id -> app_users.id
- Required: event_type, module_name, occurred_at
- Status fields: status
- Timestamps: created_at, updated_at
- Notes: fornece auditabilidade para diretoria e compliance.

---

## Bloco C - Relacionamentos

### Tabelas centrais
- app_users (centro de identidade e role)
- customers (centro comercial/recebiveis)
- suppliers (centro de compras/pagaveis)
- financial_categories (centro de classificacao financeira)
- financial_transactions (centro de consolidacao de caixa)

### Principais relacionamentos e cardinalidade
- app_users 1:N sales_reps
- sales_reps 1:N customers (owner_sales_rep_id)
- customers 1:N sales_opportunities
- customers 1:N accounts_receivable
- suppliers 1:N accounts_payable
- suppliers 1:N purchase_requests
- financial_categories 1:N accounts_payable
- financial_categories 1:N accounts_receivable
- financial_categories 1:N financial_transactions
- accounts_payable 1:N financial_transactions (saida)
- accounts_receivable 1:N financial_transactions (entrada)
- purchase_requests 1:N supplier_quotes (phase 1B)
- purchase_requests 1:N purchase_orders (phase 1B)

### Dependencias de implantacao
1. app_users
2. sales_reps, customers, financial_categories, suppliers
3. sales_opportunities, purchase_requests
4. accounts_payable, accounts_receivable
5. financial_transactions
6. tabelas phase 1B

---

## Bloco D - Regras de governanca

### Perfis (baseline)
- super_admin
- executive_admin
- finance_team
- sales_team
- procurement_team
- production_team
- quality_team
- rnd_team
- logistics_team
- hr_team
- system_admin

### Estrutura basica de permissoes
- app_users: leitura restrita por role; update apenas system_admin/super_admin no role.
- financeiro (accounts_payable, accounts_receivable, financial_transactions): finance_team + executive_admin (read), super_admin (all), system_admin (operacional tecnico).
- CRM (customers, sales_opportunities, sales_activities): sales_team no proprio escopo; executive_admin read consolidado.
- compras (suppliers, purchase_requests, supplier_quotes, purchase_orders): procurement_team escopo da area; finance_team read de impactos financeiros.
- sistema (agent_logs, system_alerts, tasks, event_history): system_admin e super_admin full; executive_admin read em visoes agregadas.

### Como preparar RLS no Supabase
1. manter role de negocio em app_users.role.
2. mapear auth.uid() -> app_users.id via app_users.auth_user_id.
3. criar funcoes helpers:
   - current_app_user_id()
   - current_user_role()
   - has_any_role(text[])
4. habilitar RLS em todas as tabelas transacionais.
5. aplicar politicas por modulo + ownership (created_by / owner_sales_rep_id / requester_user_id).

### Tabelas com maior cuidado de acesso
- accounts_payable
- accounts_receivable
- financial_transactions
- approval_requests
- app_users
- attachments (documentos sensiveis)
- event_history (auditoria)

---

## Bloco E - Sugestao de ordem de implantacao

1. Phase 1A - Core identity and masters
- app_users
- sales_reps
- customers
- financial_categories
- suppliers

2. Phase 1A - Core operations
- sales_opportunities
- purchase_requests
- accounts_payable
- accounts_receivable
- financial_transactions

3. Phase 1A - RLS baseline and views
- habilitar RLS nas 10 tabelas
- criar visoes de dashboard inicial
  - vw_finance_cash_position
  - vw_finance_overdue_receivables
  - vw_sales_pipeline_summary
  - vw_procurement_open_requests

4. Phase 1B - Complementos operacionais
- customer_contacts
- sales_activities
- approval_requests
- supplier_quotes
- purchase_orders
- agent_logs
- system_alerts
- tasks
- attachments
- event_history

5. Phase 2 - Escala e analytics
- schemas raw/mart/audit
- ingestao de Bling, Sheets, Notion, email, OpenClaw
- materialized views e snapshots executivos

---

## Entrega extra - 10 primeiras tabelas para criar no Supabase

1. app_users
2. sales_reps
3. customers
4. financial_categories
5. suppliers
6. sales_opportunities
7. purchase_requests
8. accounts_payable
9. accounts_receivable
10. financial_transactions

## SQL inicial
O SQL completo de criacao dessas 10 tabelas esta em:
- 10_banco_de_dados/migracoes/20260310_001_phase1a_core_tables.sql

O blueprint de RLS para essas tabelas esta em:
- 10_banco_de_dados/policies/20260310_001_phase1a_rls_blueprint.sql
