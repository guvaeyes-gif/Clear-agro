# Repository Guidelines

## Project Structure & Module Organization
- `app/`: Streamlit UI entrypoint (`app/main.py`).
- `src/`: core Python modules (ingest, transform, reports, integrations, helpers).
- `scripts/`: operational scripts (validation, Telegram/Google utilities, task registration).
- `tests/`: pytest test suite.
- `bling_api/`: Bling OAuth/sync scripts and local cache files (`*_cache.jsonl`).
- `playwright-e2e/`: Node + Playwright automations and E2E checks.
- `data/`, `out/`, `docs/`, `config/`: datasets, generated outputs, docs, and mappings.

## Build, Test, and Development Commands
- Bootstrap dependencies:
  - `make bootstrap`
- Run dashboard locally:
  - `make dashboard` or `python -m streamlit run app/main.py`
- Run finance ingestion/report pipeline:
  - `make ingest`
  - `make reports`
  - `make finance`
  - `make finance-all`
- Validate current KPI build:
  - `make build` (`python scripts/validate_kpis.py`)
- Quality checks:
  - `make lint`
  - `make test`
- Playwright (from `playwright-e2e/`):
  - `npm test`, `npm run test:ui`, `npm run report`

## Coding Style & Naming Conventions
- Python style is enforced with Ruff (`pyproject.toml`):
  - line length `100`, target `py311`
  - enabled rules: `E`, `F`, `W`, `I`
- Use `snake_case` for Python functions/variables/files and clear, domain-specific names.
- Keep Streamlit/UI text concise and operational; avoid hardcoded secrets in code.

## Testing Guidelines
- Primary framework: `pytest` (configured with `testpaths = ["tests"]`, `-q`).
- Test files should follow `test_*.py`.
- Prefer small unit tests for `src/` logic plus integration checks for ingest/reconcile flows.
- For browser automations, use Playwright tests in `playwright-e2e/`.

## Commit & Pull Request Guidelines
- Follow existing history style: short, imperative, outcome-focused subjects (e.g., `Fix KPI totals for quarter filter`).
- Keep commits scoped to one logical change.
- PRs should include:
  - what changed and why,
  - impacted paths/modules,
  - validation commands run (`make lint`, `make test`, relevant scripts),
  - screenshots for UI changes (`app/main.py`) when applicable.

## Security & Configuration Tips
- Never commit tokens or credentials (`bling_tokens.json`, OAuth secrets, Telegram/OpenAI keys).
- Prefer environment variables and local secret files ignored by git.
- Treat files in `out/` as generated artifacts unless explicitly required in versioned docs.
