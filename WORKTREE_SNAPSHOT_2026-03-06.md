# Worktree Snapshot - 2026-03-06

Branch: `main`
Objective: keep the repository auditable while preserving ongoing local work.

## Commit plan by theme
1. `chore(gitignore): harden local ignores and isolate heavy local folders`
- Scope: `.gitignore`
- Purpose: block accidental commit of tokens, cache artifacts, generated outputs and local lab folders.

2. `fix(scheduler): use CRM_Clear_Agro root for Bling daily task`
- Scope: `scripts/register_bling_daily_task.ps1`
- Purpose: ensure scheduled task points to the correct project root.

## Pending local work (not committed in this snapshot)
- Modified tracked files:
  - `bling_api/nfe_2026_cache.jsonl`
  - `data/metas.db`
  - `out/validacao_kpis.md`
- Deleted tracked files:
  - `pipeline/01_inventario.py`
  - `pipeline/02_padronizar_mapear.py`
  - `pipeline/03_unificar_deduplicar.py`
  - `pipeline/04_inteligencia.py`
  - `pipeline/05_exportar_outputs.py`
- Large set of untracked source/docs/scripts files still pending thematic commits.

## Suggested next commit batches
1. `feat(core): app/src/scripts baseline import`
2. `feat(integrations): bling/google/telegram automation`
3. `docs(ops): operating manual and data dictionary`
4. `chore(data): add only approved fixtures; keep caches local`
