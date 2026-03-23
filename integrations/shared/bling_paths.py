from __future__ import annotations

import json
import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ROOT_BLING_DIR = ROOT / "bling_api"
DEFAULT_NESTED_BLING_DIR = (
    ROOT / "11_agentes_automacoes" / "11_dev_codex_agent" / "repos" / "CRM_Clear_Agro" / "bling_api"
)
CONFIG_PATH = ROOT / "config" / "paths" / "bling_cache_roots.json"

APP_REQUIRED = (
    "vendas_2026_cache.jsonl",
    "nfe_2026_cache.jsonl",
    "vendedores_map.csv",
)
PIPELINE_REQUIRED = (
    "contatos_cache.jsonl",
    "contas_pagar_cache.jsonl",
    "contas_receber_cache.jsonl",
)
DEFAULT_SYNC_FILES = (
    "contatos_cache.jsonl",
    "contatos_cache_cr.jsonl",
    "contas_pagar_cache.jsonl",
    "contas_pagar_cache_cr.jsonl",
    "contas_receber_cache.jsonl",
    "contas_receber_cache_cr.jsonl",
    "estoque_cache.jsonl",
    "estoque_cache_cr.jsonl",
    "nfe_2025_cache.jsonl",
    "nfe_2026_cache.jsonl",
    "nfe_2026_cache_cr.jsonl",
    "produtos_cache.jsonl",
    "produtos_cache_cr.jsonl",
    "produtos_composicao_cache.jsonl",
    "produtos_composicao_cache_cr.jsonl",
    "vendas_2025_cache.jsonl",
    "vendas_2026_cache.jsonl",
    "vendas_2026_cache_cr.jsonl",
    "vendedores_map.csv",
    "vendedores_map_cr.csv",
)


def _default_config() -> dict[str, object]:
    return {
        "canonical_root": "bling_api",
        "compatibility_root": "11_agentes_automacoes/11_dev_codex_agent/repos/CRM_Clear_Agro/bling_api",
        "app_preferred_root": "bling_api",
        "pipeline_preferred_root": "bling_api",
        "sync_source_root": "11_agentes_automacoes/11_dev_codex_agent/repos/CRM_Clear_Agro/bling_api",
        "sync_target_root": "bling_api",
        "sync_files": list(DEFAULT_SYNC_FILES),
    }


def _load_config() -> dict[str, object]:
    if not CONFIG_PATH.exists():
        return _default_config()
    try:
        payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return _default_config()
    if not isinstance(payload, dict):
        return _default_config()
    out = _default_config()
    out.update(payload)
    return out


def _resolve_root_value(value: str | None, fallback: Path) -> Path:
    text = str(value or "").strip()
    if not text:
        return fallback
    path = Path(text)
    if path.is_absolute():
        return path
    return ROOT / path


def _dedupe(items: list[Path]) -> list[Path]:
    seen: set[str] = set()
    out: list[Path] = []
    for item in items:
        key = str(item).lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def get_bling_root_config() -> dict[str, object]:
    return _load_config()


def get_bling_root_paths() -> dict[str, Path]:
    config = _load_config()
    return {
        "canonical_root": _resolve_root_value(config.get("canonical_root"), DEFAULT_ROOT_BLING_DIR),
        "compatibility_root": _resolve_root_value(
            config.get("compatibility_root"), DEFAULT_NESTED_BLING_DIR
        ),
        "app_preferred_root": _resolve_root_value(config.get("app_preferred_root"), DEFAULT_ROOT_BLING_DIR),
        "pipeline_preferred_root": _resolve_root_value(
            config.get("pipeline_preferred_root"), DEFAULT_ROOT_BLING_DIR
        ),
        "sync_source_root": _resolve_root_value(config.get("sync_source_root"), DEFAULT_NESTED_BLING_DIR),
        "sync_target_root": _resolve_root_value(config.get("sync_target_root"), DEFAULT_ROOT_BLING_DIR),
    }


def get_bling_sync_files() -> list[str]:
    config = _load_config()
    raw = config.get("sync_files")
    if not isinstance(raw, list):
        return list(DEFAULT_SYNC_FILES)
    return [str(item).strip() for item in raw if str(item).strip()]


def _candidate_roots(mode: str) -> list[Path]:
    shared = os.getenv("CLEAR_OS_BLING_ROOT", "").strip()
    app_root = os.getenv("CLEAR_OS_BLING_APP_ROOT", "").strip()
    pipeline_root = os.getenv("CLEAR_OS_BLING_PIPELINE_ROOT", "").strip()
    configured = get_bling_root_paths()
    canonical_root = configured["canonical_root"]
    compatibility_root = configured["compatibility_root"]
    app_preferred_root = configured["app_preferred_root"]
    pipeline_preferred_root = configured["pipeline_preferred_root"]

    if mode == "pipeline":
        ordered = [
            Path(pipeline_root) if pipeline_root else None,
            Path(shared) if shared else None,
            pipeline_preferred_root,
            canonical_root,
            compatibility_root,
        ]
    else:
        ordered = [
            Path(app_root) if app_root else None,
            Path(shared) if shared else None,
            app_preferred_root,
            canonical_root,
            compatibility_root,
        ]
    return _dedupe([path for path in ordered if path is not None])


def _score_root(root: Path, required_files: tuple[str, ...]) -> int:
    return sum(1 for name in required_files if (root / name).exists())


def resolve_bling_root(mode: str = "app") -> Path:
    mode_key = (mode or "app").strip().lower()
    required = PIPELINE_REQUIRED if mode_key == "pipeline" else APP_REQUIRED
    candidates = _candidate_roots(mode_key)

    complete = [root for root in candidates if root.exists() and _score_root(root, required) == len(required)]
    if complete:
        return complete[0]

    existing = [root for root in candidates if root.exists()]
    if existing:
        ranked = sorted(
            existing,
            key=lambda root: (_score_root(root, required), -candidates.index(root)),
            reverse=True,
        )
        return ranked[0]

    return candidates[0]


def resolve_bling_file(filename: str, mode: str = "app") -> Path:
    primary = resolve_bling_root(mode)
    primary_path = primary / filename
    if primary_path.exists():
        return primary_path

    for root in _candidate_roots(mode):
        candidate = root / filename
        if candidate.exists():
            return candidate

    return primary_path


def describe_bling_roots() -> dict[str, str]:
    configured = get_bling_root_paths()
    app_root = resolve_bling_root("app")
    pipeline_root = resolve_bling_root("pipeline")
    return {
        "canonical_root": str(configured["canonical_root"]),
        "compatibility_root": str(configured["compatibility_root"]),
        "app_root": str(app_root),
        "pipeline_root": str(pipeline_root),
        "sync_source_root": str(configured["sync_source_root"]),
        "sync_target_root": str(configured["sync_target_root"]),
        "root_bling_dir": str(DEFAULT_ROOT_BLING_DIR),
        "nested_bling_dir": str(DEFAULT_NESTED_BLING_DIR),
    }
