from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ReconConfig:
    root: Path
    bank_inbox_dir: Path
    output_dir: Path
    bling_api_dir: Path
    date_window_days: int
    amount_tolerance: float

    @staticmethod
    def from_env(root: Path) -> "ReconConfig":
        inbox = Path(os.getenv("RECON_BANK_INBOX_DIR", str(root / "data" / "bank" / "inbox")))
        out = Path(os.getenv("RECON_OUTPUT_DIR", str(root / "out")))
        bling = resolve_bling_api_dir(root)
        window = int(os.getenv("RECON_DATE_WINDOW_DAYS", "2"))
        tol = float(os.getenv("RECON_AMOUNT_TOLERANCE", "1.0"))
        return ReconConfig(
            root=root,
            bank_inbox_dir=inbox,
            output_dir=out,
            bling_api_dir=bling,
            date_window_days=window,
            amount_tolerance=tol,
        )


def resolve_bling_api_dir(root: Path) -> Path:
    env_dir = (os.getenv("BLING_API_DIR") or "").strip()
    if env_dir:
        candidate = Path(env_dir)
        if candidate.is_dir():
            return candidate

    seen: set[str] = set()
    candidates: list[Path] = []

    def add_candidate(path: Path) -> None:
        key = str(path)
        if key not in seen:
            seen.add(key)
            candidates.append(path)

    for base in [root, *root.parents]:
        add_candidate(base / "bling_api")
        add_candidate(base / "CRM_Clear_Agro" / "bling_api")
        add_candidate(base / "11_agentes_automacoes" / "11_dev_codex_agent" / "repos" / "CRM_Clear_Agro" / "bling_api")

    add_candidate(Path.home() / "Documents" / "Clear_OS" / "bling_api")
    add_candidate(Path.home() / "CRM_Clear_Agro" / "bling_api")
    add_candidate(Path.home() / "bling_api")

    for candidate in candidates:
        if candidate.is_dir():
            return candidate

    raise FileNotFoundError("BLING_API_DIR nao encontrado. Configure a variavel BLING_API_DIR.")
