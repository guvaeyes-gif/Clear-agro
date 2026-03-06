from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


def stable_hash(values: Iterable[object]) -> str:
    payload = "|".join("" if v is None else str(v) for v in values)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def save_quality_log(path: Path, content: dict) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
