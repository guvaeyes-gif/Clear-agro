from __future__ import annotations

import argparse
import json
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Atualiza o monitor do Codex a partir dos logs locais em ~/.codex/sessions."
    )
    parser.add_argument(
        "--codex-home",
        default=Path.home() / ".codex",
        type=Path,
        help="Diretorio do Codex local.",
    )
    parser.add_argument(
        "--output",
        default=Path("docs/codex-monitor/usage.json"),
        type=Path,
        help="Arquivo JSON usado pelo monitor.",
    )
    parser.add_argument(
        "--publish-remote",
        default=None,
        help="Se informado, faz git add/commit/push para esse remoto quando houver mudanca.",
    )
    parser.add_argument(
        "--commit-message",
        default="Update Codex monitor automatically",
        help="Mensagem de commit usada com --publish-remote.",
    )
    return parser.parse_args()


@dataclass
class RateSample:
    timestamp: datetime
    primary_used_percent: float
    primary_window_minutes: int
    primary_resets_at: datetime | None
    secondary_used_percent: float
    secondary_window_minutes: int
    secondary_resets_at: datetime | None
    plan_type: str | None


def parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
      return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone()


def parse_epoch_timestamp(value: int | float | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).astimezone()


def iter_session_files(codex_home: Path) -> list[Path]:
    sessions_dir = codex_home / "sessions"
    return sorted(sessions_dir.rglob("*.jsonl"))


def load_samples(codex_home: Path) -> list[RateSample]:
    samples: list[RateSample] = []
    for file_path in iter_session_files(codex_home):
        try:
            with file_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if item.get("type") != "event_msg":
                        continue
                    payload = item.get("payload") or {}
                    if payload.get("type") != "token_count":
                        continue
                    rate_limits = payload.get("rate_limits") or {}
                    primary = rate_limits.get("primary") or {}
                    secondary = rate_limits.get("secondary") or {}
                    timestamp = parse_iso_timestamp(item.get("timestamp"))
                    if not timestamp:
                        continue
                    samples.append(
                        RateSample(
                            timestamp=timestamp,
                            primary_used_percent=float(primary.get("used_percent") or 0),
                            primary_window_minutes=int(primary.get("window_minutes") or 300),
                            primary_resets_at=parse_epoch_timestamp(primary.get("resets_at")),
                            secondary_used_percent=float(secondary.get("used_percent") or 0),
                            secondary_window_minutes=int(secondary.get("window_minutes") or 10080),
                            secondary_resets_at=parse_epoch_timestamp(secondary.get("resets_at")),
                            plan_type=rate_limits.get("plan_type"),
                        )
                    )
        except OSError:
            continue
    samples.sort(key=lambda sample: sample.timestamp)
    return samples


def build_daily_usage(samples: list[RateSample]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, dict[int, float]] = defaultdict(dict)
    for sample in samples:
        month_key = sample.timestamp.strftime("%Y-%m")
        day = sample.timestamp.day
        current_max = grouped[month_key].get(day, 0.0)
        grouped[month_key][day] = max(current_max, sample.primary_used_percent)

    result: dict[str, list[dict[str, Any]]] = {}
    for month_key in sorted(grouped):
        result[month_key] = [
            {"day": f"{day:02d}", "value": round(value, 1)}
            for day, value in sorted(grouped[month_key].items())
        ]
    return result


def build_payload(samples: list[RateSample]) -> dict[str, Any]:
    if not samples:
        return {
            "generated_at": None,
            "plan_type": None,
            "current": {
                "primary_used_percent": 0,
                "primary_remaining_percent": 0,
                "primary_window_minutes": 300,
                "primary_resets_at": None,
                "secondary_used_percent": 0,
                "secondary_remaining_percent": 0,
                "secondary_window_minutes": 10080,
                "secondary_resets_at": None,
            },
            "daily_usage": {},
        }

    latest = samples[-1]
    return {
        "generated_at": latest.timestamp.strftime("%d/%m/%Y %H:%M"),
        "plan_type": latest.plan_type,
        "current": {
            "primary_used_percent": round(latest.primary_used_percent, 1),
            "primary_remaining_percent": round(max(0.0, 100 - latest.primary_used_percent), 1),
            "primary_window_minutes": latest.primary_window_minutes,
            "primary_resets_at": latest.primary_resets_at.strftime("%d/%m %H:%M")
            if latest.primary_resets_at
            else None,
            "secondary_used_percent": round(latest.secondary_used_percent, 1),
            "secondary_remaining_percent": round(max(0.0, 100 - latest.secondary_used_percent), 1),
            "secondary_window_minutes": latest.secondary_window_minutes,
            "secondary_resets_at": latest.secondary_resets_at.strftime("%d/%m %H:%M")
            if latest.secondary_resets_at
            else None,
        },
        "daily_usage": build_daily_usage(samples),
    }


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def write_json_if_changed(path: Path, payload: dict[str, Any]) -> bool:
    current = read_json(path)
    if current == payload:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    return True


def current_branch(repo_root: Path) -> str:
    result = subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def publish(repo_root: Path, output_path: Path, remote: str, commit_message: str) -> None:
    relative_output = output_path.as_posix()
    subprocess.run(["git", "add", "--", relative_output], cwd=repo_root, check=True)
    subprocess.run(["git", "commit", "-m", commit_message], cwd=repo_root, check=True)
    subprocess.run(
        ["git", "push", remote, current_branch(repo_root)],
        cwd=repo_root,
        check=True,
    )


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    output_path = (repo_root / args.output).resolve()
    samples = load_samples(args.codex_home)
    payload = build_payload(samples)
    changed = write_json_if_changed(output_path, payload)

    if changed:
        print(f"Monitor atualizado em {output_path}")
    else:
        print("Sem mudancas no monitor.")

    if changed and args.publish_remote:
        publish(repo_root, output_path, args.publish_remote, args.commit_message)
        print(f"Publicacao concluida no remoto {args.publish_remote}.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
