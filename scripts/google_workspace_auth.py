from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.google_workspace import CLIENT_SECRET_FILE, TOKEN_FILE, DEFAULT_SCOPES, get_google_credentials


def main() -> int:
    print(f"Client secret: {CLIENT_SECRET_FILE}")
    print(f"Token file: {TOKEN_FILE}")
    print("Abrindo fluxo OAuth no navegador...")
    creds = get_google_credentials(scopes=DEFAULT_SCOPES)
    print(f"OAuth concluido. Token salvo em: {TOKEN_FILE}")
    print(f"Scopes ativos: {len(creds.scopes or [])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
