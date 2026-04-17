import argparse
import os
import secrets
import urllib.parse
from pathlib import Path

DEFAULT_SECRETS_FILE = Path(__file__).resolve().parent / "bling_secrets_local.txt"
SCOPES = os.getenv("BLING_SCOPES", "")  # optional
ACCOUNT_ALIASES = {"cz": "CZ", "cr": "CR"}


def _load_secrets() -> dict[str, str]:
    secrets_file = Path(os.getenv("BLING_SECRETS_FILE", str(DEFAULT_SECRETS_FILE)))
    if not secrets_file.exists():
        return {}
    out: dict[str, str] = {}
    with secrets_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _resolve(var_base: str, company: str, secrets_map: dict[str, str]) -> str:
    placeholders = {"", "seu_client_id", "seu_client_secret", "https://sua-url/callback"}
    env_company = (os.getenv(f"{var_base}_{company}") or "").strip()
    sec_company = (secrets_map.get(f"{var_base}_{company}") or "").strip()
    if env_company not in placeholders:
        return env_company
    if sec_company not in placeholders:
        return sec_company

    # Backward compatibility only for CZ legacy keys.
    if company == "CZ":
        env_generic = (os.getenv(var_base) or "").strip()
        sec_generic = (secrets_map.get(var_base) or "").strip()
        if env_generic not in placeholders:
            return env_generic
        if sec_generic not in placeholders:
            return sec_generic
    return ""


parser = argparse.ArgumentParser()
parser.add_argument(
    "--company",
    default="cz",
    choices=["cz", "cr", "CZ", "CR"],
    help="Empresa/conta Bling: cz (padrao) ou cr",
)
args = parser.parse_args()
company = ACCOUNT_ALIASES[args.company.lower()]
secrets_map = _load_secrets()

client_id = _resolve("BLING_CLIENT_ID", company, secrets_map)
redirect_uri = _resolve("BLING_REDIRECT_URI", company, secrets_map)
if not client_id or not redirect_uri:
    raise SystemExit(f"Missing BLING_CLIENT_ID/BLING_REDIRECT_URI for company {company}")

state = secrets.token_hex(16)
params = {
    "response_type": "code",
    "client_id": client_id,
    "redirect_uri": redirect_uri,
    "state": state,
}
if SCOPES:
    params["scope"] = SCOPES

url = "https://www.bling.com.br/Api/v3/oauth/authorize?" + urllib.parse.urlencode(params)
print("Open this URL in your browser, login, and authorize:")
print(url)
print("State:", state)
print(f"After redirect, run: token_exchange.py --code <CODE> --company {company.lower()}")
