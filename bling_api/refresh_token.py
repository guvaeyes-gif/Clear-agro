import argparse
import json
import os

import requests
from requests.auth import HTTPBasicAuth

SECRETS_FILE = r"C:\Users\cesar.zarovski\Documents\bling id.txt"
ACCOUNT_ALIASES = {"cz": "CZ", "cr": "CR"}
PLACEHOLDER_VALUES = {
    "",
    "seu_client_id",
    "seu_client_secret",
    "https://sua-url/callback",
}


def load_secrets_from_file():
    if not os.path.exists(SECRETS_FILE):
        return {}
    data = {}
    with open(SECRETS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            data[k.strip()] = v.strip()
    return data


def _resolve(var_base: str, company: str, secrets: dict[str, str]) -> str:
    env_company = (os.getenv(f"{var_base}_{company}") or "").strip()
    sec_company = (secrets.get(f"{var_base}_{company}") or "").strip()
    if env_company not in PLACEHOLDER_VALUES:
        return env_company
    if sec_company not in PLACEHOLDER_VALUES:
        return sec_company

    # Backward compatibility only for CZ legacy keys.
    if company == "CZ":
        env_generic = (os.getenv(var_base) or "").strip()
        sec_generic = (secrets.get(var_base) or "").strip()
        if env_generic not in PLACEHOLDER_VALUES:
            return env_generic
        if sec_generic not in PLACEHOLDER_VALUES:
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

secrets = load_secrets_from_file()
client_id = _resolve("BLING_CLIENT_ID", company, secrets)
client_secret = _resolve("BLING_CLIENT_SECRET", company, secrets)
if not client_id or not client_secret:
    raise SystemExit(f"Missing BLING_CLIENT_ID/BLING_CLIENT_SECRET for company {company}")

if company == "CZ":
    token_file = os.path.join(os.path.dirname(__file__), "bling_tokens.json")
else:
    token_file = os.path.join(os.path.dirname(__file__), f"bling_tokens_{company.lower()}.json")

if not os.path.exists(token_file):
    raise SystemExit(f"Token file not found: {token_file}. Run token_exchange.py first.")

with open(token_file, "r", encoding="utf-8") as f:
    tokens = json.load(f)

refresh_token = tokens.get("refresh_token")
if not refresh_token:
    raise SystemExit("refresh_token not found in token file")

url = "https://www.bling.com.br/Api/v3/oauth/token"
data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
headers = {"enable-jwt": "1"}
resp = requests.post(url, data=data, headers=headers, auth=HTTPBasicAuth(client_id, client_secret), timeout=30)
if resp.status_code != 200:
    raise SystemExit(f"Refresh failed: {resp.status_code} {resp.text}")

payload = resp.json()
if not payload.get("refresh_token"):
    payload["refresh_token"] = refresh_token

with open(token_file, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)

print(f"Tokens refreshed and saved: {token_file} (company={company})")
