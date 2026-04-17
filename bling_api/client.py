from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException

ROOT = Path(__file__).resolve().parent
TOKEN_FILE = ROOT / "bling_tokens.json"
DEFAULT_SECRETS_FILE = ROOT / "bling_secrets_local.txt"
SECRETS_FILE = Path(os.getenv("BLING_SECRETS_FILE", str(DEFAULT_SECRETS_FILE)))
OAUTH_URL = "https://www.bling.com.br/Api/v3/oauth/token"
API_BASE = "https://api.bling.com.br/Api/v3"
PLACEHOLDER_VALUES = {
    "",
    "seu_client_id",
    "seu_client_secret",
    "https://sua-url/callback",
}

ACCOUNT_ALIASES = {
    "cz": "CZ",
    "cr": "CR",
}


def _load_secrets_file() -> dict[str, str]:
    if not SECRETS_FILE.exists():
        return {}
    out: dict[str, str] = {}
    for ln in SECRETS_FILE.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln or "=" not in ln:
            continue
        k, v = ln.split("=", 1)
        out[k.strip()] = v.strip()
    return out


class BlingClient:
    def __init__(self, account: str = "cz") -> None:
        acct = ACCOUNT_ALIASES.get((account or "").strip().lower())
        if not acct:
            raise RuntimeError(f"Unsupported Bling account: {account}")
        self.account = acct

        secrets = _load_secrets_file()

        def resolve(base: str) -> str:
            env_company = (os.getenv(f"{base}_{self.account}") or "").strip()
            sec_company = (secrets.get(f"{base}_{self.account}") or "").strip()
            if env_company not in PLACEHOLDER_VALUES:
                return env_company
            if sec_company not in PLACEHOLDER_VALUES:
                return sec_company
            # Backward compatibility only for CZ legacy keys.
            if self.account == "CZ":
                env_generic = (os.getenv(base) or "").strip()
                sec_generic = (secrets.get(base) or "").strip()
                if env_generic not in PLACEHOLDER_VALUES:
                    return env_generic
                if sec_generic not in PLACEHOLDER_VALUES:
                    return sec_generic
            return ""

        self.client_id = resolve("BLING_CLIENT_ID")
        self.client_secret = resolve("BLING_CLIENT_SECRET")
        if not self.client_id or not self.client_secret:
            raise RuntimeError(f"Missing BLING_CLIENT_ID/BLING_CLIENT_SECRET for account {self.account}")

        account_token = ROOT / f"bling_tokens_{self.account.lower()}.json"
        if self.account == "CZ" and not account_token.exists():
            self.token_file = TOKEN_FILE
        else:
            self.token_file = account_token

        if not self.token_file.exists():
            raise RuntimeError(f"Token file not found: {self.token_file}")
        self.tokens = json.loads(self.token_file.read_text(encoding="utf-8"))
        if not self.tokens.get("access_token"):
            raise RuntimeError(f"access_token not found in {self.token_file.name}")

    def _save_tokens(self) -> None:
        self.token_file.write_text(json.dumps(self.tokens, ensure_ascii=False, indent=2), encoding="utf-8")

    def _refresh_token(self) -> None:
        refresh_token = self.tokens.get("refresh_token")
        if not refresh_token:
            raise RuntimeError("refresh_token not found")
        data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
        headers = {"enable-jwt": "1"}
        resp = requests.post(
            OAUTH_URL,
            data=data,
            headers=headers,
            auth=HTTPBasicAuth(self.client_id, self.client_secret),
            timeout=30,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"refresh_token failed: {resp.status_code} {resp.text}")
        payload = resp.json()
        if not payload.get("refresh_token"):
            payload["refresh_token"] = refresh_token
        self.tokens = payload
        self._save_tokens()

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None) -> requests.Response:
        url = f"{API_BASE}{path}"
        headers = {
            "Authorization": f"Bearer {self.tokens['access_token']}",
            "Content-Type": "application/json",
            "enable-jwt": "1",
        }
        backoff_s = 1.0
        last_error: Exception | None = None
        for attempt in range(6):
            try:
                resp = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    timeout=(10, 30),
                )
            except RequestException as exc:
                last_error = exc
                time.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, 8.0)
                continue
            if resp.status_code == 401:
                self._refresh_token()
                headers["Authorization"] = f"Bearer {self.tokens['access_token']}"
                continue
            if resp.status_code != 429:
                return resp
            retry_after = (resp.headers.get("Retry-After") or "").strip()
            try:
                wait_s = max(float(retry_after), backoff_s)
            except ValueError:
                wait_s = backoff_s
            time.sleep(wait_s)
            backoff_s = min(backoff_s * 2, 8.0)
        if last_error is not None:
            raise RuntimeError(f"{method} {path} failed after retries: {type(last_error).__name__}: {last_error}")
        return resp

    def get_data(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        resp = self._request("GET", path, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"GET {path} failed: {resp.status_code} {resp.text}")
        body = resp.json()
        return body.get("data", []) if isinstance(body, dict) else []

    def get_detail(self, path: str) -> dict[str, Any]:
        resp = self._request("GET", path, params=None)
        if resp.status_code != 200:
            raise RuntimeError(f"GET {path} failed: {resp.status_code} {resp.text}")
        body = resp.json()
        if isinstance(body, dict):
            return body.get("data", body)
        return {}
