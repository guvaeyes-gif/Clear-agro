from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

ROOT = Path(__file__).resolve().parents[1]
GOOGLE_DIR = ROOT / "data" / "google"
CLIENT_SECRET_FILE = Path(
    os.getenv("GOOGLE_OAUTH_CLIENT_SECRET_FILE", str(GOOGLE_DIR / "client_secret.json"))
)
TOKEN_FILE = Path(os.getenv("GOOGLE_OAUTH_TOKEN_FILE", str(GOOGLE_DIR / "token.json")))

DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.settings.basic",
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


def _ensure_google_dir() -> None:
    GOOGLE_DIR.mkdir(parents=True, exist_ok=True)


def get_google_credentials(
    scopes: list[str] | None = None, force_reauth: bool = False
) -> Credentials:
    _ensure_google_dir()
    use_scopes = scopes or DEFAULT_SCOPES
    creds: Credentials | None = None

    if TOKEN_FILE.exists() and not force_reauth:
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), use_scopes)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    if not creds or not creds.valid:
        if not CLIENT_SECRET_FILE.exists():
            raise FileNotFoundError(
                f"Arquivo OAuth nao encontrado: {CLIENT_SECRET_FILE}. "
                "Baixe o OAuth Client ID (Desktop app) no Google Cloud e salve nesse caminho."
            )
        flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET_FILE), use_scopes)
        creds = flow.run_local_server(port=0)
        TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")

    return creds


def build_google_service(service_name: str, version: str, scopes: list[str] | None = None) -> Any:
    creds = get_google_credentials(scopes=scopes)
    return build(service_name, version, credentials=creds)


def gmail_profile() -> dict[str, Any]:
    svc = build_google_service("gmail", "v1")
    return svc.users().getProfile(userId="me").execute()


def list_calendars() -> list[dict[str, Any]]:
    svc = build_google_service("calendar", "v3")
    data = svc.calendarList().list(maxResults=50).execute()
    return data.get("items", [])


def list_drive_files(page_size: int = 10) -> list[dict[str, Any]]:
    svc = build_google_service("drive", "v3")
    data = (
        svc.files()
        .list(
            pageSize=page_size,
            fields="files(id,name,mimeType,modifiedTime,owners(displayName))",
        )
        .execute()
    )
    return data.get("files", [])


def read_sheet_range(spreadsheet_id: str, range_name: str) -> list[list[Any]]:
    svc = build_google_service("sheets", "v4")
    data = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return data.get("values", [])


def write_sheet_range(
    spreadsheet_id: str,
    range_name: str,
    values: list[list[Any]],
    value_input_option: str = "USER_ENTERED",
) -> dict[str, Any]:
    svc = build_google_service("sheets", "v4")
    body = {"values": values}
    data = (
        svc.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption=value_input_option,
            body=body,
        )
        .execute()
    )
    return data
