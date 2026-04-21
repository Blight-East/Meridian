#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import secrets
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import requests


SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.modify",
]


def mask(value: str) -> str:
    if not value:
        return "<missing>"
    if len(value) < 12:
        return "<set>"
    return f"{value[:6]}...{value[-6:]}"


def load_client(path: Path) -> dict:
    data = json.loads(path.read_text())
    section = data.get("web") or data.get("installed")
    if not section:
        raise ValueError("Credentials file must contain a 'web' or 'installed' OAuth client section.")
    return section


def build_auth_url(client: dict, state: str) -> str:
    params = {
        "client_id": client["client_id"],
        "redirect_uri": client["redirect_uris"][0],
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    return f"{client['auth_uri']}?{urlencode(params)}"


def exchange_code(client: dict, redirect_url: str) -> dict:
    parsed = urlparse(redirect_url)
    query = parse_qs(parsed.query)
    code = (query.get("code") or [None])[0]
    if not code:
        raise ValueError("Redirect URL does not contain an OAuth code.")
    response = requests.post(
        client["token_uri"],
        data={
            "code": code,
            "client_id": client["client_id"],
            "client_secret": client["client_secret"],
            "redirect_uri": client["redirect_uris"][0],
            "grant_type": "authorization_code",
        },
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def upsert_env(env_path: Path, updates: dict[str, str]):
    lines = env_path.read_text().splitlines() if env_path.exists() else []
    existing = {}
    for idx, line in enumerate(lines):
        if "=" in line and not line.lstrip().startswith("#"):
            key = line.split("=", 1)[0]
            existing[key] = idx
    for key, value in updates.items():
        line = f"{key}={value}"
        if key in existing:
            lines[existing[key]] = line
        else:
            lines.append(line)
    env_path.write_text("\n".join(lines).rstrip() + "\n")


def main():
    parser = argparse.ArgumentParser(description="Bootstrap a Gmail OAuth refresh token for Agent Flux.")
    parser.add_argument("--client-secret-file", required=True, help="Path to Google OAuth client secret JSON.")
    parser.add_argument("--redirect-url", help="Full redirect URL copied from the browser after consent.")
    parser.add_argument("--env-file", help="Optional .env file to update with GOOGLE_* values.")
    args = parser.parse_args()

    client = load_client(Path(args.client_secret_file).expanduser())
    print("client_id", mask(client.get("client_id", "")))
    print("client_secret", mask(client.get("client_secret", "")))
    print("redirect_uri", client["redirect_uris"][0])
    print("scopes", ",".join(SCOPES))

    if not args.redirect_url:
        state = secrets.token_urlsafe(16)
        print("state", state)
        print("auth_url", build_auth_url(client, state))
        print("next", "Open auth_url in a browser, approve the Gmail mailbox, then rerun with --redirect-url '<full redirected URL>'.")
        return

    tokens = exchange_code(client, args.redirect_url)
    refresh_token = tokens.get("refresh_token", "")
    if not refresh_token:
        raise RuntimeError("Google did not return a refresh token. Re-run the consent flow with prompt=consent.")

    print("refresh_token", mask(refresh_token))
    if args.env_file:
        env_path = Path(args.env_file).expanduser()
        upsert_env(
            env_path,
            {
                "GOOGLE_CLIENT_ID": client["client_id"],
                "GOOGLE_CLIENT_SECRET": client["client_secret"],
                "GOOGLE_REFRESH_TOKEN": refresh_token,
            },
        )
        print("env_updated", str(env_path))


if __name__ == "__main__":
    main()
