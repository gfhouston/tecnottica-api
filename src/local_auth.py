import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any

from fastapi import HTTPException, status
from pydantic import BaseModel


class LoginRequest(BaseModel):
    username: str
    password: str


class LocalTokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


def get_auth_username() -> str:
    return os.environ.get("LOCAL_AUTH_USERNAME") or os.environ["PLC_API_USERNAME"]


def get_auth_password() -> str:
    return os.environ.get("LOCAL_AUTH_PASSWORD") or os.environ["PLC_API_PASSWORD"]


def get_token_secret() -> str:
    return os.environ.get("LOCAL_AUTH_SECRET") or get_auth_password()


def get_token_expires_seconds() -> int:
    return int(os.environ.get("LOCAL_AUTH_TOKEN_EXPIRES_SECONDS", "3600"))


def authenticate(username: str, password: str) -> None:
    valid_username = hmac.compare_digest(username, get_auth_username())
    valid_password = hmac.compare_digest(password, get_auth_password())
    if not (valid_username and valid_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenziali non valide",
            headers={"WWW-Authenticate": "Basic"},
        )


def create_access_token(subject: str) -> LocalTokenResponse:
    expires_in = get_token_expires_seconds()
    payload = {
        "sub": subject,
        "iat": int(time.time()),
        "exp": int(time.time()) + expires_in,
    }
    token = _encode_token(payload)
    return LocalTokenResponse(access_token=token, expires_in=expires_in)


def verify_access_token(token: str) -> dict[str, Any]:
    try:
        payload_part, signature = token.rsplit(".", 1)
    except ValueError as exc:
        raise _invalid_token() from exc

    expected_signature = _sign(payload_part)
    if not hmac.compare_digest(signature, expected_signature):
        raise _invalid_token()

    try:
        payload = json.loads(_base64url_decode(payload_part))
    except (ValueError, json.JSONDecodeError) as exc:
        raise _invalid_token() from exc

    if int(payload.get("exp", 0)) <= int(time.time()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token scaduto",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return payload


def _encode_token(payload: dict[str, Any]) -> str:
    payload_part = _base64url_encode(json.dumps(payload, separators=(",", ":")).encode())
    signature = _sign(payload_part)
    return f"{payload_part}.{signature}"


def _sign(value: str) -> str:
    digest = hmac.new(
        get_token_secret().encode(),
        value.encode(),
        hashlib.sha256,
    ).digest()
    return _base64url_encode(digest)


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _base64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _invalid_token() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token non valido",
        headers={"WWW-Authenticate": "Bearer"},
    )
