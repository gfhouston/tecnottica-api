import asyncio
import base64
import time
from dataclasses import dataclass, field

import httpx
from pydantic import BaseModel

from .settings import get_bool_env


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # secondi


@dataclass
class TokenState:
    access_token: str
    expires_at: float  # timestamp Unix
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def is_expired(self, buffer_seconds: int = 30) -> bool:
        """Considera il token scaduto 30s prima della scadenza effettiva."""
        return time.monotonic() >= (self.expires_at - buffer_seconds)


class AuthManager:
    """
    Gestisce l'autenticazione Basic Auth e il refresh automatico del bearer token.

    Uso:
        auth = AuthManager(base_url, username, password)
        token = await auth.get_valid_token()
    """

    def __init__(
        self, base_url: str, username: str, password: str, verify_ssl: bool | None = None
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._basic_header = _build_basic_header(username, password)
        self._verify_ssl = (
            verify_ssl
            if verify_ssl is not None
            else get_bool_env("PLC_API_VERIFY_SSL", True)
        )
        self._token_state: TokenState | None = None
        self._lock = asyncio.Lock()

    async def get_valid_token(self) -> str:
        """Restituisce un token valido, acquisendone uno nuovo o rinnovandolo se scaduto."""
        async with self._lock:
            if self._token_state is None or self._token_state.is_expired():
                await self._fetch_token()
        return self._token_state.access_token  # type: ignore[union-attr]

    async def get_token_response(self) -> TokenResponse:
        """Restituisce il token nel formato atteso dal frontend/API."""
        token = await self.get_valid_token()
        expires_in = max(0, int(self._token_state.expires_at - time.monotonic()))  # type: ignore[union-attr]
        return TokenResponse(access_token=token, expires_in=expires_in)

    async def _fetch_token(self) -> None:
        async with httpx.AsyncClient(verify=self._verify_ssl) as client:
            response = await client.post(
                f"{self._base_url}/plc_api/auth/token",
                headers={
                    "Authorization": self._basic_header,
                    "Content-Type": "application/json",
                },
            )
            response.raise_for_status()

        data = TokenResponse.model_validate(response.json())
        self._token_state = TokenState(
            access_token=data.access_token,
            expires_at=time.monotonic() + data.expires_in,
        )


def _build_basic_header(username: str, password: str) -> str:
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {credentials}"
