import os

import httpx
from dotenv import load_dotenv

from .auth import AuthManager, TokenResponse
from .settings import get_bool_env

load_dotenv()


class PlcApiClient:
    """
    Client HTTP async per le API PLC.
    Il bearer token viene iniettato automaticamente ad ogni richiesta
    e rinnovato trasparentemente alla scadenza.

    Uso come context manager:
        async with PlcApiClient() as client:
            data = await client.get("/plc_api/machines")
    """

    def __init__(
        self,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        verify_ssl: bool | None = None,
    ) -> None:
        self._base_url = (base_url or os.environ["PLC_API_BASE_URL"]).rstrip("/")
        self._verify_ssl = (
            verify_ssl
            if verify_ssl is not None
            else get_bool_env("PLC_API_VERIFY_SSL", True)
        )
        self._auth = AuthManager(
            base_url=self._base_url,
            username=username or os.environ["PLC_API_USERNAME"],
            password=password or os.environ["PLC_API_PASSWORD"],
            verify_ssl=self._verify_ssl,
        )
        self._http: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "PlcApiClient":
        self._http = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=30.0,
            verify=self._verify_ssl,
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._http:
            await self._http.aclose()

    async def get(self, path: str, **kwargs: object) -> httpx.Response:
        return await self._request("GET", path, **kwargs)

    async def post(self, path: str, **kwargs: object) -> httpx.Response:
        return await self._request("POST", path, **kwargs)

    async def put(self, path: str, **kwargs: object) -> httpx.Response:
        return await self._request("PUT", path, **kwargs)

    async def delete(self, path: str, **kwargs: object) -> httpx.Response:
        return await self._request("DELETE", path, **kwargs)

    async def get_token_response(self) -> TokenResponse:
        return await self._auth.get_token_response()

    async def _request(self, method: str, path: str, **kwargs: object) -> httpx.Response:
        assert self._http is not None, "Usa il client come context manager (async with)"
        token = await self._auth.get_valid_token()
        headers = kwargs.pop("headers", {})  # type: ignore[assignment]
        headers["Authorization"] = f"Bearer {token}"
        response = await self._http.request(method, path, headers=headers, **kwargs)
        response.raise_for_status()
        return response
