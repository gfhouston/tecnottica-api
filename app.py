import os
from contextlib import asynccontextmanager
from typing import Annotated, Any

from dotenv import load_dotenv
from fastapi import Body, Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBasic, HTTPBearer
from fastapi.responses import JSONResponse

from src.client import PlcApiClient
from src.local_auth import (
    LoginRequest,
    authenticate,
    create_access_token,
    verify_access_token,
)
from src.plc.buhler_monitor import BuhlerVentingMonitor
from src.plc.models import WriteOrderRequest
from src.plc.monitor import VentingMonitor
from src.plc.unified_registry import UnifiedMachineInfo, UnifiedRegistry

load_dotenv()

API_PREFIX = "/plc_api"
basic_auth = HTTPBasic(auto_error=False)
bearer_auth = HTTPBearer()

# --- Lifespan: client condiviso per tutta la durata dell'app ---

_client: PlcApiClient | None = None
_unified: UnifiedRegistry | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client, _unified
    _unified = UnifiedRegistry()

    log_file = os.environ.get("VENTING_LOG_FILE", "/tmp/venting_events.log")
    webhook_url = os.environ.get("VENTING_WEBHOOK_URL") or None

    monitor = VentingMonitor(
        registry=_unified.step7,
        poll_interval=float(os.environ.get("VENTING_POLL_INTERVAL_SECONDS", "300")),
        webhook_url=webhook_url,
        log_file=log_file,
    )
    monitor.start()

    buhler_monitor = BuhlerVentingMonitor(
        registry=_unified.buhler,
        poll_interval=float(os.environ.get("BUHLER_VENTING_POLL_INTERVAL_SECONDS", "300")),
        webhook_url=os.environ.get("BUHLER_VENTING_WEBHOOK_URL") or webhook_url,
        log_file=log_file,
    )
    buhler_monitor.start()

    async with PlcApiClient() as client:
        _client = client
        yield

    await monitor.stop()
    await buhler_monitor.stop()
    await _unified.disconnect_all()
    _unified = None
    _client = None


def get_client() -> PlcApiClient:
    if _client is None:
        raise RuntimeError("Client PLC non inizializzato")
    return _client


def get_unified() -> UnifiedRegistry:
    if _unified is None:
        raise RuntimeError("UnifiedRegistry non inizializzato")
    return _unified


def require_local_token(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_auth),
) -> dict[str, Any]:
    return verify_access_token(credentials.credentials)


# --- App ---

app = FastAPI(
    title="Tecnottica PLC API",
    version="0.1.0",
    lifespan=lifespan,
)


# --- Exception handler generico per errori upstream ---

@app.exception_handler(Exception)
async def upstream_error_handler(request, exc):
    detail = str(exc) or repr(exc)
    return JSONResponse(
        status_code=status.HTTP_502_BAD_GATEWAY,
        content={"detail": f"Errore upstream: {type(exc).__name__}: {detail}"},
    )


# ----------------------------------------------------------------
# AUTH
# ----------------------------------------------------------------

@app.get(f"{API_PREFIX}/health", summary="Verifica che la API locale sia attiva")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(f"{API_PREFIX}/auth/token", summary="Genera un bearer token per il frontend")
async def get_token(
    body: Annotated[LoginRequest | None, Body()] = None,
    credentials=Depends(basic_auth),
) -> Any:
    if body is not None:
        username = body.username
        password = body.password
    elif credentials is not None:
        username = credentials.username
        password = credentials.password
    else:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"detail": "Credenziali richieste"},
            headers={"WWW-Authenticate": "Basic"},
        )

    authenticate(username, password)
    token = create_access_token(username)
    return token.model_dump()


# ----------------------------------------------------------------
# MACCHINE (proxy upstream)
# ----------------------------------------------------------------

@app.get(f"{API_PREFIX}/machines", summary="Lista tutte le macchine")
async def list_machines(
    _: dict[str, Any] = Depends(require_local_token),
    client: PlcApiClient = Depends(get_client),
) -> Any:
    response = await client.get("/plc_api/machines")
    return response.json()


@app.get(f"{API_PREFIX}/machines/{{machine_id}}", summary="Dettaglio macchina")
async def get_machine(
    machine_id: str,
    _: dict[str, Any] = Depends(require_local_token),
    client: PlcApiClient = Depends(get_client),
) -> Any:
    response = await client.get(f"/plc_api/machines/{machine_id}")
    return response.json()


# ----------------------------------------------------------------
# PLC (proxy upstream)
# ----------------------------------------------------------------

@app.get(f"{API_PREFIX}/machines/{{machine_id}}/plc", summary="Lista PLC della macchina")
async def list_plc(
    machine_id: str,
    _: dict[str, Any] = Depends(require_local_token),
    client: PlcApiClient = Depends(get_client),
) -> Any:
    response = await client.get(f"/plc_api/machines/{machine_id}/plc")
    return response.json()


@app.get(f"{API_PREFIX}/machines/{{machine_id}}/plc/{{plc_id}}", summary="Dettaglio PLC")
async def get_plc(
    machine_id: str,
    plc_id: str,
    _: dict[str, Any] = Depends(require_local_token),
    client: PlcApiClient = Depends(get_client),
) -> Any:
    response = await client.get(f"/plc_api/machines/{machine_id}/plc/{plc_id}")
    return response.json()


@app.get(
    f"{API_PREFIX}/machines/{{machine_id}}/plc/{{plc_id}}/status",
    summary="Stato corrente del PLC",
)
async def get_plc_status(
    machine_id: str,
    plc_id: str,
    _: dict[str, Any] = Depends(require_local_token),
    client: PlcApiClient = Depends(get_client),
) -> Any:
    response = await client.get(f"/plc_api/machines/{machine_id}/plc/{plc_id}/status")
    return response.json()


@app.post(
    f"{API_PREFIX}/machines/{{machine_id}}/plc/{{plc_id}}/command",
    summary="Invia comando al PLC",
)
async def send_plc_command(
    machine_id: str,
    plc_id: str,
    body: dict[str, Any],
    _: dict[str, Any] = Depends(require_local_token),
    client: PlcApiClient = Depends(get_client),
) -> Any:
    response = await client.post(
        f"/plc_api/machines/{machine_id}/plc/{plc_id}/command",
        json=body,
    )
    return response.json()


# ----------------------------------------------------------------
# DIRECT PLC — endpoint unificati (Step7 + OPC-UA)
# ----------------------------------------------------------------


@app.get(
    f"{API_PREFIX}/direct/machines",
    summary="Lista tutte le macchine configurate (Step7 + OPC-UA)",
    response_model=list[UnifiedMachineInfo],
)
async def list_direct_machines(
    _: dict[str, Any] = Depends(require_local_token),
    unified: UnifiedRegistry = Depends(get_unified),
) -> list[UnifiedMachineInfo]:
    return unified.list_machines()


@app.get(
    f"{API_PREFIX}/direct/machines/{{machine_id}}/state",
    summary="Legge lo stato corrente dal PLC (Step7 → DB10 | OPC-UA → nodi BMMC)",
)
async def get_direct_machine_state(
    machine_id: str,
    _: dict[str, Any] = Depends(require_local_token),
    unified: UnifiedRegistry = Depends(get_unified),
) -> Any:
    try:
        return await unified.read_state(machine_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Macchina '{machine_id}' non trovata")
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Errore comunicazione PLC: {exc}",
        ) from exc


@app.post(
    f"{API_PREFIX}/direct/machines/{{machine_id}}/order",
    summary="Scrive commessa/ricetta sul PLC (Step7: DB10 offset 0 | OPC-UA: Process_NextRecipeName)",
)
async def write_direct_machine_order(
    machine_id: str,
    body: WriteOrderRequest,
    _: dict[str, Any] = Depends(require_local_token),
    unified: UnifiedRegistry = Depends(get_unified),
) -> dict[str, Any]:
    try:
        return await unified.write_order(machine_id, body.order_part_program)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Macchina '{machine_id}' non trovata")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Errore comunicazione PLC: {exc}",
        ) from exc
