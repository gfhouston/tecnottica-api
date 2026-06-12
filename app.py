import os
from contextlib import asynccontextmanager
from typing import Annotated, Any

import aiomysql
from dotenv import load_dotenv
from fastapi import Body, Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBasic, HTTPBearer
from fastapi.responses import JSONResponse

from src.db import init_db, save_assignment
from src.settings import EmailSettings
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
from src.plc.webhook_retry import WebhookRetryWorker

load_dotenv()

API_PREFIX = "/plc_api"
basic_auth = HTTPBasic(auto_error=False)
bearer_auth = HTTPBearer()

_unified: UnifiedRegistry | None = None
_db: aiomysql.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _unified, _db

    _db = await init_db(
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ.get("DB_USER", "root"),
        password=os.environ.get("DB_PASSWORD", ""),
        database=os.environ.get("DB_NAME", "tecnottica"),
    )

    plc_timeout = float(os.environ.get("PLC_TIMEOUT_SECONDS", "10"))
    _unified = UnifiedRegistry(plc_timeout=plc_timeout)

    log_file = os.environ.get("VENTING_LOG_FILE", "/tmp/venting_events.log")
    webhook_url = os.environ.get("VENTING_WEBHOOK_URL") or None
    slack_webhook_url = os.environ.get(
        "SLACK_WEBHOOK_URL",
        "",
    )
    smtp_port_raw = os.environ.get("SMTP_PORT", "465")
    try:
        smtp_port = int(smtp_port_raw)
    except ValueError:
        smtp_port = 465
    configured_email_settings = EmailSettings(
        smtp_host=os.environ.get("SMTP_HOST", ""),
        smtp_port=smtp_port,
        smtp_user=os.environ.get("SMTP_USER", ""),
        smtp_password=os.environ.get("SMTP_PASSWORD", ""),
        from_name=os.environ.get("EMAIL_FROM_NAME", ""),
        to_address=os.environ.get("EMAIL_TO", ""),
    )
    email_settings = configured_email_settings if configured_email_settings.is_configured else None

    monitor = VentingMonitor(
        registry=_unified.step7,
        poll_interval=float(os.environ.get("VENTING_POLL_INTERVAL_SECONDS", "300")),
        webhook_url=webhook_url,
        log_file=log_file,
        db=_db,
        slack_webhook_url=slack_webhook_url,
        email_settings=email_settings,
        trigger_mode=os.environ.get("OPTOTECH_END_OF_WORK_TRIGGER", "venting_stop"),
        plc_timeout=plc_timeout,
    )
    monitor.start()

    buhler_monitor = BuhlerVentingMonitor(
        registry=_unified.buhler,
        poll_interval=float(os.environ.get("BUHLER_VENTING_POLL_INTERVAL_SECONDS", "300")),
        webhook_url=os.environ.get("BUHLER_VENTING_WEBHOOK_URL") or webhook_url,
        log_file=log_file,
        db=_db,
        slack_webhook_url=slack_webhook_url,
        email_settings=email_settings,
        plc_timeout=plc_timeout,
    )
    buhler_monitor.start()

    retry_worker = WebhookRetryWorker(
        pool=_db,
        retry_interval=float(os.environ.get("WEBHOOK_RETRY_INTERVAL_SECONDS", "60")),
        log_file=log_file,
    )
    retry_worker.start()

    yield

    await monitor.stop()
    await buhler_monitor.stop()
    await retry_worker.stop()
    await _unified.disconnect_all()
    _db.close()
    await _db.wait_closed()
    _unified = None
    _db = None


def get_unified() -> UnifiedRegistry:
    if _unified is None:
        raise RuntimeError("UnifiedRegistry non inizializzato")
    return _unified


def get_db() -> aiomysql.Pool:
    if _db is None:
        raise RuntimeError("Database non inizializzato")
    return _db


def require_local_token(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_auth),
) -> dict[str, Any]:
    return verify_access_token(credentials.credentials)


# --- App ---

_tags_metadata = [
    {
        "name": "autenticazione",
        "description": "Generazione e verifica del **bearer token** utilizzato da tutte le altre route.",
    },
    {
        "name": "macchine dirette",
        "description": (
            "Lettura e scrittura **diretta** sui PLC, senza passare per l'upstream. "
            "Supporta macchine **Step7** (snap7 / DB10 Optotech) e macchine **OPC-UA** (Buhler BMMC)."
        ),
    },
]

app = FastAPI(
    title="Tecnottica PLC API",
    version="0.1.0",
    docs_url=f"{API_PREFIX}/docs",
    redoc_url=f"{API_PREFIX}/redoc",
    openapi_url=f"{API_PREFIX}/openapi.json",
    description="""
API per la comunicazione con i PLC delle macchine industriali Tecnottica.

## Protocolli supportati
- **Step7 (snap7)** — macchine Optotech Scandicci; lettura stato da DB10, scrittura commessa a offset 0.
- **OPC-UA (asyncua)** — macchine Buhler BMMC; nodi `CMD`, `ACT` e `STA`.

## Autenticazione
Tutte le route (eccetto `/health`) richiedono un **bearer token** JWT-like ottenuto tramite
`POST /plc_api/auth/token` con credenziali Basic o body JSON.
""",
    contact={"name": "Tecnottica Srl"},
    openapi_tags=_tags_metadata,
    lifespan=lifespan,
)


# --- Exception handler generico ---

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

@app.get(f"{API_PREFIX}/health", summary="Verifica che la API locale sia attiva", tags=["autenticazione"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    f"{API_PREFIX}/auth/token",
    summary="Genera un bearer token per il frontend",
    description=(
        "Accetta credenziali come **Basic Auth** nell'header `Authorization` "
        "oppure come body JSON `{username, password}`. "
        "Restituisce un bearer token da usare nell'header `Authorization: Bearer <token>`."
    ),
    tags=["autenticazione"],
    response_description="Token di accesso con durata in secondi",
)
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
# DIRECT PLC — endpoint unificati (Step7 + OPC-UA)
# ----------------------------------------------------------------

@app.get(
    f"{API_PREFIX}/direct/machines",
    summary="Lista tutte le macchine configurate (Step7 + OPC-UA)",
    description="Restituisce l'elenco di tutte le macchine registrate localmente, sia Step7 (snap7) che OPC-UA (Buhler), con protocollo e parametri di connessione.",
    tags=["macchine dirette"],
    response_model=list[UnifiedMachineInfo],
)
async def list_direct_machines(
    _: dict[str, Any] = Depends(require_local_token),
    unified: UnifiedRegistry = Depends(get_unified),
) -> list[UnifiedMachineInfo]:
    return unified.list_machines()


@app.get(
    f"{API_PREFIX}/direct/machines/{{machine_id}}/state",
    summary="Legge lo stato corrente dal PLC",
    description=(
        "Interroga il PLC direttamente:\n\n"
        "- **Step7**: legge il blocco dati DB10 (layout Optotech Scandicci) e restituisce un `MachineState`.\n"
        "- **OPC-UA (Buhler)**: legge i nodi `CMD`, `ACT` e `STA` dal server BMMC e restituisce un `BuhlerState`."
    ),
    tags=["macchine dirette"],
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
    summary="Scrive commessa/ricetta sul PLC",
    description=(
        "Invia la commessa (part program) direttamente al PLC e salva l'assegnazione nel database locale.\n\n"
        "- **Step7**: scrive la stringa in DB10 a offset 0 (max 50 caratteri ASCII).\n"
        "- **OPC-UA (Buhler)**: scrive il nodo `CMD.Process_NextRecipeName` e verifica la conferma dal nodo `ACT.recipe_name`.\n\n"
        "Gli `production_order_ids` vengono **accumulati**: chiamate successive con lo stesso `order_part_program` "
        "aggiungono nuovi ID alla lista esistente senza sovrascrivere quelli precedenti. "
        "La risposta include sempre la lista completa degli ID accumulati fino a quel momento."
    ),
    tags=["macchine dirette"],
    response_description="Esito dell'operazione con machine_id, commessa scritta e lista completa degli ID ordini accumulati",
)
async def write_direct_machine_order(
    machine_id: str,
    body: WriteOrderRequest,
    _: dict[str, Any] = Depends(require_local_token),
    unified: UnifiedRegistry = Depends(get_unified),
    db: aiomysql.Pool = Depends(get_db),
) -> dict[str, Any]:
    try:
        result = await unified.write_order(machine_id, body.order_part_program)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Macchina '{machine_id}' non trovata")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Errore comunicazione PLC: {exc}",
        ) from exc

    accumulated_ids = await save_assignment(db, machine_id, body.order_part_program, body.production_order_ids)
    result["production_order_ids"] = accumulated_ids
    return result
