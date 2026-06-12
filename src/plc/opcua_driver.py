import asyncio
import re
from typing import Any

from asyncua import Client, ua

from .buhler_models import BuhlerState, TimestampField

# Timestamp Buhler: "NomeStepRicetta2026-06-03-09:47:16.568"
# Cattura tutto prima dei 4 digit dell'anno come label, poi YYYY-MM-DD-HH:MM:SS.mmm
_TS_RE = re.compile(
    r'^(.*?)(\d{4}-\d{2}-\d{2}-\d{2}:\d{2}:\d{2}(?:\.\d+)?)$'
)

_NODES: dict[str, str] = {
    "next_recipe_name":           "ns=4;s=BMMC.CMD.Process_NextRecipeName",
    "send_next_recipe":           "ns=4;s=BMMC.CMD.Process_SendNextRecipe",
    "recipe_name":                "ns=4;s=BMMC.ACT.Process_RecipeName",
    "step_name":                  "ns=4;s=BMMC.ACT.Process_StepName",
    "step_number":                "ns=4;s=BMMC.ACT.Process_StepNumber",
    "step_time":                  "ns=4;s=BMMC.ACT.Process_StepTime",
    "step_start_timestamp":       "ns=4;s=BMMC.ACT.Process_Step_Start_Timestamp",
    "step_end_timestamp":         "ns=4;s=BMMC.ACT.Process_Step_End_Timestamp",
    "start_timestamp":            "ns=4;s=BMMC.ACT.Process_Start_Timestamp",
    "end_timestamp":              "ns=4;s=BMMC.ACT.Process_End_Timestamp",
    "mode_idle":                  "ns=4;s=BMMC.STA.Mode_Idle",
    "process_start":              "ns=4;s=BMMC.STA.Process_Start",
    "process_stop":               "ns=4;s=BMMC.STA.Process_Stop",
    "process_end":                "ns=4;s=BMMC.STA.Process_End",
    "water_temp_inlet_coldwater": "ns=4;s=BMMC.ACT.A2.Water_TemperatureInletColdwater",
}

_WRITE_NODE_ID = "ns=4;s=BMMC.CMD.Process_NextRecipeName"
_REQUIRED_READ_NODES = {"next_recipe_name", "recipe_name", "step_name", "step_number", "process_start"}


class OpcUaReadError(RuntimeError):
    """Errore di lettura OPC-UA: almeno un nodo critico non e' affidabile."""


class OpcUaDriver:
    """
    Driver asincrono per PLC Buhler via OPC-UA (asyncua).

    La connessione viene aperta al primo uso e ripristinata automaticamente
    dopo ogni errore di comunicazione.
    """

    def __init__(self, machine_id: str, url: str, timeout: float = 10.0) -> None:
        self.machine_id = machine_id
        self.url = url
        self.timeout = timeout
        self._client: Client | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def read_state(self) -> BuhlerState:
        async with self._lock:
            try:
                await self._ensure_connected()
                values: dict[str, Any] = {}
                read_errors: dict[str, str] = {}
                for key, node_id in _NODES.items():
                    node = self._client.get_node(node_id)
                    try:
                        values[key] = await node.read_value()
                    except Exception as exc:
                        values[key] = None
                        read_errors[key] = f"{type(exc).__name__}: {exc}"
                bad_required = [
                    key
                    for key in _REQUIRED_READ_NODES
                    if key in read_errors or values.get(key) is None
                ]
                if bad_required:
                    details = ", ".join(
                        f"{key}={read_errors.get(key, 'None')}" for key in sorted(bad_required)
                    )
                    raise OpcUaReadError(
                        f"Lettura OPC-UA incompleta su nodi critici: {details}"
                    )
                return _build_state(self.machine_id, values)
            except Exception:
                await self._reset_client()
                raise

    async def write_recipe(self, recipe: str) -> str:
        """
        Scrive Process_NextRecipeName e restituisce il valore riletto per
        confermare la scrittura avvenuta con successo.
        """
        async with self._lock:
            try:
                await self._ensure_connected()
                node = self._client.get_node(_WRITE_NODE_ID)
                dv = ua.DataValue(ua.Variant(recipe, ua.VariantType.String))
                await node.write_value(dv)
                confirmed: str = await node.read_value()
                return str(confirmed)
            except Exception:
                await self._reset_client()
                raise

    async def disconnect(self) -> None:
        async with self._lock:
            await self._reset_client()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _ensure_connected(self) -> None:
        if self._client is None:
            client = Client(url=self.url, timeout=self.timeout)
            await asyncio.wait_for(client.connect(), timeout=self.timeout)
            self._client = client

    async def _reset_client(self) -> None:
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                pass
            self._client = None


# ------------------------------------------------------------------
# State builder
# ------------------------------------------------------------------

def _build_state(machine_id: str, v: dict[str, Any]) -> BuhlerState:
    process_start_raw = v.get("process_start")
    process_start = bool(process_start_raw) if process_start_raw is not None else False

    return BuhlerState(
        machine_id=machine_id,
        next_recipe_name=str(v.get("next_recipe_name") or ""),
        send_next_recipe=_to_bool(v.get("send_next_recipe")),
        recipe_name=str(v.get("recipe_name") or ""),
        step_name=str(v.get("step_name") or ""),
        step_number=v.get("step_number"),
        step_time=v.get("step_time"),
        step_start_timestamp=_parse_timestamp(v.get("step_start_timestamp")),
        step_end_timestamp=_parse_timestamp(v.get("step_end_timestamp")),
        start_timestamp=_parse_timestamp(v.get("start_timestamp")),
        end_timestamp=_parse_timestamp(v.get("end_timestamp")),
        mode_idle=_to_bool(v.get("mode_idle")),
        process_start=process_start,
        process_stop=_to_bool(v.get("process_stop")),
        process_end=_to_bool(v.get("process_end")),
        water_temp_inlet_coldwater=_to_float(v.get("water_temp_inlet_coldwater")),
        is_running=process_start,
    )


def _parse_timestamp(v: Any) -> TimestampField | None:
    if v is None:
        return None
    raw = str(v).strip()
    if not raw:
        return None
    m = _TS_RE.match(raw)
    if not m:
        return TimestampField(label=raw or None, timestamp=None, raw=raw)
    label = m.group(1).strip() or None
    # Normalizza YYYY-MM-DD-HH:MM:SS.mmm → YYYY-MM-DDTHH:MM:SS.mmm
    ts_iso = re.sub(r'^(\d{4}-\d{2}-\d{2})-(\d{2}:\d{2}:\d{2})', r'\1T\2', m.group(2))
    return TimestampField(label=label, timestamp=ts_iso, raw=raw)


def _to_bool(v: Any) -> bool | None:
    if v is None:
        return None
    return bool(v)


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
