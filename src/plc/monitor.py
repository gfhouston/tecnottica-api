import asyncio
import os
import re
from datetime import datetime, timezone

import httpx

from .models import MachineState, MachineStatus
from .registry import MachineRegistry

_VENTING_RE = re.compile(r"Venting\s*\(\d+\s*of\s*\d+\)", re.IGNORECASE)
_ACTIVE_STATUSES = {MachineStatus.RUNNING, MachineStatus.RUNNING_PAUSED}


class VentingMonitor:
    """
    Polling task che rileva la transizione:
      working_phase ~ "Venting (N of M)"  +  status in {RUNNING, RUNNING_PAUSED}
      →
      working_phase ~ "Venting (N of M)"  +  status == STOPPED

    Alla rilevazione: POST webhook + append su log file.
    """

    def __init__(
        self,
        registry: MachineRegistry,
        poll_interval: float,
        webhook_url: str | None,
        log_file: str,
    ) -> None:
        self._registry = registry
        self._poll_interval = poll_interval
        self._webhook_url = webhook_url
        self._log_file = log_file
        self._was_venting_active: dict[str, bool] = {}
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="venting_monitor")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------------

    async def _run(self) -> None:
        loop = asyncio.get_event_loop()
        while True:
            for info in self._registry.list_machines():
                try:
                    driver = self._registry.get(info.machine_id)
                    state: MachineState = await loop.run_in_executor(None, driver.read_state)
                    await self._check(state)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._append_log(
                        f"POLL_ERROR | machine={info.machine_id} | {type(exc).__name__}: {exc}"
                    )
            await asyncio.sleep(self._poll_interval)

    async def _check(self, state: MachineState) -> None:
        mid = state.machine_id
        is_venting = bool(_VENTING_RE.search(state.working_phase))
        is_active = state.machine_status in _ACTIVE_STATUSES

        was_venting_active = self._was_venting_active.get(mid, False)

        if was_venting_active and is_venting and not is_active:
            await self._fire(state)

        self._was_venting_active[mid] = is_venting and is_active

    async def _fire(self, state: MachineState) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        payload = {
            "event": "venting_stopped",
            "timestamp": ts,
            "machine_id": state.machine_id,
            "working_phase": state.working_phase,
            "machine_status": state.machine_status.value,
            "alarm_code": state.alarm_code,
        }

        self._append_log(
            f"EVENT | machine={state.machine_id} | phase={state.working_phase!r} "
            f"| status={state.machine_status.value} | alarm={state.alarm_code}"
        )

        if self._webhook_url:
            try:
                async with httpx.AsyncClient(timeout=10.0) as http:
                    r = await http.post(self._webhook_url, json=payload)
                    self._append_log(
                        f"WEBHOOK_OK | machine={state.machine_id} | http={r.status_code}"
                    )
            except Exception as exc:
                self._append_log(
                    f"WEBHOOK_ERR | machine={state.machine_id} | {type(exc).__name__}: {exc}"
                )

    def _append_log(self, message: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        line = f"{ts} | {message}\n"
        try:
            with open(self._log_file, "a") as f:
                f.write(line)
        except Exception:
            pass
