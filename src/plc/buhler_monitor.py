import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import httpx

from .buhler_models import BuhlerState
from .buhler_registry import BuhlerRegistry

if TYPE_CHECKING:
    import aiomysql


class BuhlerVentingMonitor:
    """
    Polling task che rileva la transizione su macchine Buhler OPC-UA:
      Process_StepName == "Vent"  +  Process_Start == 1 (in esecuzione)
      →
      Process_StepName == "Vent"  +  Process_Start == 0 (arresto)

    Alla rilevazione: POST webhook + append su log file.
    """

    def __init__(
        self,
        registry: BuhlerRegistry,
        poll_interval: float,
        webhook_url: str | None,
        log_file: str,
        db: "aiomysql.Pool | None" = None,
    ) -> None:
        self._registry = registry
        self._poll_interval = poll_interval
        self._webhook_url = webhook_url
        self._log_file = log_file
        self._db = db
        self._was_vent_running: dict[str, bool] = {}
        self._last_step: dict[str, str] = {}
        self._last_vent_step: dict[str, str] = {}
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="buhler_venting_monitor")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------------

    async def _run(self) -> None:
        while True:
            for info in self._registry.list_machines():
                try:
                    driver = self._registry.get(info.machine_id)
                    state: BuhlerState = await driver.read_state()
                    await self._check(state)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._append_log(
                        f"POLL_ERROR | machine={info.machine_id} | {type(exc).__name__}: {exc}"
                    )
            await asyncio.sleep(self._poll_interval)

    async def _check(self, state: BuhlerState) -> None:
        mid = state.machine_id
        is_vent = state.step_name == "Vent"
        is_running = state.process_start

        was_vent_running = self._was_vent_running.get(mid, False)

        if state.step_name != self._last_step.get(mid):
            self._append_log(
                f"PHASE_CHANGE | machine={mid} | step={state.step_name!r}"
                f" | process_start={state.process_start}"
            )
            self._last_step[mid] = state.step_name

        if is_vent and is_running:
            self._last_vent_step[mid] = state.step_name

        if was_vent_running and not is_running:
            await self._fire(state, self._last_vent_step.get(mid, state.step_name))

        self._was_vent_running[mid] = is_vent and is_running

    async def _fire(self, state: BuhlerState, vent_step: str) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        production_order_ids: list[str] = []
        if self._db is not None:
            try:
                from src.db import get_production_orders
                production_order_ids = await get_production_orders(
                    self._db, state.machine_id, state.recipe_name
                )
            except Exception as exc:
                self._append_log(
                    f"DB_ERR | machine={state.machine_id} | {type(exc).__name__}: {exc}"
                )
        payload = {
            "event": "buhler_venting_stopped",
            "timestamp": ts,
            "machine_id": state.machine_id,
            "step_name": vent_step,
            "recipe_name": state.recipe_name,
            "next_recipe_name": state.next_recipe_name,
            "process_start": state.process_start,
            "production_order_ids": production_order_ids,
        }

        self._append_log(
            f"EVENT | machine={state.machine_id} | step={state.step_name!r} "
            f"| process_start={state.process_start} | recipe={state.recipe_name!r}"
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
        ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        line = f"{ts} | {message}\n"
        try:
            with open(self._log_file, "a") as f:
                f.write(line)
        except Exception:
            pass
