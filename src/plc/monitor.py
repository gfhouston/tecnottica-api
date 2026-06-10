import asyncio
import os
import re
import smtplib
import ssl
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import TYPE_CHECKING

import httpx

from src.settings import EmailSettings
from .models import MachineState, MachineStatus
from .registry import MachineRegistry

if TYPE_CHECKING:
    import aiomysql

_VENTING_RE = re.compile(r"Venting\s*\(\d+\s*of\s*\d+\)", re.IGNORECASE)
_ACTIVE_STATUSES = {MachineStatus.RUNNING, MachineStatus.RUNNING_PAUSED}


class VentingMonitor:
    """
    Polling task che rileva la transizione:
      working_phase != "Venting (N of M)"  +  status in {RUNNING, RUNNING_PAUSED}
      →
      working_phase ~ "Venting (N of M)"  +  status in {RUNNING, RUNNING_PAUSED}

    La fase Venting non è considerata lavorazione; il suo ingresso segna la fine lavoro.
    Alla rilevazione: POST webhook + append su log file.
    """

    def __init__(
        self,
        registry: MachineRegistry,
        poll_interval: float,
        webhook_url: str | None,
        log_file: str,
        db: "aiomysql.Pool | None" = None,
        slack_webhook_url: str | None = None,
        email_settings: EmailSettings | None = None,
    ) -> None:
        self._registry = registry
        self._poll_interval = poll_interval
        self._webhook_url = webhook_url
        self._log_file = log_file
        self._db = db
        self._slack_webhook_url = slack_webhook_url
        self._email_settings = email_settings
        self._was_working_active: dict[str, bool] = {}
        self._last_phase: dict[str, str] = {}
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
                    await self._notify_slack(
                        f"*POLL_ERROR Optotech* | `{info.machine_id}`\n"
                        f"{type(exc).__name__}: {exc}",
                        machine_id=info.machine_id,
                    )
                    await self._notify_email(
                        subject=f"POLL_ERROR Optotech | {info.machine_id}",
                        body=(
                            f"Errore di polling su macchina Optotech.\n\n"
                            f"Macchina: {info.machine_id}\n"
                            f"{type(exc).__name__}: {exc}\n"
                        ),
                        machine_id=info.machine_id,
                    )
            await asyncio.sleep(self._poll_interval)

    async def _check(self, state: MachineState) -> None:
        mid = state.machine_id
        is_venting = bool(_VENTING_RE.search(state.working_phase))
        is_active = state.machine_status in _ACTIVE_STATUSES

        was_working_active = self._was_working_active.get(mid, False)

        if state.working_phase != self._last_phase.get(mid):
            self._append_log(
                f"PHASE_CHANGE | machine={mid} | phase={state.working_phase!r}"
                f" | status={state.machine_status.value}"
            )
            self._last_phase[mid] = state.working_phase

        # Fine lavoro: transizione da fase non-Venting (attiva) → fase Venting (attiva)
        if was_working_active and is_venting and is_active:
            await self._fire(state)

        self._was_working_active[mid] = not is_venting and is_active

    async def _fire(self, state: MachineState) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        production_order_ids: list[str] = []
        if self._db is not None:
            try:
                from src.db import get_production_orders, set_ended_at
                production_order_ids = await get_production_orders(
                    self._db, state.machine_id, state.order_part_program
                )
                await set_ended_at(self._db, state.machine_id, state.order_part_program)
            except Exception as exc:
                self._append_log(
                    f"DB_ERR | machine={state.machine_id} | {type(exc).__name__}: {exc}"
                )
        payload = {
            "event": "optotech_end_of_work",
            "timestamp": ts,
            "machine_id": state.machine_id,
            "order_part_program": state.order_part_program,
            "production_order_ids": production_order_ids,
        }

        self._append_log(
            f"END_OF_WORK | machine={state.machine_id} | phase={state.working_phase!r} "
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

        orders_str = ", ".join(production_order_ids) if production_order_ids else "—"
        await self._notify_slack(
            f"*Fine lavoro Optotech* | `{state.machine_id}`\n"
            f"Programma: `{state.order_part_program}`\n"
            f"Ordini: {orders_str}",
            machine_id=state.machine_id,
        )
        await self._notify_email(
            subject=f"Fine lavoro Optotech | {state.machine_id} | {state.order_part_program}",
            body=(
                f"Fine lavoro rilevata su macchina Optotech.\n\n"
                f"Macchina: {state.machine_id}\n"
                f"Programma: {state.order_part_program}\n"
                f"Ordini di produzione: {orders_str}\n"
            ),
            machine_id=state.machine_id,
        )

    async def _notify_slack(self, text: str, machine_id: str = "") -> None:
        if not self._slack_webhook_url:
            return
        try:
            async with httpx.AsyncClient(timeout=10.0) as http:
                await http.post(self._slack_webhook_url, json={"text": text})
        except Exception as exc:
            self._append_log(
                f"SLACK_ERR | machine={machine_id} | {type(exc).__name__}: {exc}"
            )

    async def _notify_email(self, subject: str, body: str, machine_id: str = "") -> None:
        if not self._email_settings:
            return
        cfg = self._email_settings

        def _send() -> None:
            msg = MIMEText(body, "plain", "utf-8")
            msg["Subject"] = subject
            msg["From"] = f"{cfg.from_name} <{cfg.smtp_user}>"
            msg["To"] = cfg.to_address
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port, context=context) as server:
                server.login(cfg.smtp_user, cfg.smtp_password)
                server.sendmail(cfg.smtp_user, cfg.to_address, msg.as_string())

        try:
            await asyncio.get_event_loop().run_in_executor(None, _send)
        except Exception as exc:
            self._append_log(
                f"EMAIL_ERR | machine={machine_id} | {type(exc).__name__}: {exc}"
            )

    def _append_log(self, message: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        line = f"{ts} | {message}\n"
        try:
            with open(self._log_file, "a") as f:
                f.write(line)
        except Exception:
            pass
