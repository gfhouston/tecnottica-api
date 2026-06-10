import asyncio
import smtplib
import ssl
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import TYPE_CHECKING

import httpx

from src.settings import EmailSettings
from .buhler_models import BuhlerState
from .buhler_registry import BuhlerRegistry

if TYPE_CHECKING:
    import aiomysql


class BuhlerVentingMonitor:
    """
    Polling task che rileva la fine lavoro su macchine Buhler OPC-UA:
      fase != "Venting"  +  Process_Start == True  (lavorazione in corso)
      →
      fase == "Venting"  +  Process_Start == False  (fine lavorazione)

    Alla rilevazione: POST webhook + append su log file.
    """

    def __init__(
        self,
        registry: BuhlerRegistry,
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
        self._last_step: dict[str, str] = {}
        self._prev_step: dict[str, str | None] = {}
        self._prev_running: dict[str, bool] = {}
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
                    await self._notify_slack(
                        f"*POLL_ERROR Buhler* | `{info.machine_id}`\n"
                        f"{type(exc).__name__}: {exc}",
                        machine_id=info.machine_id,
                    )
                    await self._notify_email(
                        subject=f"POLL_ERROR Buhler | {info.machine_id}",
                        body=(
                            f"Errore di polling su macchina Buhler.\n\n"
                            f"Macchina: {info.machine_id}\n"
                            f"{type(exc).__name__}: {exc}\n"
                        ),
                        machine_id=info.machine_id,
                    )
            await asyncio.sleep(self._poll_interval)

    async def _check(self, state: BuhlerState) -> None:
        mid = state.machine_id

        if state.step_name != self._last_step.get(mid):
            self._append_log(
                f"PHASE_CHANGE | machine={mid} | step={state.step_name!r}"
                f" | process_start={state.process_start}"
            )
            self._last_step[mid] = state.step_name

        prev_step = self._prev_step.get(mid)
        prev_running = self._prev_running.get(mid, False)

        # Fine lavoro: da fase != Venting con process_start=True → Venting con process_start=False
        if (
            prev_step is not None
            and prev_step != "Venting"
            and prev_running
            and state.step_name == "Venting"
            and not state.process_start
        ):
            await self._fire(state, prev_step)

        self._prev_step[mid] = state.step_name
        self._prev_running[mid] = state.process_start

    async def _fire(self, state: BuhlerState, last_work_step: str) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        production_order_ids: list[str] = []
        if self._db is not None:
            try:
                from src.db import get_production_orders, set_ended_at
                production_order_ids = await get_production_orders(
                    self._db, state.machine_id, state.next_recipe_name
                )
                await set_ended_at(self._db, state.machine_id, state.next_recipe_name)
            except Exception as exc:
                self._append_log(
                    f"DB_ERR | machine={state.machine_id} | {type(exc).__name__}: {exc}"
                )
        payload = {
            "event": "buhler_end_of_work",
            "timestamp": ts,
            "machine_id": state.machine_id,
            "order_part_program": state.next_recipe_name,
            "production_order_ids": production_order_ids,
        }

        self._append_log(
            f"END_OF_WORK | machine={state.machine_id} | last_step={last_work_step!r}"
            f" | recipe={state.recipe_name!r}"
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
            f"*Fine lavoro Buhler* | `{state.machine_id}`\n"
            f"Programma: `{state.next_recipe_name}`\n"
            f"Ordini: {orders_str}",
            machine_id=state.machine_id,
        )
        await self._notify_email(
            subject=f"Fine lavoro Buhler | {state.machine_id} | {state.next_recipe_name}",
            body=(
                f"Fine lavoro rilevata su macchina Buhler.\n\n"
                f"Macchina: {state.machine_id}\n"
                f"Programma: {state.next_recipe_name}\n"
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
