import asyncio
import smtplib
import ssl
import uuid
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import TYPE_CHECKING

_ERROR_NOTIFY_COOLDOWN = 1800  # secondi tra notifiche ripetute durante outage prolungato


def _fmt_sec(sec: float) -> str:
    sec = int(sec)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _format_timing(timing_data: dict | None) -> str:
    if not timing_data:
        return ""
    return (
        f"\n1ª fase (50%): {_fmt_sec(timing_data['t_first_phase_seconds'])}"
        f" | lavorazione (100%): {_fmt_sec(timing_data['t_working_seconds'])}"
        f" | venting (30%): {_fmt_sec(timing_data['t_venting_seconds'])}"
    )

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

    Il cambio fase è considerato reale solo quando step_number E step_name
    cambiano contemporaneamente nello stesso tick di polling.
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
        plc_timeout: float = 10.0,
    ) -> None:
        self._registry = registry
        self._poll_interval = poll_interval
        self._webhook_url = webhook_url
        self._log_file = log_file
        self._db = db
        self._slack_webhook_url = slack_webhook_url
        self._email_settings = email_settings
        self._prev_step_name: dict[str, str | None] = {}
        self._prev_step_number: dict[str, object] = {}
        self._prev_running: dict[str, bool] = {}
        self._job_start_time: dict[str, datetime] = {}
        self._first_phase_end_time: dict[str, datetime] = {}
        self._venting_start_time: dict[str, datetime] = {}
        self._plc_timeout = plc_timeout
        self._task: asyncio.Task | None = None
        self._polling_error: dict[str, bool] = {}
        self._error_since: dict[str, datetime] = {}
        self._last_error_notified: dict[str, datetime] = {}
        self._pre_error_running: dict[str, bool] = {}
        self._pending_recipe_reset: dict[str, bool] = {}

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
                mid = info.machine_id
                try:
                    driver = self._registry.get(mid)
                    state: BuhlerState = await asyncio.wait_for(
                        driver.read_state(),
                        timeout=self._plc_timeout + 2.0,
                    )
                    if self._polling_error.get(mid):
                        await self._on_recovery(mid, state)
                    await self._check(state)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    await self._on_poll_error(mid, exc)
            await asyncio.sleep(self._poll_interval)

    async def _on_poll_error(self, mid: str, exc: Exception) -> None:
        now = datetime.now(timezone.utc)
        self._append_log(f"POLL_ERROR | machine={mid} | {type(exc).__name__}: {exc}")
        if not self._polling_error.get(mid):
            self._polling_error[mid] = True
            self._error_since[mid] = now
            self._pre_error_running[mid] = self._prev_running.get(mid, False)
            await self._notify_slack(
                f"*POLL_ERROR Buhler* | `{mid}`\n{type(exc).__name__}: {exc}",
                machine_id=mid,
            )
            await self._notify_email(
                subject=f"POLL_ERROR Buhler | {mid}",
                body=(
                    f"Errore di polling su macchina Buhler.\n\n"
                    f"Macchina: {mid}\n{type(exc).__name__}: {exc}\n"
                ),
                machine_id=mid,
            )
            self._last_error_notified[mid] = now
        else:
            last = self._last_error_notified.get(mid)
            if last is None or (now - last).total_seconds() >= _ERROR_NOTIFY_COOLDOWN:
                elapsed = _fmt_sec((now - self._error_since[mid]).total_seconds())
                await self._notify_slack(
                    f"*POLL_ERROR Buhler (in corso da {elapsed})* | `{mid}`\n"
                    f"{type(exc).__name__}: {exc}",
                    machine_id=mid,
                )
                await self._notify_email(
                    subject=f"POLL_ERROR Buhler | {mid}",
                    body=(
                        f"Errore di polling su macchina Buhler (in corso da {elapsed}).\n\n"
                        f"Macchina: {mid}\n{type(exc).__name__}: {exc}\n"
                    ),
                    machine_id=mid,
                )
                self._last_error_notified[mid] = now

    async def _on_recovery(self, mid: str, state: BuhlerState) -> None:
        error_since = self._error_since.get(mid)
        elapsed = _fmt_sec((datetime.now(timezone.utc) - error_since).total_seconds()) if error_since else "?"
        was_running = self._pre_error_running.get(mid, False)

        self._append_log(f"POLL_OK | machine={mid} | ripristinato dopo {elapsed}")

        base_msg = f"*PLC Buhler tornato online* | `{mid}` | assente da {elapsed}"
        base_body = f"Il PLC Buhler è tornato raggiungibile.\n\nMacchina: {mid}\nAssenza: {elapsed}\n"

        if was_running and not state.process_start:
            self._append_log(
                f"MISSED_EVENT? | machine={mid} | era in produzione prima del blackout, ora è ferma"
            )
            await self._notify_slack(
                base_msg + "\n*ATTENZIONE*: la macchina era in produzione prima del blackout "
                "ed è ora ferma. Un evento di fine lavoro potrebbe essere andato perso.",
                machine_id=mid,
            )
            await self._notify_email(
                subject=f"PLC tornato online | {mid} | POSSIBILE EVENTO PERSO",
                body=base_body + (
                    "\nATTENZIONE: la macchina era in produzione prima del blackout ed è ora ferma.\n"
                    "Un evento di fine lavoro potrebbe essere andato perso.\n"
                ),
                machine_id=mid,
            )
        else:
            await self._notify_slack(base_msg, machine_id=mid)
            await self._notify_email(
                subject=f"PLC tornato online | {mid}",
                body=base_body,
                machine_id=mid,
            )

        self._polling_error.pop(mid, None)
        self._error_since.pop(mid, None)
        self._last_error_notified.pop(mid, None)
        self._pre_error_running.pop(mid, None)

    async def _check(self, state: BuhlerState) -> None:
        mid = state.machine_id
        now = datetime.now(timezone.utc)

        if self._pending_recipe_reset.get(mid):
            try:
                await asyncio.wait_for(
                    self._registry.get(mid).write_recipe(""),
                    timeout=self._plc_timeout + 2.0,
                )
                self._pending_recipe_reset.pop(mid, None)
                self._append_log(f"PLC_RESET_RETRY_OK | machine={mid} | next_recipe_name cleared")
            except Exception as exc:
                self._append_log(f"PLC_RESET_RETRY_ERR | machine={mid} | {type(exc).__name__}: {exc}")

        prev_step_name = self._prev_step_name.get(mid)
        prev_step_number = self._prev_step_number.get(mid)
        prev_running = self._prev_running.get(mid, False)

        step_name_changed = state.step_name != prev_step_name
        step_number_changed = state.step_number != prev_step_number

        # Avvio macchina (o prima osservazione come attiva)
        if state.process_start and not prev_running:
            self._job_start_time[mid] = now
            self._first_phase_end_time.pop(mid, None)
            self._venting_start_time.pop(mid, None)
            await self._notify_slack(
                f"*Inizio lavorazione Buhler* | `{mid}`\n"
                f"Ricetta: `{state.next_recipe_name}`",
                machine_id=mid,
            )

        # Cambio fase reale: step_number E step_name cambiano contemporaneamente
        if step_name_changed and step_number_changed:
            self._append_log(
                f"PHASE_CHANGE | machine={mid} | step_num={state.step_number} | step={state.step_name!r}"
                f" | process_start={state.process_start}"
            )
            # Traccia fine prima fase e inizio venting (solo dopo aver visto almeno uno step precedente)
            if prev_step_name is not None:
                if mid not in self._first_phase_end_time:
                    self._first_phase_end_time[mid] = now
                if state.step_name == "Venting" and mid not in self._venting_start_time:
                    self._venting_start_time[mid] = now

        # Fine lavoro: da fase != Venting con process_start=True → Venting (step_name E step_number cambiano) con process_start=False
        if (
            prev_step_name is not None
            and prev_step_name != "Venting"
            and prev_running
            and state.step_name == "Venting"
            and step_number_changed
            and not state.process_start
        ):
            await self._fire(state, prev_step_name)

        self._prev_step_name[mid] = state.step_name
        self._prev_step_number[mid] = state.step_number
        self._prev_running[mid] = state.process_start

    async def _fire(self, state: BuhlerState, last_work_step: str) -> None:
        fire_dt = datetime.now(timezone.utc)
        ts = fire_dt.isoformat()
        mid = state.machine_id

        # Calcolo tempi di lavorazione
        job_start = self._job_start_time.get(mid)
        first_phase_end = self._first_phase_end_time.get(mid)
        venting_start = self._venting_start_time.get(mid)
        timing_data: dict | None = None
        if job_start is not None:
            t_first = (first_phase_end or fire_dt) - job_start
            t_working = (venting_start or fire_dt) - (first_phase_end or job_start)
            t_venting = fire_dt - (venting_start or fire_dt)
            timing_data = {
                "t_first_phase_seconds": max(0.0, t_first.total_seconds()),
                "t_working_seconds": max(0.0, t_working.total_seconds()),
                "t_venting_seconds": max(0.0, t_venting.total_seconds()),
            }
        # Reset timer per il prossimo ciclo
        self._job_start_time.pop(mid, None)
        self._first_phase_end_time.pop(mid, None)
        self._venting_start_time.pop(mid, None)

        production_order_ids: list[str] = []
        if self._db is not None:
            try:
                from src.db import get_production_orders, set_ended_at
                production_order_ids = await get_production_orders(
                    self._db, state.machine_id, state.next_recipe_name
                )
                await set_ended_at(self._db, state.machine_id, state.next_recipe_name, timing_data)
            except Exception as exc:
                self._append_log(
                    f"DB_ERR | machine={state.machine_id} | {type(exc).__name__}: {exc}"
                )
        payload = {
            "event_id": f"buhler:{state.machine_id}:{uuid.uuid4().hex}",
            "event": "buhler_end_of_work",
            "timestamp": ts,
            "machine_id": state.machine_id,
            "order_part_program": state.next_recipe_name,
            "production_order_ids": production_order_ids,
            "timing_data": timing_data,
        }

        self._append_log(
            f"END_OF_WORK | machine={state.machine_id} | last_step={last_work_step!r}"
            f" | recipe={state.recipe_name!r}"
        )

        # Reset Process_NextRecipeName sul PLC (BMMC.CMD.Process_NextRecipeName)
        try:
            await asyncio.wait_for(
                self._registry.get(mid).write_recipe(""),
                timeout=self._plc_timeout + 2.0,
            )
            self._append_log(f"PLC_RESET_OK | machine={mid} | next_recipe_name cleared")
        except Exception as exc:
            self._append_log(f"PLC_RESET_ERR | machine={mid} | {type(exc).__name__}: {exc}")
            self._pending_recipe_reset[mid] = True

        if self._webhook_url:
            await self._deliver_webhook(payload, state.machine_id)

        orders_str = ", ".join(production_order_ids) if production_order_ids else "—"
        timing_str = _format_timing(timing_data)
        await self._notify_slack(
            f"*Fine lavoro Buhler* | `{state.machine_id}`\n"
            f"Programma: `{state.next_recipe_name}`\n"
            f"Ordini: {orders_str}{timing_str}",
            machine_id=state.machine_id,
        )
        email_timing = (
            f"\nTempi di lavorazione:\n"
            f"  1ª fase (50%):       {_fmt_sec(timing_data['t_first_phase_seconds'])}\n"
            f"  Lavorazione (100%): {_fmt_sec(timing_data['t_working_seconds'])}\n"
            f"  Venting (30%):      {_fmt_sec(timing_data['t_venting_seconds'])}\n"
            if timing_data else ""
        )
        await self._notify_email(
            subject=f"Fine lavoro Buhler | {state.machine_id} | {state.next_recipe_name}",
            body=(
                f"Fine lavoro rilevata su macchina Buhler.\n\n"
                f"Macchina: {state.machine_id}\n"
                f"Programma: {state.next_recipe_name}\n"
                f"Ordini di produzione: {orders_str}\n"
                f"{email_timing}"
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

    async def _deliver_webhook(self, payload: dict, machine_id: str) -> None:
        if not self._webhook_url:
            return
        if self._db is None:
            try:
                async with httpx.AsyncClient(timeout=10.0) as http:
                    response = await http.post(self._webhook_url, json=payload)
                if response.status_code < 500:
                    self._append_log(f"WEBHOOK_OK | machine={machine_id} | http={response.status_code}")
                else:
                    self._append_log(f"WEBHOOK_ERR | machine={machine_id} | http={response.status_code}")
            except Exception as exc:
                self._append_log(f"WEBHOOK_ERR | machine={machine_id} | {type(exc).__name__}: {exc}")
            return

        try:
            from src.db import save_pending_event
            from src.plc.webhook_retry import send_pending_webhook

            pending_id = await save_pending_event(
                self._db,
                self._webhook_url,
                payload,
                event_id=payload["event_id"],
            )
            try:
                delivered, status_code = await send_pending_webhook(
                    self._db,
                    pending_id,
                    self._webhook_url,
                    payload,
                )
                if delivered:
                    self._append_log(f"WEBHOOK_OK | machine={machine_id} | id={pending_id} | http={status_code}")
                else:
                    self._append_log(f"WEBHOOK_PENDING | machine={machine_id} | id={pending_id} | http={status_code}")
            except Exception as exc:
                self._append_log(f"WEBHOOK_PENDING | machine={machine_id} | id={pending_id} | {type(exc).__name__}: {exc}")
        except Exception as db_exc:
            self._append_log(f"WEBHOOK_OUTBOX_ERR | machine={machine_id} | {type(db_exc).__name__}: {db_exc}")
            try:
                async with httpx.AsyncClient(timeout=10.0) as http:
                    response = await http.post(self._webhook_url, json=payload)
                self._append_log(
                    f"WEBHOOK_DIRECT_AFTER_OUTBOX_ERR | machine={machine_id} | http={response.status_code}"
                )
            except Exception as exc:
                self._append_log(
                    f"WEBHOOK_DIRECT_AFTER_OUTBOX_ERR | machine={machine_id} | {type(exc).__name__}: {exc}"
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
