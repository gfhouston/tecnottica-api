import asyncio
import smtplib
import ssl
import uuid
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import TYPE_CHECKING, Any

import httpx
from asyncua import Client

from src.settings import EmailSettings
from .buhler_config import BUHLER_MACHINES
from .buhler_models import BuhlerState
from .buhler_registry import BuhlerRegistry

if TYPE_CHECKING:
    import aiomysql

_ERROR_NOTIFY_COOLDOWN = 1800

_SUBSCRIBE_NODES: dict[str, str] = {
    "next_recipe_name": "ns=4;s=BMMC.CMD.Process_NextRecipeName",
    "recipe_name":      "ns=4;s=BMMC.ACT.Process_RecipeName",
    "step_name":        "ns=4;s=BMMC.ACT.Process_StepName",
    "step_number":      "ns=4;s=BMMC.ACT.Process_StepNumber",
    "process_start":    "ns=4;s=BMMC.STA.Process_Start",
}
_NODE_ID_TO_KEY: dict[str, str] = {v: k for k, v in _SUBSCRIBE_NODES.items()}

_PUBLISH_INTERVAL_MS = 500
_KEEPALIVE_TIMEOUT = 90.0   # secondi senza dati prima di verificare la connessione
_RECONNECT_DELAY = 10.0     # secondi tra un tentativo di riconnessione e l'altro


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


class _DataChangeHandler:
    """Riceve le notifiche OPC-UA e le immette in una coda asyncio."""

    def __init__(self, queue: asyncio.Queue) -> None:
        self._queue = queue

    def datachange_notification(self, node, val, data) -> None:
        node_id_str = node.nodeid.to_string()
        key = _NODE_ID_TO_KEY.get(node_id_str)
        if key is not None:
            self._queue.put_nowait(("data", key, val))

    def status_change_notification(self, status) -> None:
        self._queue.put_nowait(("status", status, None))


class BuhlerSubscriptionMonitor:
    """
    Monitor Buhler basato su OPC-UA Subscription.

    Per ogni macchina mantiene una connessione persistente e una
    Subscription al server OPC-UA. Invece di un polling periodico,
    riceve notifiche push ogni volta che un tag cambia valore.

    La logica di rilevamento fine-lavoro è identica a BuhlerVentingMonitor:
      fase != "Venting" + Process_Start == True
      →
      fase == "Venting" + Process_Start == False  (step_number cambia)
    """

    def __init__(
        self,
        registry: BuhlerRegistry,
        webhook_url: str | None,
        log_file: str,
        db: "aiomysql.Pool | None" = None,
        slack_webhook_url: str | None = None,
        email_settings: EmailSettings | None = None,
        plc_timeout: float = 10.0,
    ) -> None:
        self._registry = registry
        self._webhook_url = webhook_url
        self._log_file = log_file
        self._db = db
        self._slack_webhook_url = slack_webhook_url
        self._email_settings = email_settings
        self._plc_timeout = plc_timeout

        # Cache valori correnti: {machine_id: {key: value}}
        self._cache: dict[str, dict[str, Any]] = {}

        # Stato per la logica di rilevamento (per macchina)
        self._prev_step_name: dict[str, str | None] = {}
        self._prev_step_number: dict[str, object] = {}
        self._prev_running: dict[str, bool] = {}
        self._job_start_time: dict[str, datetime] = {}
        self._first_phase_end_time: dict[str, datetime] = {}
        self._venting_start_time: dict[str, datetime] = {}

        # Con subscription ogni nodo notifica separatamente:
        # "step_name→Venting" e "process_start→False" arrivano in tick distinti.
        # _in_venting mantiene lo stato tra i due tick.
        self._in_venting: dict[str, bool] = {}       # abbiamo visto step_name→"Venting"
        self._was_running: dict[str, bool] = {}      # process_start è diventato True nel ciclo corrente
        self._pending_last_step: dict[str, str] = {} # step precedente a Venting (per il log)

        # Stato errore connessione (per macchina)
        self._conn_error: dict[str, bool] = {}
        self._error_since: dict[str, datetime] = {}
        self._last_error_notified: dict[str, datetime] = {}
        self._pre_error_running: dict[str, bool] = {}

        self._tasks: dict[str, asyncio.Task] = {}

    # ------------------------------------------------------------------
    # Ciclo di vita
    # ------------------------------------------------------------------

    def start(self) -> None:
        for info in self._registry.list_machines():
            cfg = BUHLER_MACHINES[info.machine_id]
            task = asyncio.create_task(
                self._run_machine(cfg.machine_id, cfg.url),
                name=f"buhler_sub_{cfg.machine_id}",
            )
            self._tasks[cfg.machine_id] = task

    async def stop(self) -> None:
        for task in self._tasks.values():
            task.cancel()
        results = await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        for r in results:
            if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
                pass  # eccezioni già loggato nel task
        self._tasks.clear()

    # ------------------------------------------------------------------
    # Loop per singola macchina (con riconnessione automatica)
    # ------------------------------------------------------------------

    async def _run_machine(self, mid: str, url: str) -> None:
        while True:
            try:
                await self._run_session(mid, url)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._on_conn_error(mid, exc)
                await asyncio.sleep(_RECONNECT_DELAY)

    async def _run_session(self, mid: str, url: str) -> None:
        """Singola sessione OPC-UA: connessione + subscription + loop eventi."""
        queue: asyncio.Queue = asyncio.Queue()
        handler = _DataChangeHandler(queue)

        async with Client(url=url, timeout=self._plc_timeout) as client:
            # Ripristino: svuota cache per ricevere valori iniziali freschi
            self._cache.pop(mid, None)

            subscription = await client.create_subscription(_PUBLISH_INTERVAL_MS, handler)
            nodes = [client.get_node(node_id) for node_id in _SUBSCRIBE_NODES.values()]
            await subscription.subscribe_data_change(nodes)

            if self._conn_error.get(mid):
                await self._on_conn_restored(mid)

            self._append_log(f"SUB_CONNECTED | machine={mid} | url={url}")

            while True:
                try:
                    kind, a, b = await asyncio.wait_for(
                        queue.get(), timeout=_KEEPALIVE_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    # Nessun dato per troppo tempo: verifica connessione
                    await asyncio.wait_for(
                        client.get_node("i=2258").read_value(),
                        timeout=5.0,
                    )
                    continue

                if kind == "status":
                    status = a
                    # asyncua può passare un oggetto StatusChangeNotification
                    code = getattr(status, "Status", status)
                    is_good = getattr(code, "is_good", lambda: True)()
                    if not is_good:
                        raise ConnectionError(f"Subscription status: {code}")
                    continue

                # kind == "data"
                key, val = a, b
                self._cache.setdefault(mid, {})[key] = val
                await self._process_change(mid)

    # ------------------------------------------------------------------
    # Elaborazione di un cambiamento di valore
    # ------------------------------------------------------------------

    async def _process_change(self, mid: str) -> None:
        c = self._cache.get(mid, {})
        required = {"step_name", "step_number", "process_start", "next_recipe_name", "recipe_name"}
        if not required.issubset(c.keys()):
            return  # cache ancora incompleta (valori iniziali non ancora tutti arrivati)

        state = BuhlerState(
            machine_id=mid,
            next_recipe_name=str(c.get("next_recipe_name") or ""),
            send_next_recipe=None,
            recipe_name=str(c.get("recipe_name") or ""),
            step_name=str(c.get("step_name") or ""),
            step_number=c.get("step_number"),
            step_time=None,
            step_start_timestamp=None,
            step_end_timestamp=None,
            start_timestamp=None,
            end_timestamp=None,
            mode_idle=None,
            process_start=bool(c.get("process_start")),
            process_stop=None,
            process_end=None,
            water_temp_inlet_coldwater=None,
            is_running=bool(c.get("process_start")),
        )
        await self._check(state)

    # ------------------------------------------------------------------
    # Logica di rilevamento (identica a BuhlerVentingMonitor)
    # ------------------------------------------------------------------

    async def _check(self, state: BuhlerState) -> None:
        """
        Con subscription ogni nodo arriva in una notifica separata, quindi
        step_name→"Venting" e process_start→False non arrivano mai nello stesso
        tick. Usiamo _was_running / _in_venting come flag inter-notifica:

          process_start diventa True      → _was_running = True
          step_name diventa "Venting"
            (dopo step non-Venting, con _was_running)  → _in_venting = True
          process_start diventa False
            (con _in_venting)              → fire end-of-work

        L'ordine delle due ultime notifiche è indifferente.
        """
        mid = state.machine_id
        now = datetime.now(timezone.utc)

        prev_step_name = self._prev_step_name.get(mid)
        prev_running = self._prev_running.get(mid, False)

        step_name_changed = state.step_name != prev_step_name

        # ── Nuovo ciclo di produzione ──────────────────────────────────────
        if state.process_start and not prev_running:
            self._job_start_time[mid] = now
            self._first_phase_end_time.pop(mid, None)
            self._venting_start_time.pop(mid, None)
            self._in_venting.pop(mid, None)
            self._pending_last_step.pop(mid, None)

        # Memorizza che la macchina è stata in produzione in questo ciclo
        if state.process_start:
            self._was_running[mid] = True

        # ── Cambio step name ───────────────────────────────────────────────
        # Con subscription non richiediamo step_number_changed simultaneo:
        # ogni notifica è già un cambiamento reale.
        if step_name_changed and prev_step_name is not None:
            self._append_log(
                f"PHASE_CHANGE | machine={mid} | step={state.step_name!r}"
                f" | process_start={state.process_start}"
            )
            if mid not in self._first_phase_end_time:
                self._first_phase_end_time[mid] = now

        # ── Ingresso in Venting ────────────────────────────────────────────
        if (
            step_name_changed
            and prev_step_name is not None
            and prev_step_name != "Venting"
            and state.step_name == "Venting"
            and self._was_running.get(mid)
        ):
            self._in_venting[mid] = True
            self._pending_last_step.setdefault(mid, prev_step_name)
            if mid not in self._venting_start_time:
                self._venting_start_time[mid] = now

        # ── Fine lavoro: Venting + process_start=False ────────────────────
        # Si scatena su whichever delle due notifiche arriva per seconda.
        if (
            self._in_venting.get(mid)
            and self._was_running.get(mid)
            and not state.process_start
        ):
            last_step = self._pending_last_step.pop(mid, prev_step_name or "")
            self._in_venting.pop(mid, None)
            self._was_running.pop(mid, None)
            await self._fire(state, last_step)

        self._prev_step_name[mid] = state.step_name
        self._prev_step_number[mid] = state.step_number
        self._prev_running[mid] = state.process_start

    async def _fire(self, state: BuhlerState, last_work_step: str) -> None:
        fire_dt = datetime.now(timezone.utc)
        ts = fire_dt.isoformat()
        mid = state.machine_id

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

        try:
            await asyncio.wait_for(
                self._registry.get(mid).write_recipe(""),
                timeout=self._plc_timeout + 2.0,
            )
            self._append_log(f"PLC_RESET_OK | machine={mid} | next_recipe_name cleared")
        except Exception as exc:
            self._append_log(f"PLC_RESET_ERR | machine={mid} | {type(exc).__name__}: {exc}")

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

    # ------------------------------------------------------------------
    # Gestione errori di connessione
    # ------------------------------------------------------------------

    async def _on_conn_error(self, mid: str, exc: Exception) -> None:
        now = datetime.now(timezone.utc)
        self._append_log(f"CONN_ERROR | machine={mid} | {type(exc).__name__}: {exc}")
        if not self._conn_error.get(mid):
            self._conn_error[mid] = True
            self._error_since[mid] = now
            self._pre_error_running[mid] = self._prev_running.get(mid, False)
            await self._notify_slack(
                f"*CONN_ERROR Buhler* | `{mid}`\n{type(exc).__name__}: {exc}",
                machine_id=mid,
            )
            await self._notify_email(
                subject=f"CONN_ERROR Buhler | {mid}",
                body=(
                    f"Errore di connessione OPC-UA su macchina Buhler.\n\n"
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
                    f"*CONN_ERROR Buhler (in corso da {elapsed})* | `{mid}`\n"
                    f"{type(exc).__name__}: {exc}",
                    machine_id=mid,
                )
                await self._notify_email(
                    subject=f"CONN_ERROR Buhler | {mid}",
                    body=(
                        f"Errore di connessione OPC-UA (in corso da {elapsed}).\n\n"
                        f"Macchina: {mid}\n{type(exc).__name__}: {exc}\n"
                    ),
                    machine_id=mid,
                )
                self._last_error_notified[mid] = now

    async def _on_conn_restored(self, mid: str) -> None:
        error_since = self._error_since.get(mid)
        elapsed = (
            _fmt_sec((datetime.now(timezone.utc) - error_since).total_seconds())
            if error_since else "?"
        )
        was_running = self._pre_error_running.get(mid, False)
        current_running = self._prev_running.get(mid, False)

        self._append_log(f"CONN_OK | machine={mid} | ripristinato dopo {elapsed}")

        base_msg = f"*PLC Buhler tornato online* | `{mid}` | assente da {elapsed}"
        base_body = f"Il PLC Buhler è tornato raggiungibile.\n\nMacchina: {mid}\nAssenza: {elapsed}\n"

        if was_running and not current_running:
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

        self._conn_error.pop(mid, None)
        self._error_since.pop(mid, None)
        self._last_error_notified.pop(mid, None)
        self._pre_error_running.pop(mid, None)

    # ------------------------------------------------------------------
    # Notifiche e log
    # ------------------------------------------------------------------

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
                self._db, self._webhook_url, payload, event_id=payload["event_id"]
            )
            try:
                delivered, status_code = await send_pending_webhook(
                    self._db, pending_id, self._webhook_url, payload
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
