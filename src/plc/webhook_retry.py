import asyncio
from datetime import datetime
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    import aiomysql


async def send_pending_webhook(
    pool: "aiomysql.Pool",
    event_id: int,
    url: str,
    payload: dict,
) -> tuple[bool, int | None]:
    """Invia un evento gia' salvato in outbox e aggiorna attempts/delivered_at."""
    from src.db import increment_event_attempts, mark_event_delivered

    await increment_event_attempts(pool, event_id)
    async with httpx.AsyncClient(timeout=10.0) as http:
        response = await http.post(url, json=payload)
    if response.status_code < 500:
        await mark_event_delivered(pool, event_id)
        return True, response.status_code
    return False, response.status_code


class WebhookRetryWorker:
    """
    Task in background che ritenta la consegna degli eventi webhook falliti.

    Al primo ciclo (subito dopo start) legge tutti i pending dal DB — questo
    recupera anche gli eventi rimasti in sospeso dopo un riavvio dell'app.
    Poi ripete ogni `retry_interval` secondi.

    Un evento viene marcato come consegnato su qualsiasi risposta HTTP < 500.
    Errori di rete/timeout e risposte 5xx lasciano l'evento in coda.
    """

    def __init__(
        self,
        pool: "aiomysql.Pool",
        retry_interval: float,
        log_file: str,
    ) -> None:
        self._pool = pool
        self._retry_interval = retry_interval
        self._log_file = log_file
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="webhook_retry_worker")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        while True:
            await self._retry_pending()
            await asyncio.sleep(self._retry_interval)

    async def _retry_pending(self) -> None:
        from src.db import get_pending_events

        try:
            events = await get_pending_events(self._pool)
        except Exception as exc:
            self._append_log(f"RETRY_WORKER_DB_ERR | {type(exc).__name__}: {exc}")
            return

        for event in events:
            event_id: int = event["id"]
            url: str = event["url"]
            payload: dict = event["payload"]
            attempts: int = event["attempts"]

            try:
                delivered, status_code = await send_pending_webhook(
                    self._pool,
                    event_id,
                    url,
                    payload,
                )
                if delivered:
                    self._append_log(
                        f"WEBHOOK_RETRY_OK | id={event_id} | http={status_code} | attempts={attempts + 1}"
                    )
                else:
                    self._append_log(
                        f"WEBHOOK_RETRY_FAIL | id={event_id} | http={status_code} | attempts={attempts + 1}"
                    )
            except Exception as exc:
                self._append_log(
                    f"WEBHOOK_RETRY_ERR | id={event_id} | {type(exc).__name__}: {exc} | attempts={attempts + 1}"
                )

    def _append_log(self, message: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        try:
            with open(self._log_file, "a") as f:
                f.write(f"{ts} | {message}\n")
        except Exception:
            pass
