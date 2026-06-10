"""
Persistenza MySQL asincrona per le associazioni ordine→produzione.

La tabella order_assignments registra, per ogni scrittura sul PLC,
il mapping (machine_id, order_part_program) → lista ID ordini di produzione
che girano contemporaneamente su quella macchina con quel programma.

Gli ID sono accumulati: chiamate successive con lo stesso order_part_program
aggiungono nuovi ID alla lista esistente senza sovrascrivere quelli precedenti.
"""
import json
from datetime import datetime, timezone

import aiomysql

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS order_assignments (
    id                   INT AUTO_INCREMENT PRIMARY KEY,
    machine_id           VARCHAR(128)  NOT NULL,
    order_part_program   VARCHAR(128)  NOT NULL,
    production_order_ids JSON          NOT NULL,
    assigned_at          DATETIME(6)   NOT NULL,
    ended_at             DATETIME(6)   NULL,
    INDEX idx_machine_order (machine_id, order_part_program)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

_ALTER_ADD_ENDED_AT = """
ALTER TABLE order_assignments
ADD COLUMN IF NOT EXISTS ended_at DATETIME(6) NULL
"""


async def init_db(
    host: str,
    user: str,
    password: str,
    database: str,
    port: int = 3306,
) -> aiomysql.Pool:
    pool = await aiomysql.create_pool(
        host=host,
        port=port,
        user=user,
        password=password,
        db=database,
        autocommit=True,
        charset="utf8mb4",
        minsize=1,
        maxsize=5,
    )
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_CREATE_TABLE)
            await cur.execute(_ALTER_ADD_ENDED_AT)
    return pool


async def save_assignment(
    pool: aiomysql.Pool,
    machine_id: str,
    order_part_program: str,
    production_order_ids: list[str],
) -> list[str]:
    """
    Mantiene una sola riga per (machine_id, order_part_program).
    Gli ID nuovi vengono aggiunti in coda a quelli già presenti; i duplicati vengono ignorati.
    Restituisce la lista accumulata completa dopo l'aggiornamento.
    """
    existing = await get_production_orders(pool, machine_id, order_part_program)

    seen = set(existing)
    merged = list(existing)
    for oid in production_order_ids:
        if oid not in seen:
            merged.append(oid)
            seen.add(oid)

    async with pool.acquire() as conn:
        await conn.begin()
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "DELETE FROM order_assignments "
                    "WHERE machine_id = %s AND order_part_program = %s",
                    (machine_id, order_part_program),
                )
                await cur.execute(
                    "INSERT INTO order_assignments "
                    "(machine_id, order_part_program, production_order_ids, assigned_at) "
                    "VALUES (%s, %s, %s, %s)",
                    (
                        machine_id,
                        order_part_program,
                        json.dumps(merged),
                        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
                    ),
                )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    return merged


async def get_production_orders(
    pool: aiomysql.Pool,
    machine_id: str,
    order_part_program: str,
) -> list[str]:
    """
    Restituisce gli ID ordini più recenti associati a (machine_id, order_part_program).
    Torna lista vuota se non esiste nessuna associazione.
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT production_order_ids FROM order_assignments "
                "WHERE machine_id = %s AND order_part_program = %s "
                "ORDER BY assigned_at DESC LIMIT 1",
                (machine_id, order_part_program),
            )
            row = await cur.fetchone()
    if row is None:
        return []
    value = row[0]
    if isinstance(value, str):
        return json.loads(value)
    return list(value) if value else []


async def set_ended_at(
    pool: aiomysql.Pool,
    machine_id: str,
    order_part_program: str,
) -> None:
    """Scrive ended_at sulla riga (machine_id, order_part_program) al momento di fine lavoro."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE order_assignments SET ended_at = %s "
                "WHERE machine_id = %s AND order_part_program = %s",
                (
                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
                    machine_id,
                    order_part_program,
                ),
            )
