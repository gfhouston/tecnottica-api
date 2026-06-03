import struct
import threading

import snap7

from .models import MachineState, MachineStatus

# Bit mask per lo stato macchina nel word CONTROL
_RUNNING_BITS = 0x6000  # bit 14 e 13: in esecuzione
_PAUSED_BIT = 0x8000    # bit 15: in pausa


class Step7Driver:
    """
    Driver diretto per PLC Step7 via protocollo S7comm (snap7).

    La connessione viene aperta al primo uso e riaperta automaticamente
    in caso di disconnessione. Tutte le operazioni sono thread-safe.
    """

    def __init__(self, machine_id: str, ip: str, rack: int, slot: int) -> None:
        self.machine_id = machine_id
        self.ip = ip
        self.rack = rack
        self.slot = slot
        self._client: snap7.Client | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read_state(self) -> MachineState:
        with self._lock:
            try:
                client = self._ensure_connected()
                data = client.db_read(10, 0, 222)
                return _parse_buffer(self.machine_id, data)
            except Exception:
                self._reset_client()
                raise

    def write_order(self, order: str) -> None:
        """Scrive ORDER_PART_PROGRAM in DB10 offset 0 (CHAR ARRAY[50], raw bytes)."""
        if len(order) > 50:
            raise ValueError(f"ORDER_PART_PROGRAM: max 50 chars, ricevuto {len(order)}")
        with self._lock:
            try:
                client = self._ensure_connected()
                buf = bytearray(50)
                encoded = order.encode("ascii")
                buf[: len(encoded)] = encoded
                client.db_write(10, 0, bytes(buf))
            except Exception:
                self._reset_client()
                raise

    def disconnect(self) -> None:
        with self._lock:
            self._reset_client()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_connected(self) -> snap7.Client:
        if self._client is None:
            client = snap7.Client()
            client.set_connection_type(1)  # 1 = PG (programming device)
            client.connect(self.ip, self.rack, self.slot)
            self._client = client
        elif not self._client.get_connected():
            self._client.connect(self.ip, self.rack, self.slot)
        return self._client

    def _reset_client(self) -> None:
        if self._client is not None:
            try:
                self._client.disconnect()
            except Exception:
                pass
            self._client = None


# ------------------------------------------------------------------
# Buffer parsing
# ------------------------------------------------------------------

def _parse_char_array(data: bytes | bytearray, start: int, length: int) -> str:
    """Legge un CHAR ARRAY[n] raw, filtrando i caratteri non stampabili (0x00-0x1F, 0x7F+)."""
    segment = data[start : start + length]
    return "".join(chr(b) for b in segment if 0x20 <= b <= 0x7E).rstrip()


def _parse_buffer(machine_id: str, data: bytes | bytearray) -> MachineState:
    order = _parse_char_array(data, 0, 50)
    working_phase = _parse_char_array(data, 50, 50)
    partprogram_name = _parse_char_array(data, 100, 50)

    # Valori big-endian (byte order Motorola dei PLC Siemens)
    status_raw = struct.unpack_from(">H", data, 150)[0]
    control_raw = struct.unpack_from(">H", data, 152)[0]
    alarm_code = struct.unpack_from(">H", data, 154)[0]

    bits = format(control_raw, "016b")
    control_binary = f"{bits[:4]} {bits[4:8]} {bits[8:12]} {bits[12:]}"
    control_hex = format(control_raw, "04X")

    if (control_raw & _RUNNING_BITS) == _RUNNING_BITS:
        machine_status = (
            MachineStatus.RUNNING_PAUSED
            if control_raw & _PAUSED_BIT
            else MachineStatus.RUNNING
        )
    else:
        machine_status = MachineStatus.STOPPED

    return MachineState(
        machine_id=machine_id,
        order_part_program=order,
        working_phase=working_phase,
        partprogram_name=partprogram_name,
        status_raw=status_raw,
        control_raw=control_raw,
        control_binary=control_binary,
        control_hex=control_hex,
        machine_status=machine_status,
        alarm_code=alarm_code,
    )
