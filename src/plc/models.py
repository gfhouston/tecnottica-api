from enum import Enum

from pydantic import BaseModel, field_validator


class MachineStatus(str, Enum):
    RUNNING = "in_esecuzione"
    RUNNING_PAUSED = "in_esecuzione_pausa"
    STOPPED = "arresto"


class MachineState(BaseModel):
    machine_id: str
    order_part_program: str
    working_phase: str
    partprogram_name: str
    status_raw: int
    control_raw: int
    control_binary: str   # es. "0110 0100 1100 0010"
    control_hex: str      # es. "64C2"
    machine_status: MachineStatus
    alarm_code: int


class WriteOrderRequest(BaseModel):
    order_part_program: str

    @field_validator("order_part_program")
    @classmethod
    def check_length(cls, v: str) -> str:
        if len(v) > 50:
            raise ValueError("order_part_program: lunghezza massima 50 caratteri")
        try:
            v.encode("ascii")
        except UnicodeEncodeError as exc:
            raise ValueError("order_part_program: solo caratteri ASCII") from exc
        return v


class MachineInfo(BaseModel):
    machine_id: str
    name: str
    ip: str
    rack: int
    slot: int
