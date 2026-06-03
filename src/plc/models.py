from enum import Enum

from pydantic import BaseModel, Field, field_validator


class MachineStatus(str, Enum):
    RUNNING = "in_esecuzione"
    RUNNING_PAUSED = "in_esecuzione_pausa"
    STOPPED = "arresto"


class MachineState(BaseModel):
    machine_id: str = Field(description="Identificatore univoco della macchina")
    order_part_program: str = Field(description="Codice commessa/part program attivo")
    working_phase: str = Field(description="Fase di lavorazione corrente (stringa descrittiva)")
    partprogram_name: str = Field(description="Nome del part program letto dal PLC")
    status_raw: int = Field(description="Valore grezzo del registro di stato (DB10)")
    control_raw: int = Field(description="Valore grezzo del registro di controllo (DB10)")
    control_binary: str = Field(description="Registro di controllo in formato binario, es. '0110 0100 1100 0010'")
    control_hex: str = Field(description="Registro di controllo in formato esadecimale, es. '64C2'")
    machine_status: MachineStatus = Field(description="Stato operativo della macchina")
    alarm_code: int = Field(description="Codice allarme corrente (0 = nessun allarme)")


class WriteOrderRequest(BaseModel):
    order_part_program: str = Field(
        description="Codice commessa o part program da scrivere sul PLC (max 50 caratteri ASCII)"
    )
    production_order_ids: list[str] = Field(
        default=[],
        description="Lista degli ID degli ordini di produzione da associare alla commessa",
    )

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
    machine_id: str = Field(description="Identificatore univoco della macchina")
    name: str = Field(description="Nome descrittivo della macchina")
    ip: str = Field(description="Indirizzo IP del PLC Step7")
    rack: int = Field(description="Numero rack del PLC")
    slot: int = Field(description="Numero slot del PLC")
