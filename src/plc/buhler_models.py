from typing import Any

from pydantic import BaseModel, Field, field_validator


class TimestampField(BaseModel):
    label: str | None = Field(description="Nome dello step o della ricetta estratto dalla stringa OPC-UA")
    timestamp: str | None = Field(description="Timestamp in formato ISO 8601 (YYYY-MM-DDTHH:MM:SS.mmm), None se assente")
    raw: str | None = Field(description="Valore originale restituito dal PLC Buhler")


class BuhlerState(BaseModel):
    machine_id: str = Field(description="Identificatore univoco della macchina Buhler")
    # CMD nodes
    next_recipe_name: str = Field(description="Nome della prossima ricetta da avviare (nodo CMD.Process_NextRecipeName)")
    send_next_recipe: bool | None = Field(description="Flag di invio ricetta (nodo CMD.Process_SendNextRecipe)")
    # ACT process nodes
    recipe_name: str = Field(description="Nome della ricetta in esecuzione (nodo ACT.recipe_name)")
    step_name: str = Field(description="Nome dello step corrente (nodo ACT.step_name)")
    step_number: Any = Field(description="Numero dello step corrente")
    step_time: Any = Field(description="Durata dello step corrente")
    step_start_timestamp: TimestampField | None = Field(description="Timestamp di inizio step corrente")
    step_end_timestamp: TimestampField | None = Field(description="Timestamp di fine step corrente")
    start_timestamp: TimestampField | None = Field(description="Timestamp di inizio processo")
    end_timestamp: TimestampField | None = Field(description="Timestamp di fine processo")
    # STA nodes
    mode_idle: bool | None = Field(description="True se la macchina è in modalità idle (nodo STA)")
    process_start: bool = Field(description="True se il processo è avviato (nodo STA.process_start)")
    process_stop: bool | None = Field(description="True se il processo è in arresto (nodo STA.process_stop)")
    process_end: bool | None = Field(description="True se il processo è terminato (nodo STA.process_end)")
    # Temperature
    water_temp_inlet_coldwater: float | None = Field(description="Temperatura acqua fredda in ingresso (°C)")
    # Derived
    is_running: bool = Field(description="True se la macchina è in esecuzione (derivato da process_start e mode_idle)")


class WriteRecipeRequest(BaseModel):
    recipe_name: str = Field(description="Nome della ricetta da inviare al PLC Buhler (max 256 caratteri)")

    @field_validator("recipe_name")
    @classmethod
    def check_length(cls, v: str) -> str:
        if len(v) > 256:
            raise ValueError("recipe_name: lunghezza massima 256 caratteri")
        return v


class BuhlerMachineInfo(BaseModel):
    machine_id: str = Field(description="Identificatore univoco della macchina Buhler")
    name: str = Field(description="Nome descrittivo della macchina")
    url: str = Field(description="URL del server OPC-UA (es. opc.tcp://192.168.1.10:4840)")
