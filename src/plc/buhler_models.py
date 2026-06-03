from typing import Any

from pydantic import BaseModel, field_validator


class TimestampField(BaseModel):
    """
    Il PLC Buhler restituisce i campi timestamp come stringhe che concatenano
    nome (step/ricetta) e timestamp nel formato YYYY-MM-DD-HH:MM:SS.mmm.
    Questo modello li espone già separati.
    """
    label: str | None      # nome step o ricetta estratto
    timestamp: str | None  # ISO 8601: YYYY-MM-DDTHH:MM:SS.mmm (None se assente)
    raw: str | None        # valore originale dal PLC


class BuhlerState(BaseModel):
    machine_id: str
    # CMD nodes
    next_recipe_name: str
    send_next_recipe: bool | None
    # ACT process nodes
    recipe_name: str
    step_name: str
    step_number: Any
    step_time: Any
    step_start_timestamp: TimestampField | None
    step_end_timestamp: TimestampField | None
    start_timestamp: TimestampField | None
    end_timestamp: TimestampField | None
    # STA nodes
    mode_idle: bool | None
    process_start: bool
    process_stop: bool | None
    process_end: bool | None
    # Temperature
    water_temp_inlet_coldwater: float | None
    # Derived
    is_running: bool


class WriteRecipeRequest(BaseModel):
    recipe_name: str

    @field_validator("recipe_name")
    @classmethod
    def check_length(cls, v: str) -> str:
        if len(v) > 256:
            raise ValueError("recipe_name: lunghezza massima 256 caratteri")
        return v


class BuhlerMachineInfo(BaseModel):
    machine_id: str
    name: str
    url: str
