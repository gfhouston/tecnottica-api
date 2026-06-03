import asyncio
from typing import Any

from pydantic import BaseModel, Field

from .buhler_config import BUHLER_MACHINES
from .buhler_registry import BuhlerRegistry
from .machines_config import MACHINES
from .registry import MachineRegistry


class UnifiedMachineInfo(BaseModel):
    machine_id: str = Field(description="Identificatore univoco della macchina")
    name: str = Field(description="Nome descrittivo della macchina")
    protocol: str = Field(description="Protocollo di comunicazione: 'step7' (snap7) oppure 'opcua' (Buhler BMMC)")
    ip: str | None = Field(default=None, description="Indirizzo IP del PLC (solo Step7)")
    rack: int | None = Field(default=None, description="Numero rack del PLC (solo Step7)")
    slot: int | None = Field(default=None, description="Numero slot del PLC (solo Step7)")
    url: str | None = Field(default=None, description="URL del server OPC-UA (solo Buhler, es. opc.tcp://...)")


class UnifiedRegistry:
    """
    Registro unificato che aggrega driver Step7 (snap7) e OPC-UA (asyncua).
    Espone anche i sub-registry separati per i monitor di venting.
    """

    def __init__(self) -> None:
        self._step7 = MachineRegistry()
        self._buhler = BuhlerRegistry()

    @property
    def step7(self) -> MachineRegistry:
        return self._step7

    @property
    def buhler(self) -> BuhlerRegistry:
        return self._buhler

    def list_machines(self) -> list[UnifiedMachineInfo]:
        result: list[UnifiedMachineInfo] = []
        for cfg in MACHINES.values():
            result.append(UnifiedMachineInfo(
                machine_id=cfg.machine_id,
                name=cfg.name,
                protocol="step7",
                ip=cfg.ip,
                rack=cfg.rack,
                slot=cfg.slot,
            ))
        for cfg in BUHLER_MACHINES.values():
            result.append(UnifiedMachineInfo(
                machine_id=cfg.machine_id,
                name=cfg.name,
                protocol="opcua",
                url=cfg.url,
            ))
        return result

    async def read_state(self, machine_id: str) -> dict[str, Any]:
        if machine_id in MACHINES:
            loop = asyncio.get_event_loop()
            state = await loop.run_in_executor(
                None, self._step7.get(machine_id).read_state
            )
            return state.model_dump()
        if machine_id in BUHLER_MACHINES:
            state = await self._buhler.get(machine_id).read_state()
            return state.model_dump()
        raise KeyError(machine_id)

    async def write_order(self, machine_id: str, order: str) -> dict[str, Any]:
        if machine_id in MACHINES:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, self._step7.get(machine_id).write_order, order
            )
            return {
                "success": True,
                "machine_id": machine_id,
                "order_part_program": order,
            }
        if machine_id in BUHLER_MACHINES:
            confirmed = await self._buhler.get(machine_id).write_recipe(order)
            return {
                "success": True,
                "machine_id": machine_id,
                "order_part_program": order,
                "recipe_name_confirmed": confirmed,
                "write_confirmed": confirmed == order,
            }
        raise KeyError(machine_id)

    async def disconnect_all(self) -> None:
        self._step7.disconnect_all()
        await self._buhler.disconnect_all()
