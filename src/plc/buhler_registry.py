from .buhler_config import BUHLER_MACHINES
from .buhler_models import BuhlerMachineInfo
from .opcua_driver import OpcUaDriver


class BuhlerRegistry:
    """Registro centralizzato dei driver OPC-UA Buhler."""

    def __init__(self, timeout: float = 10.0) -> None:
        self._drivers: dict[str, OpcUaDriver] = {
            machine_id: OpcUaDriver(
                machine_id=cfg.machine_id,
                url=cfg.url,
                timeout=timeout,
            )
            for machine_id, cfg in BUHLER_MACHINES.items()
        }

    def get(self, machine_id: str) -> OpcUaDriver:
        driver = self._drivers.get(machine_id)
        if driver is None:
            raise KeyError(machine_id)
        return driver

    def list_machines(self) -> list[BuhlerMachineInfo]:
        return [
            BuhlerMachineInfo(
                machine_id=cfg.machine_id,
                name=cfg.name,
                url=cfg.url,
            )
            for cfg in BUHLER_MACHINES.values()
        ]

    async def disconnect_all(self) -> None:
        for driver in self._drivers.values():
            await driver.disconnect()
