from .machines_config import MACHINES, MachineConfig
from .models import MachineInfo
from .step7_driver import Step7Driver


class MachineRegistry:
    """Registro centralizzato dei driver PLC."""

    def __init__(self, timeout: float = 10.0) -> None:
        self._drivers: dict[str, Step7Driver] = {
            machine_id: Step7Driver(
                machine_id=cfg.machine_id,
                ip=cfg.ip,
                rack=cfg.rack,
                slot=cfg.slot,
                timeout=timeout,
            )
            for machine_id, cfg in MACHINES.items()
        }

    def get(self, machine_id: str) -> Step7Driver:
        driver = self._drivers.get(machine_id)
        if driver is None:
            raise KeyError(machine_id)
        return driver

    def list_machines(self) -> list[MachineInfo]:
        return [
            MachineInfo(
                machine_id=cfg.machine_id,
                name=cfg.name,
                ip=cfg.ip,
                rack=cfg.rack,
                slot=cfg.slot,
            )
            for cfg in MACHINES.values()
        ]

    def disconnect_all(self) -> None:
        for driver in self._drivers.values():
            driver.close()
