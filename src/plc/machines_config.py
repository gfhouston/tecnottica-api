from dataclasses import dataclass


@dataclass(frozen=True)
class MachineConfig:
    machine_id: str
    name: str
    ip: str
    rack: int
    slot: int


MACHINES: dict[str, MachineConfig] = {
    "optotech_scandicci": MachineConfig(
        machine_id="optotech_scandicci",
        name="Optotech Scandicci",
        ip="192.168.11.50",
        rack=0,
        slot=2,
    ),
}
