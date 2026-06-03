from dataclasses import dataclass


@dataclass(frozen=True)
class BuhlerMachineConfig:
    machine_id: str
    name: str
    url: str


BUHLER_MACHINES: dict[str, BuhlerMachineConfig] = {
    "buhler_scandicci": BuhlerMachineConfig(
        machine_id="buhler_scandicci",
        name="Buhler Scandicci",
        url="opc.tcp://192.168.11.101:4840",
    ),
}
