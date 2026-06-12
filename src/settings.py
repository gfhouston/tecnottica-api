import os
from dataclasses import dataclass


def get_bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class EmailSettings:
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    from_name: str
    to_address: str

    @property
    def is_configured(self) -> bool:
        return all(
            [
                self.smtp_host.strip(),
                self.smtp_user.strip(),
                self.smtp_password.strip(),
                self.to_address.strip(),
            ]
        )
