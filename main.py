"""Esempio di utilizzo del client PLC API."""
import asyncio

from src.client import PlcApiClient


async def main() -> None:
    async with PlcApiClient() as client:
        # Esempio: prima chiamata (acquisisce il token)
        response = await client.get("/plc_api/machines")
        print(response.json())


if __name__ == "__main__":
    asyncio.run(main())
