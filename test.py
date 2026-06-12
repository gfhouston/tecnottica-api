import asyncio
from asyncua import Client

class Handler:
    def datachange_notification(self, node, val, data):
        print(f"Valore cambiato: {val}")

async def main():
    async with Client('opc.tcp://192.168.11.101:4840') as client:
        handler = Handler()

        subscription = await client.create_subscription(
            500,
            handler
        )

        node = client.get_node("ns=4;s=BMMC.ACT.Process_Step_Start_Timestamp")

        _handle = await subscription.subscribe_data_change(node)

        while True:
            await asyncio.sleep(1)

asyncio.run(main())