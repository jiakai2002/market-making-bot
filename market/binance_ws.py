import asyncio
import json
import websockets


class BinanceWSClient:
    def __init__(self, url: str, on_message):
        self.url = url
        self.on_message = on_message

    async def connect(self):
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=20) as ws:
                    print(f"Connected to {self.url}")

                    async for raw in ws:
                        msg = json.loads(raw)
                        await self.on_message(msg)

            except Exception as e:
                print(f"WebSocket error: {e}")
                print("Reconnecting in 3s...")
                await asyncio.sleep(3)