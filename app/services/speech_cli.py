from __future__ import annotations

import asyncio

from app.config import settings
from app.services.speech_data import SpeechTapeService


async def main() -> None:
    service = SpeechTapeService(settings)
    await service.start()
    try:
        snapshot = await service.snapshot(force=True)
    finally:
        await service.stop()
    print(snapshot.model_dump_json())


if __name__ == "__main__":
    asyncio.run(main())
