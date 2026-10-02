import asyncio
import os

import uvicorn

import collector
from research_app import app

async def run_web():
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
        log_level="info",
        access_log=False,
    )
    server = uvicorn.Server(config)
    await server.serve()

async def main():
    web = asyncio.create_task(run_web())
    collector_task = asyncio.create_task(collector.main())

    done, pending = await asyncio.wait(
        {web, collector_task},
        return_when=asyncio.FIRST_EXCEPTION,
    )

    for task in pending:
        task.cancel()

    for task in done:
        exc = task.exception()
        if exc:
            raise exc

if __name__ == "__main__":
    asyncio.run(main())
