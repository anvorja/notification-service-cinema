# app/main.py
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.kafka.consumer import start_consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

_consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _consumer_task
    logger.info("Starting Notification Service...")
    _consumer_task = asyncio.create_task(start_consumer())
    logger.info("Notification Service ready — consuming Kafka events")
    yield
    logger.info("Shutting down Notification Service...")
    if _consumer_task:
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Notification Service", lifespan=lifespan)


@app.get("/health")
async def health():
    consumer_running = _consumer_task is not None and not _consumer_task.done()
    return {
        "status": "healthy" if consumer_running else "degraded",
        "consumer": "running" if consumer_running else "stopped",
    }
