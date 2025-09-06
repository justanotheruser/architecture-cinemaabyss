from contextlib import asynccontextmanager
import os
import json
import uuid
import asyncio
import logging
from datetime import datetime, timezone
from contextlib import suppress
from typing import Any

from fastapi import FastAPI, APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


PORT = int(os.getenv("PORT", "8082"))
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
TOPIC_MOVIE = os.getenv("TOPIC_MOVIE", "movie-events")
TOPIC_USER = os.getenv("TOPIC_USER", "user-events")
TOPIC_PAYMENT = os.getenv("TOPIC_PAYMENT", "payment-events")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "events-service")
CONSUME_ENABLED = os.getenv("CONSUME_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("events-service")


class MovieEventIn(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: int | None = None
    rating: float | None = None
    genres: list[str] | None = None
    description: str | None = None

class UserEventIn(BaseModel):
    user_id: int
    action: str
    timestamp: datetime | None = None
    username: str | None = None
    email: EmailStr | None = None

class PaymentEventIn(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: datetime | None = None
    method_type: str | None = None

class EventOut(BaseModel):
    id: str
    type: str
    timestamp: datetime
    payload: dict[str, Any]

class EventResponse(BaseModel):
    status: str
    partition: int
    offset: int
    event: EventOut

@asynccontextmanager
async def lifespan(_: FastAPI):
    global producer, consumer

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS, acks="all")
    await producer.start()
    logger.info("Kafka producer connected | brokers=%s", KAFKA_BROKERS)

    consume_task = None
    if CONSUME_ENABLED:
        consumer = AIOKafkaConsumer(
            TOPIC_MOVIE,
            TOPIC_USER,
            TOPIC_PAYMENT,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await consumer.start()
        consume_task = asyncio.create_task(consume_loop())
    yield
    if consume_task is not None:
        consume_task.cancel()
        with suppress(asyncio.CancelledError):
            await consume_task
    if consumer is not None:
        await consumer.stop()
        logger.info("Kafka consumer disconnected")
    if producer is not None:
        await producer.stop()
        logger.info("Kafka producer disconnected")


app = FastAPI(lifespan=lifespan)
router = APIRouter(prefix="/api/events", tags=["events"])

producer: AIOKafkaProducer = None  # type: ignore
consumer: AIOKafkaConsumer = None  # type: ignore


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def build_event(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": f"{event_type}-{uuid.uuid4().hex}",
        "type": event_type,
        "timestamp": now_utc().isoformat(),
        "payload": payload,
    }


async def send_to_kafka(topic: str, event: dict[str, Any]):
    data = json.dumps(event, ensure_ascii=False).encode("utf-8")
    key = event["id"].encode("utf-8")

    logger.info(
        "Kafka OUT | topic=%s id=%s type=%s payload_keys=%s",
        topic,
        event["id"],
        event["type"],
        list(event["payload"].keys()),
    )

    try:
        meta = await producer.send_and_wait(topic, data, key=key)
        return meta
    except Exception as e:
        logger.exception("Failed to send to Kafka: %s", e)
        raise HTTPException(status_code=502, detail="Failed to send event to Kafka")


async def consume_loop():
    logger.info(
        "Kafka consumer started | brokers=%s topics=%s group=%s",
        KAFKA_BROKERS,
        [TOPIC_MOVIE, TOPIC_USER, TOPIC_PAYMENT],
        CONSUMER_GROUP,
    )
    try:
        async for msg in consumer:
            try:
                payload = json.loads(msg.value.decode("utf-8"))
            except Exception:
                payload = {"raw": msg.value.decode("utf-8", errors="replace")}

            logger.info(
                "Kafka IN  | topic=%s partition=%s offset=%s key=%s id=%s type=%s",
                msg.topic,
                msg.partition,
                msg.offset,
                (msg.key.decode("utf-8") if msg.key else None),
                payload.get("id"),
                payload.get("type"),
            )
    except asyncio.CancelledError:
        logger.info("Consumer task cancelled")
        raise
    except Exception:
        logger.exception("Consumer loop error")
    finally:
        logger.info("Consumer loop stopped")


@app.get("/api/events/health")
async def health():
    return {"status": True}


@router.post("/movie", response_model=EventResponse, status_code=201)
async def create_movie_event(body: MovieEventIn):
    event = build_event("movie", body.model_dump(mode='json'))
    meta = await send_to_kafka(TOPIC_MOVIE, event)
    return {
        "status": "success",
        "partition": meta.partition,
        "offset": meta.offset,
        "event": event,
    }


@router.post("/user", response_model=EventResponse, status_code=201)
async def create_user_event(body: UserEventIn):
    payload = body.model_dump(mode='json')
    # Если клиент не прислал timestamp — проставим сейчас (для совместимости со схемой)
    payload.setdefault("timestamp", now_utc().isoformat())
    event = build_event("user", payload)
    meta = await send_to_kafka(TOPIC_USER, event)
    return {
        "status": "success",
        "partition": meta.partition,
        "offset": meta.offset,
        "event": event,
    }


@router.post("/payment", response_model=EventResponse, status_code=201)
async def create_payment_event(body: PaymentEventIn):
    payload = body.model_dump(mode='json')
    payload.setdefault("timestamp", now_utc().isoformat())
    event = build_event("payment", payload)
    meta = await send_to_kafka(TOPIC_PAYMENT, event)
    return {
        "status": "success",
        "partition": meta.partition,
        "offset": meta.offset,
        "event": event,
    }


app.include_router(router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT)
