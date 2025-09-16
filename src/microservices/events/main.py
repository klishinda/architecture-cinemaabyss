# src/microservices/events/main.py  (пример для вашей текущей структуры: /app/main.py)
import os
import uvicorn
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from kafka_client import KafkaClient
from models import EventResponse, UserEvent, PaymentEvent, MovieEvent

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("events.service")

app = FastAPI(
    title="CinemaAbyss - Events Service",
    version="0.1.0",
    docs_url="/docs",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

loop = asyncio.get_event_loop()
kafka_client = KafkaClient(loop=loop)


async def internal_message_handler(topic: str, data: dict):
    logger.info("INTERNAL HANDLER | topic=%s | event=%s", topic, data)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    try:
        await kafka_client.start(internal_message_handler)
        logger.info("Kafka client started in lifespan startup")
    except Exception:
        logger.exception("Failed to start Kafka client at startup")
    try:
        yield
    finally:
        # shutdown
        try:
            await kafka_client.stop()
            logger.info("Kafka client stopped in lifespan shutdown")
        except Exception:
            logger.exception("Error while stopping Kafka client")


app.router.lifespan_context = lifespan


def model_to_dict(model):
    if hasattr(model, "model_dump"):
        return model.model_dump()
    if hasattr(model, "dict"):
        return model.dict()
    return dict(model)

class IDGenerator:
    def __init__(self):
        self._counters = {
            "user": 0,
            "payment": 0,
            "movie": 0,
        }
        self._locks = {k: asyncio.Lock() for k in self._counters}

    async def next(self, group: str) -> int:
        if group not in self._counters:
            raise ValueError(f"Unknown id group: {group}")
        async with self._locks[group]:
            self._counters[group] += 1
            return self._counters[group]


id_gen = IDGenerator()


@app.get("/api/events/health")
async def health():
    status = True
    return {"status": status, "kafka_bootstrap": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")}

@app.post("/api/events/user", response_model=EventResponse, status_code=201)
async def create_user_event(event: UserEvent):
    topic = "user-events"
    try:
        new_id = await id_gen.next("user")
        payload = {
            "id": new_id,
            "type": "user",
            "timestamp": event.timestamp.isoformat(),
            "payload": model_to_dict(event),
        }
        partition, offset = await kafka_client.send(topic, payload, key=str(event.user_id))
        return {"status": "success", "partition": partition, "offset": offset, "event": payload}
    except Exception as e:
        logger.exception("Failed to produce user-event")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/events/payment", response_model=EventResponse, status_code=201)
async def create_payment_event(event: PaymentEvent):
    topic = "payment-events"
    try:
        new_id = await id_gen.next("payment")
        payload = {
            "id": new_id,
            "type": "payment",
            "timestamp": event.timestamp.isoformat(),
            "payload": model_to_dict(event),
        }
        partition, offset = await kafka_client.send(topic, payload, key=str(event.payment_id))
        return {"status": "success", "partition": partition, "offset": offset, "event": payload}
    except Exception as e:
        logger.exception("Failed to produce payment-event")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/events/movie", response_model=EventResponse, status_code=201)
async def create_movie_event(event: MovieEvent):
    topic = "movie-events"
    try:
        new_id = await id_gen.next("movie")
        payload = {
            "id": new_id,
            "type": "movie",
            "timestamp": event.timestamp.isoformat(),
            "payload": model_to_dict(event),
        }
        partition, offset = await kafka_client.send(topic, payload, key=str(event.movie_id))
        return {"status": "success", "partition": partition, "offset": offset, "event": payload}
    except Exception as e:
        logger.exception("Failed to produce movie-event")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8082")), reload=False)
