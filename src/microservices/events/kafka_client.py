import asyncio
import os
import json
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from typing import Callable, Coroutine, Any
from datetime import datetime, timezone, date

logger = logging.getLogger("events.kafka")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICS = os.getenv("TOPICS", "user-events,payment-events,movie-events").split(",")

def _json_default(obj):
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        iso = obj.astimezone(timezone.utc).isoformat()
        return iso.replace("+00:00", "Z")
    if isinstance(obj, date):
        return obj.isoformat()
    return str(obj)

class KafkaClient:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP)
        self.consumer = AIOKafkaConsumer(
            *TOPICS,
            loop=loop,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="events-service-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )
        self._consumer_task = None

    async def start(self, message_handler: Callable[[str, dict], Coroutine[Any,Any, None]]):
        logger.info("Starting Kafka producer and consumer. Brokers=%s Topics=%s", KAFKA_BOOTSTRAP, TOPICS)
        await self.producer.start()
        await self.consumer.start()
        self._consumer_task = asyncio.create_task(self._consume_loop(message_handler))
        logger.info("Kafka started")

    async def stop(self):
        logger.info("Stopping Kafka client")
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        await self.consumer.stop()
        await self.producer.stop()
        logger.info("Kafka stopped")

    async def send(self, topic: str, value: dict, key: str = None):
        payload = json.dumps(value, default=_json_default).encode("utf-8")
        fut = await self.producer.send_and_wait(topic, payload, key=(key.encode("utf-8") if key else None))
        partition = fut.partition
        offset = fut.offset
        logger.info("Produced to %s partition=%s offset=%s key=%s", topic, partition, offset, key)
        return partition, offset

    async def _consume_loop(self, handler):
        try:
            async for msg in self.consumer:
                try:
                    val = msg.value.decode("utf-8")
                    data = json.loads(val)
                except Exception as e:
                    logger.exception("Failed to decode message: %s", e)
                    continue
                topic = msg.topic
                logger.debug("Consumed message topic=%s partition=%s offset=%s", msg.topic, msg.partition, msg.offset)
                try:
                    await handler(topic, data)
                except Exception:
                    logger.exception("Error in message handler")
        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled, exiting")
        except Exception:
            logger.exception("Consumer loop error")
