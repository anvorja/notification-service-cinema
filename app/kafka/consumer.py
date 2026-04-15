# app/kafka/consumer.py
import asyncio
import json
import logging
import ssl

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from app.core.config import settings
from app.services.email_service import (
    send_purchase_confirmation,
    send_refund_confirmation,
    send_password_reset_email,
)

logger = logging.getLogger(__name__)

TOPICS = ["purchase.confirmed", "order.refunded", "auth.password_reset_requested"]

# How long to wait before restarting the consumer after a failure
_RESTART_DELAY_SECONDS = 10


async def _run_consumer() -> None:
    """Single consumer lifecycle: connect → consume → stop."""
    ssl_context = ssl.create_default_context()
    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=settings.KAFKA_API_KEY,
        sasl_plain_password=settings.KAFKA_API_SECRET,
        ssl_context=ssl_context,
        group_id=settings.KAFKA_GROUP_ID,
        # Evita reenviar correos históricos cuando arranca un consumer sin offsets previos.
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
    )

    await consumer.start()
    logger.info(
        "Notification consumer started | topics=%s | group=%s",
        TOPICS,
        settings.KAFKA_GROUP_ID,
    )

    try:
        async for msg in consumer:
            topic = msg.topic
            payload = msg.value
            order_id = payload.get("order_id", "?")

            logger.info(
                "Event received | topic=%s | order_id=%s | user=%s",
                topic,
                order_id,
                payload.get("user_email", "?"),
            )

            try:
                if topic == "purchase.confirmed":
                    success = await send_purchase_confirmation(payload)
                    if success:
                        logger.info("Confirmation email sent | order_id=%s", order_id)
                    else:
                        logger.error("Email failed | order_id=%s — will NOT retry (offset committed)", order_id)

                elif topic == "order.refunded":
                    success = await send_refund_confirmation(payload)
                    if success:
                        logger.info("Refund email sent | order_id=%s", order_id)
                    else:
                        logger.error("Refund email failed | order_id=%s — will NOT retry (offset committed)", order_id)

                elif topic == "auth.password_reset_requested":
                    success = await send_password_reset_email(payload)
                    if success:
                        logger.info("Password reset email sent | user=%s", payload.get("user_email", "?"))
                    else:
                        logger.error("Password reset email failed | user=%s", payload.get("user_email", "?"))

                # Commit offset only after processing (manual commit)
                await consumer.commit()

            except Exception as e:
                logger.error(
                    "Unhandled error processing event | topic=%s | order_id=%s | error=%s",
                    topic, order_id, e,
                )
                # Still commit to avoid infinite retry on poison pill messages
                await consumer.commit()

    finally:
        await consumer.stop()
        logger.info("Notification consumer stopped")


async def start_consumer() -> None:
    """
    Consumer supervisor loop. Restarts the consumer if it crashes.
    Runs indefinitely until the task is cancelled by the app lifespan.
    """
    if not settings.KAFKA_BOOTSTRAP_SERVERS:
        logger.warning(
            "KAFKA_BOOTSTRAP_SERVERS not set — consumer disabled. "
            "Set Confluent Cloud credentials in .env to activate."
        )
        return

    while True:
        try:
            await _run_consumer()
            # _run_consumer exited cleanly (only happens on CancelledError propagation)
            break
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled — shutting down")
            raise
        except KafkaConnectionError as e:
            logger.error("Kafka connection lost: %s — restarting in %ds", e, _RESTART_DELAY_SECONDS)
        except Exception as e:
            logger.error("Consumer crashed: %s — restarting in %ds", e, _RESTART_DELAY_SECONDS)

        await asyncio.sleep(_RESTART_DELAY_SECONDS)
