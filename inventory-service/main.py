import os
import pika
import json
import time
import threading
from fastapi import FastAPI
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "redis")

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=True
)


DATABASE_URL = os.getenv("DATABASE_URL")
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")

INVENTORY_QUEUE = "inventory_events"

engine = create_engine(DATABASE_URL, echo=True)

app = FastAPI(title="Inventory Service")


def wait_for_db(retries=10, delay=2):
    for i in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                return
        except OperationalError:
            print(f"[inventory] DB not ready ({i+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Inventory DB unavailable")

def invalidate_inventory_cache():
    redis_client.delete("inventory:all")


def consume_inventory_events():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBIT_HOST)
            )
            channel = connection.channel()
            channel.queue_declare(queue=INVENTORY_QUEUE, durable=True)

            print("[inventory] Waiting for inventory events...")

            def callback(ch, method, properties, body):
                event = json.loads(body)
                print("[inventory] Event received:", event)

                event_type = event["type"]
                items = event["items"]

                with engine.connect() as conn:
                    tx = conn.begin()

                    if event_type == "PRESCRIPTION_VALIDATED":
                        # STEP 2A → RESERVE
                        for item in items:
                            conn.execute(
                                text("""
                                    INSERT INTO inventory (brand, stock, reserved)
                                    VALUES (:brand, 0, 0)
                                    ON CONFLICT (brand) DO NOTHING
                                """),
                                {"brand": item["brand"]}
                            )

                            result = conn.execute(
                                text("""
                                    UPDATE inventory
                                    SET reserved = reserved + :qty
                                    WHERE brand = :brand
                                      AND (stock - reserved) >= :qty
                                    RETURNING stock, reserved
                                """),
                                {"brand": item["brand"], "qty": item["quantity"]}
                            )

                            if result.fetchone() is None:
                                tx.rollback()
                                print(f"[inventory] OUT OF STOCK: {item['brand']}")
                                ch.basic_ack(delivery_tag=method.delivery_tag)
                                return

                        tx.commit()
                        print("[inventory] Reservation successful")
                        invalidate_inventory_cache()

                    elif event_type == "ORDER_CONFIRMED":
                        # STEP 2B → COMMIT
                        for item in items:
                            conn.execute(
                                text("""
                                    UPDATE inventory
                                    SET stock = stock - :qty,
                                        reserved = reserved - :qty
                                    WHERE brand = :brand
                                      AND reserved >= :qty
                                """),
                                {"brand": item["brand"], "qty": item["quantity"]}
                            )

                        tx.commit()
                        print("[inventory] Order confirmed → stock committed")
                        invalidate_inventory_cache()

                    elif event_type == "ORDER_CANCELLED":
                        # STEP 2B → RELEASE
                        for item in items:
                            conn.execute(
                                text("""
                                    UPDATE inventory
                                    SET reserved = reserved - :qty
                                    WHERE brand = :brand
                                      AND reserved >= :qty
                                """),
                                {"brand": item["brand"], "qty": item["quantity"]}
                            )

                        tx.commit()
                        print("[inventory] Order cancelled → reservation released")
                        invalidate_inventory_cache()


                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(
                queue=INVENTORY_QUEUE,
                on_message_callback=callback,
                auto_ack=False
            )

            channel.start_consuming()

        except Exception as e:
            print("[inventory] Consumer crashed, retrying:", e)
            time.sleep(3)


@app.on_event("startup")
def startup():
    wait_for_db()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inventory (
                id SERIAL PRIMARY KEY,
                brand TEXT UNIQUE,
                stock INT NOT NULL DEFAULT 0,
                reserved INT NOT NULL DEFAULT 0
            );
        """))
        conn.commit()

    threading.Thread(target=consume_inventory_events, daemon=True).start()


@app.get("/inventory")
def list_inventory():
    cache_key = "inventory:all"

    # 1) Try cache
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    # 2) Fetch from DB
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT brand, stock, reserved, (stock - reserved) AS available
            FROM inventory
            ORDER BY brand
        """))
        data = [dict(r._mapping) for r in rows]

    # 3) Store in cache (TTL 30 sec)
    redis_client.setex(cache_key, 30, json.dumps(data))

    return data

