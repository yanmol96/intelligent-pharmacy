import os
import pika
import json
import time
import threading
from fastapi import FastAPI, UploadFile, File, Form
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from pydantic import BaseModel
from typing import List
from jose import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Header
import boto3


SECRET_KEY = "supersecretkey"
ALGORITHM = "HS256"

class ReturnItem(BaseModel):
    medicine_id: int
    quantity: int
    reason: str


DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, echo=True)

# -------- S3 / MinIO CONFIG --------
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password123")
S3_BUCKET = "prescriptions"

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)


RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
QUEUE_NAME = "prescription_events"
ML_RESULT_QUEUE = "ml_results"
INVENTORY_QUEUE = "inventory_events"

app = FastAPI(title="Order & Prescription Service")

# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()

def create_token(user):
    return jwt.encode(
        {"sub": user["username"], "role": user["role"]},
        SECRET_KEY,
        algorithm=ALGORITHM
    )


def check_idempotency(conn, key, endpoint):
    row = conn.execute(
        text("""
            SELECT response
            FROM idempotency_keys
            WHERE key = :k AND endpoint = :e
        """),
        {"k": key, "e": endpoint}
    ).fetchone()

    return row[0] if row else None


def save_idempotency(conn, key, endpoint, response):
    conn.execute(
        text("""
            INSERT INTO idempotency_keys (key, endpoint, response)
            VALUES (:k, :e, :r)
        """),
        {"k": key, "e": endpoint, "r": json.dumps(response)}
    )



# ------------------ RABBIT HELPERS ------------------

def publish_event(event: dict):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBIT_HOST)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(event),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    connection.close()


def publish_inventory_event(event: dict):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBIT_HOST)
    )
    channel = connection.channel()
    channel.queue_declare(queue=INVENTORY_QUEUE, durable=True)

    channel.basic_publish(
        exchange="",
        routing_key=INVENTORY_QUEUE,
        body=json.dumps(event),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    connection.close()


# ------------------ DB WAIT ------------------

def wait_for_db(retries=10, delay=2):
    for i in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                return
        except OperationalError:
            print(f"DB not ready ({i+1}/{retries}), retrying...")
            time.sleep(delay)
    raise RuntimeError("DB not available")

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    try:
        payload = jwt.decode(
            credentials.credentials,
            SECRET_KEY,
            algorithms=[ALGORITHM]
        )
        return payload
    except:
        raise HTTPException(status_code=401, detail="Invalid token")


def require_role(role: str):
    def checker(user = Depends(get_current_user)):
        if user["role"] != role:
            raise HTTPException(status_code=403, detail="Forbidden")
        return user
    return checker




# ------------------ ML RESULT CONSUMER ------------------

def consume_ml_results():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBIT_HOST)
            )
            channel = connection.channel()
            channel.queue_declare(queue=ML_RESULT_QUEUE, durable=True)

            def callback(ch, method, properties, body):
                try:
                    result = json.loads(body)
                    print("Order service received ML result:", result)

                    prescription_id = result["prescription_id"]
                    items = result.get("items", [])
                    interaction_found = result.get("interaction_found", False)

                    # ---------------- SAVE ITEMS ----------------
                    missing_brands = []

                    with engine.connect() as conn:

                        conn.execute(
                            text("DELETE FROM prescription_items WHERE prescription_id = :pid"),
                            {"pid": prescription_id}
                        )

                        for item in items:
                            brand = item["brand"]

                            med = conn.execute(
                                text("SELECT id FROM medicines WHERE brand_name = :b"),
                                {"b": brand}
                            ).fetchone()

                            if not med:
                                missing_brands.append(brand)
                                continue

                            medicine_id = med[0]

                            conn.execute(
                                text("""
                                    INSERT INTO prescription_items
                                    (prescription_id, brand, quantity, unit, source, medicine_id)
                                    VALUES (:pid, :brand, :qty, :unit, :src, :mid)
                                """),
                                {
                                    "pid": prescription_id,
                                    "brand": brand,
                                    "qty": item["quantity"],
                                    "unit": item.get("unit", "tablet"),
                                    "src": item.get("quantity_source", "ml"),
                                    "mid": medicine_id
                                }
                            )

                        conn.commit()


                    # ---------------- STATUS UPDATE ----------------
                    if missing_brands:
                        new_status = "REVIEW_REQUIRED"
                        print("Unknown medicines:", missing_brands)

                    elif interaction_found:
                        new_status = "REVIEW_REQUIRED"

                    else:
                        new_status = "VALIDATED"


                    with engine.connect() as conn:
                        conn.execute(
                            text("""
                                UPDATE prescriptions
                                SET status = :status
                                WHERE id = :pid
                            """),
                            {"pid": prescription_id, "status": new_status}
                        )
                        conn.commit()

                    # ---------------- INVENTORY RESERVE ----------------
                    if not interaction_found and not missing_brands and items:
                        publish_inventory_event({
                            "type": "PRESCRIPTION_VALIDATED",
                            "prescription_id": prescription_id,
                            "items": items
                        })
                        print("[order] Inventory reservation requested")
                    else:
                        print("[order] Interaction detected → pharmacist review required")

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    print("ERROR inside ML callback:", e)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_consume(
                queue=ML_RESULT_QUEUE,
                on_message_callback=callback,
                auto_ack=False
            )

            channel.start_consuming()

        except Exception as e:
            print("Order ML consumer error, retrying:", e)
            time.sleep(3)


# ------------------ STARTUP ------------------

@app.on_event("startup")
def startup():
    wait_for_db()
    threading.Thread(target=consume_ml_results, daemon=True).start()

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password123",
    )

    try:
        s3.create_bucket(Bucket="prescriptions")
        print("MinIO bucket created")
    except:
        print("MinIO bucket already exists")

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS prescriptions (
                id SERIAL PRIMARY KEY,
                patient_name TEXT,
                file_path TEXT,
                status TEXT
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                prescription_id INT,
                status TEXT
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS prescription_items (
                id SERIAL PRIMARY KEY,
                prescription_id INT,
                brand TEXT,
                quantity INT,
                unit TEXT,
                source TEXT
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE,
                password TEXT,
                role TEXT
            );
        """))


        conn.commit()


# ------------------ ROUTES ------------------

@app.post("/prescriptions")
def create_prescription(
    patient_name: str = Form(...),
    file: UploadFile = File(...),
    user=Depends(require_role("DOCTOR"))
):
    filename = f"{os.urandom(6).hex()}_{file.filename}"
    file_path = f"/data/prescriptions/{filename}"

    filename = f"{os.urandom(6).hex()}_{file.filename}"

    s3.upload_fileobj(
        file.file,
        "prescriptions",
        filename
    )

    file_path = f"s3://prescriptions/{filename}"


    with engine.connect() as conn:
        result = conn.execute(
            text("""
                INSERT INTO prescriptions (patient_name, file_path, status)
                VALUES (:p, :fp, 'CREATED')
                RETURNING id
            """),
            {"p": patient_name, "fp": file_path}
        )
        pid = result.fetchone()[0]
        conn.commit()

    publish_event({
        "type": "PRESCRIPTION_CREATED",
        "prescription_id": pid,
        "file_path": file_path
    })

    return {"prescription_id": pid}


@app.post("/orders")
def create_order(prescription_id: int):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                INSERT INTO orders (prescription_id, status)
                VALUES (:pid, 'CREATED')
                RETURNING id
            """),
            {"pid": prescription_id}
        )
        oid = result.fetchone()[0]
        conn.commit()
    return {"order_id": oid}


@app.post("/orders/{order_id}/confirm")
def confirm_order(
    order_id: int,
    idempotency_key: str | None = Header(default=None)
):
    with engine.connect() as conn:

        # 🔒 IDEMPOTENCY CHECK
        if idempotency_key:
            cached = check_idempotency(conn, idempotency_key, "confirm_order")
            if cached:
                return json.loads(cached)

        # 1️⃣ Ensure order exists
        order = conn.execute(
            text("""
                SELECT id, status
                FROM orders
                WHERE id = :oid
            """),
            {"oid": order_id}
        ).fetchone()

        if not order:
            return {"error": "Order not found"}

        if order.status == "CONFIRMED":
            return {"status": "already_confirmed"}

        # 2️⃣ Mark CONFIRMED (NO inventory action here)
        conn.execute(
            text("""
                UPDATE orders
                SET status='CONFIRMED',
                    confirmed_at = NOW()
                WHERE id = :oid
            """),
            {"oid": order_id}
        )


        response = {"status": "order_confirmed"}

        # 3️⃣ Save idempotency
        if idempotency_key:
            save_idempotency(conn, idempotency_key, "confirm_order", response)

        conn.commit()

    return response





@app.post("/orders/{prescription_id}/cancel")
def cancel_order(prescription_id: int):
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE orders SET status='CANCELLED' WHERE prescription_id=:pid"),
            {"pid": prescription_id}
        )

        rows = conn.execute(
            text("SELECT brand, quantity FROM prescription_items WHERE prescription_id=:pid"),
            {"pid": prescription_id}
        )

        items = [dict(r._mapping) for r in rows]
        conn.commit()

    publish_inventory_event({
        "type": "ORDER_CANCELLED",
        "prescription_id": prescription_id,
        "items": items
    })

    return {"status": "order_cancelled"}


@app.get("/prescriptions")
def list_prescriptions():
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM prescriptions ORDER BY id"))
        return [dict(r._mapping) for r in rows]


@app.get("/orders")
def list_orders():
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM orders"))
        return [dict(r._mapping) for r in rows]


@app.get("/health")
def health():
    return {"status": "order-prescription-service-up"}

@app.post("/prescriptions/{prescription_id}/approve")
def approve_prescription(
    prescription_id: int,
    user=Depends(require_role("PHARMACIST"))
):

    with engine.connect() as conn:
        # 1) mark validated
        conn.execute(
            text("""
                UPDATE prescriptions
                SET status = 'VALIDATED'
                WHERE id = :pid
            """),
            {"pid": prescription_id}
        )

        # 2) fetch items
        rows = conn.execute(
            text("""
                SELECT brand, quantity
                FROM prescription_items
                WHERE prescription_id = :pid
            """),
            {"pid": prescription_id}
        )

        items = [dict(r._mapping) for r in rows]
        conn.commit()

    # 3) reserve inventory
    publish_inventory_event({
        "type": "PRESCRIPTION_VALIDATED",
        "prescription_id": prescription_id,
        "items": items
    })

    return {"status": "approved_by_pharmacist"}


@app.post("/prescriptions/{prescription_id}/reject")
def reject_prescription(prescription_id: int):

    with engine.connect() as conn:
        conn.execute(
            text("""
                UPDATE prescriptions
                SET status = 'REJECTED'
                WHERE id = :pid
            """),
            {"pid": prescription_id}
        )
        conn.commit()

    return {"status": "rejected_by_pharmacist"}


# ADD THIS NEW ROUTE (replace old billing API)

@app.get("/orders/{order_id}/bill")
def generate_bill(
    order_id: int,
    idempotency_key: str = Header(None)
):
    with engine.connect() as conn:

        # 🔒 IDEMPOTENCY CHECK
        if idempotency_key:
            existing = check_idempotency(conn, idempotency_key, "generate_bill")
            if existing:
                return existing

        order = conn.execute(
            text("""
                SELECT prescription_id
                FROM orders
                WHERE id = :oid
            """),
            {"oid": order_id}
        ).fetchone()

        if not order:
            return {"error": "Order not found"}

        prescription_id = order[0]

        rows = conn.execute(
            text("""
                SELECT
                    m.brand_name,
                    pi.quantity,
                    mp.selling_price,
                    mp.gst_percent,
                    (pi.quantity * mp.selling_price) AS line_total,
                    (pi.quantity * mp.selling_price * mp.gst_percent / 100) AS gst_amount
                FROM prescription_items pi
                JOIN medicines m ON m.id = pi.medicine_id
                JOIN medicine_prices mp ON mp.medicine_id = m.id
                WHERE pi.prescription_id = :pid
            """),
            {"pid": prescription_id}
        ).fetchall()

        if not rows:
            return {"error": "No items found"}

        items = []
        subtotal = 0
        gst_total = 0

        for r in rows:
            line_total = float(r.line_total)
            gst_amount = float(r.gst_amount)

            items.append({
                "brand": r.brand_name,
                "quantity": r.quantity,
                "price": float(r.selling_price),
                "gst_percent": float(r.gst_percent),
                "line_total": line_total,
                "gst_amount": gst_amount,
                "final_line_total": line_total + gst_amount
            })

            subtotal += line_total
            gst_total += gst_amount

    response = {
        "order_id": order_id,
        "items": items,
        "subtotal": subtotal,
        "gst_total": gst_total,
        "grand_total": subtotal + gst_total
    }

    if idempotency_key:
        with engine.connect() as conn:
            save_idempotency(conn, idempotency_key, "generate_bill", response)
            conn.commit()

    return response


@app.post("/orders/{order_id}/return")
def return_items(order_id: int, items: List[ReturnItem]):

    with engine.connect() as conn:

        # 1️⃣ Check order exists
        order = conn.execute(
            text("""
                SELECT prescription_id, confirmed_at
                FROM orders
                WHERE id = :oid
            """),
            {"oid": order_id}
        ).fetchone()

        if not order:
            return {"error": "Order not found"}

        prescription_id, confirmed_at = order

        if not confirmed_at:
            return {"error": "Order not confirmed yet"}

        # 2️⃣ Check 14-day window
        within_window = conn.execute(
            text("""
                SELECT NOW() - :confirmed_at <= INTERVAL '14 days'
            """),
            {"confirmed_at": confirmed_at}
        ).scalar()

        if not within_window:
            return {"error": "Return window expired (14 days)"}

        # 3️⃣ Process return
        for item in items:
            mid = item.medicine_id
            qty = item.quantity
            reason = item.reason

            # Save return history
            conn.execute(
                text("""
                    INSERT INTO returns (order_id, medicine_id, quantity, reason)
                    VALUES (:oid, :mid, :qty, :reason)
                """),
                {
                    "oid": order_id,
                    "mid": mid,
                    "qty": qty,
                    "reason": reason
                }
            )

            # Restock
            conn.execute(
                text("""
                    UPDATE inventory
                    SET stock = stock + :qty
                    WHERE medicine_id = :mid
                """),
                {"mid": mid, "qty": qty}
            )

        conn.commit()

    return {"status": "return_processed"}


@app.post("/auth/login")
def login(username: str, password: str):

    with engine.connect() as conn:
        user = conn.execute(
            text("""
                SELECT username, password, role
                FROM users
                WHERE username=:u
            """),
            {"u": username}
        ).fetchone()

    if not user:
        raise HTTPException(status_code=401, detail="User not found")

    if user.password != password:
        raise HTTPException(status_code=401, detail="Wrong password")

    token = jwt.encode(
        {"sub": user.username, "role": user.role},
        SECRET_KEY,
        algorithm=ALGORITHM
    )

    return {
        "access_token": token,
        "role": user.role
    }
