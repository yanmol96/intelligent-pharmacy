import pika
import json
import threading
import time
import os
from fastapi import FastAPI
import pdfplumber
from sqlalchemy import create_engine, text
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password123"
)


RABBIT_HOST = "rabbitmq"
INPUT_QUEUE = "prescription_events"
OUTPUT_QUEUE = "ml_results"

app = FastAPI(title="ML Service (PDF Parser)")

# DB connection for interaction knowledge
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)


# ---------------- INTERACTION ENGINE ----------------

def check_interactions(drugs):
    found = []

    if len(drugs) < 2:
        return found

    with engine.connect() as conn:
        for i in range(len(drugs)):
            for j in range(i + 1, len(drugs)):
                d1 = drugs[i]
                d2 = drugs[j]

                result = conn.execute(
                    text("""
                        SELECT severity, description
                        FROM drug_interactions
                        WHERE (drug_a = :d1 AND drug_b = :d2)
                           OR (drug_a = :d2 AND drug_b = :d1)
                    """),
                    {"d1": d1, "d2": d2}
                ).fetchone()

                if result:
                    found.append({
                        "drug_1": d1,
                        "drug_2": d2,
                        "severity": result[0],
                        "description": result[1]
                    })

    return found


# ---------------- PDF TEXT EXTRACTION ----------------

def download_from_s3(s3_path: str) -> str:
    bucket, key = s3_path.replace("s3://", "").split("/", 1)

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password123",
    )

    local_path = f"/tmp/{key}"

    s3.download_file(bucket, key, local_path)

    return local_path


def extract_text_from_pdf(pdf_path: str) -> str:
    if not os.path.exists(pdf_path):
        return ""

    text_data = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_data.append(page_text)
    except Exception as e:
        print("PDF parsing error:", e)

    return "\n".join(text_data).strip()


# ---------------- FAKE ML PARSER ----------------

def fake_ml_parse(text: str):
    items = []

    known_drugs = ["paracetamol", "ibuprofen", "aspirin", "amoxicillin"]

    for d in known_drugs:
        if d in text.lower():
            items.append({
                "brand": d.capitalize(),
                "quantity": 6,              # conservative default
                "unit": "tablet",
                "quantity_source": "default"
            })

    return {
        "items": items
    }


# ---------------- EVENT PUBLISHER ----------------

def publish_result(result: dict):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBIT_HOST)
    )
    channel = connection.channel()
    channel.queue_declare(queue=OUTPUT_QUEUE, durable=True)

    channel.basic_publish(
        exchange="",
        routing_key=OUTPUT_QUEUE,
        body=json.dumps(result),
    )
    connection.close()


# ---------------- EVENT CONSUMER ----------------

def consume_events():
    print("🔥 ML CONSUMER THREAD STARTED")

    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBIT_HOST)
            )
            channel = connection.channel()
            channel.queue_declare(queue=INPUT_QUEUE, durable=True)

            def callback(ch, method, properties, body):
                event = json.loads(body)
                print("ML received event:", event)

                if event["type"] == "PRESCRIPTION_CREATED":
                    file_path = event["file_path"]

                    # Convert:
                    # s3://prescriptions/abc.pdf
                    # → abc.pdf
                    file_key = file_path.replace("s3://prescriptions/", "")


                    local_tmp = f"/tmp/{os.path.basename(file_key)}"

                    # Download from MinIO
                    s3.download_file("prescriptions", file_key, local_tmp)

                    text = extract_text_from_pdf(local_tmp)

                    parsed = fake_ml_parse(text)
                    items = parsed["items"]

                    # Extract drug names for interaction engine
                    drug_names = [i["brand"] for i in items]

                    interactions = check_interactions(drug_names)

                    result = {
                        "prescription_id": event["prescription_id"],
                        "extracted_text": text,
                        "items": items,
                        "interaction_found": len(interactions) > 0,
                        "interaction_details": interactions
                    }

                    print("ML result:", result)

                    publish_result(result)

                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(
                queue=INPUT_QUEUE,
                on_message_callback=callback,
                auto_ack=False
            )

            channel.start_consuming()

        except Exception as e:
            print("ML consumer error, retrying:", e)
            time.sleep(3)


# ---------------- STARTUP ----------------

@app.on_event("startup")
def start_consumer():
    print("🔥 ML CONSUMER THREAD STARTED", flush=True)
    thread = threading.Thread(target=consume_events, daemon=True)
    thread.start()


@app.get("/health")
def health():
    return {"status": "ml-service-up"}
