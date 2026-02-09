from fastapi import FastAPI
from sqlalchemy import create_engine, text
import os
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import redis
import json

REDIS_HOST = os.getenv("REDIS_HOST", "redis")

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=True
)

app = FastAPI(title="Pharmacy Service")

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, echo=True)

def wait_for_db(retries=10, delay=2):
    for i in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                return
        except OperationalError:
            print(f"DB not ready, retrying ({i+1}/{retries})...")
            time.sleep(delay)
    raise RuntimeError("Database not available")

@app.on_event("startup")
def startup():
    wait_for_db()

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS medicines (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                manufacturer TEXT
            );
        """))
        conn.commit()

@app.get("/medicines/search")
def search_medicine(q: str):
    cache_key = f"med_search:{q}"

    # 1) Try cache
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    # 2) Query DB
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT *
                FROM medicines
                WHERE brand_name ILIKE :q
                LIMIT 10
            """),
            {"q": f"%{q}%"}
        )

        data = [dict(r._mapping) for r in rows]

    # 3) Save to Redis (60 sec)
    redis_client.setex(cache_key, 60, json.dumps(data))

    return data


@app.get("/health")
def health():
    return {"status": "pharmacy-service-up"}

@app.post("/medicines")
def add_medicine(name: str, manufacturer: str | None = None):
    with engine.connect() as conn:
        conn.execute(
            text("INSERT INTO medicines (name, manufacturer) VALUES (:n, :m)"),
            {"n": name, "m": manufacturer},
        )
        conn.commit()
    return {"message": "medicine added"}

@app.get("/medicines")
def list_medicines():
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM medicines ORDER BY brand_name"))
        return [dict(r._mapping) for r in rows]

