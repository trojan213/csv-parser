from celery import Celery
from app import database
from sqlalchemy import text
import csv
import os
import io

REDIS_URL = os.getenv("REDIS_URL")

celery_app = Celery(
    "tasks",
    broker=REDIS_URL,
    backend=REDIS_URL
)

celery_app.conf.update(
    task_track_started=True,
    result_extended=True,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
)

BATCH_SIZE = 2000


@celery_app.task(bind=True, name="app.tasks.import_products")
def import_products(self, csv_bytes: bytes):
    db = database.SessionLocal()

    try:
        f = io.StringIO(csv_bytes.decode("utf-8"))
        reader = list(csv.DictReader(f))
        total = len(reader)
        processed = 0
        batch = []

        for row in reader:
            processed += 1

            batch.append({
                "sku": row["sku"].strip().lower(),
                "name": row["name"].strip(),
                "description": (row.get("description") or "").strip(),
                "active": True
            })

            if len(batch) == BATCH_SIZE:
                db.execute(text("""
                    INSERT INTO products (sku, name, description, active)
                    VALUES (:sku, :name, :description, :active)
                    ON CONFLICT (sku) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        active = EXCLUDED.active
                """), batch)
                db.commit()
                batch.clear()

            if processed % 500 == 0 or processed == total:
                self.update_state(
                    state="PROGRESS",
                    meta={"current": processed, "total": total}
                )

        if batch:
            db.execute(text("""
                INSERT INTO products (sku, name, description, active)
                VALUES (:sku, :name, :description, :active)
                ON CONFLICT (sku) DO UPDATE
                SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    active = EXCLUDED.active
            """), batch)
            db.commit()

        return {"current": processed, "total": total}

    finally:
        db.close()
