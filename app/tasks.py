# app/tasks.py
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
    task_store_errors_even_if_ignored=True,
    result_serializer="json",
    accept_content=["json"],
    task_serializer="json",
)
celery_app.conf.task_ignore_result = False

BATCH_SIZE = 2000

@celery_app.task(bind=True, name="app.tasks.import_products")
def import_products(self, csv_bytes: bytes):

    db = database.SessionLocal()

    try:
        # Read CSV from memory (no filesystem)
        text_data = csv_bytes.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text_data))

        rows = list(reader)
        total = len(rows)

        processed = 0
        batch = []

        for row in rows:
            processed += 1

            batch.append({
                "sku": row["sku"].strip().lower(),
                "name": row["name"].strip(),
                "description": (row.get("description") or "").strip(),
                "active": True
            })

            # Flush batch
            if len(batch) == BATCH_SIZE:
                db.execute(text("""
                    INSERT INTO products (sku, name, description, active)
                    VALUES (:sku, :name, :description, :active)
                    ON CONFLICT (sku)
                    DO UPDATE SET
                      name = EXCLUDED.name,
                      description = EXCLUDED.description,
                      active = EXCLUDED.active
                """), batch)

                db.commit()
                batch.clear()

            # Emit progress every 500 rows
            if processed % 500 == 0 or processed == total:
                self.update_state(
                    state="PROGRESS",
                    meta={"current": processed, "total": total}
                )

        if batch:
            db.execute(text("""
                INSERT INTO products (sku, name, description, active)
                VALUES (:sku, :name, :description, :active)
                ON CONFLICT (sku)
                DO UPDATE SET
                  name = EXCLUDED.name,
                  description = EXCLUDED.description,
                  active = EXCLUDED.active
            """), batch)
            db.commit()

        return {"current": processed, "total": total}

    finally:
        db.close()


if __name__ == "__main__":
    celery_app.worker_main()
