# app/tasks.py
from celery import Celery
from app import database
from sqlalchemy import text
import csv
import os

REDIS_URL=os.getenv("REDIS_URL")

celery_app = Celery(
    "tasks",
    broker=REDIS_URL,
    backend=REDIS_URL
)

BATCH_SIZE = 2000  

@celery_app.task(bind=True)
def import_products(self, csv_file_path: str):
    db = database.SessionLocal()

    try:
        # First pass: count rows
        with open(csv_file_path, newline='', encoding="utf-8") as f:
            total = sum(1 for _ in f) - 1  # remove header row

        processed = 0
        batch = []

        # Second pass: process rows
        with open(csv_file_path, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                processed += 1

                batch.append({
                    "sku": row["sku"].strip().lower(),
                    "name": row["name"].strip(),
                    "description": (row.get("description") or "").strip(),
                    "active": True
                })

                # Write batch
                if len(batch) == BATCH_SIZE:
                    db.execute(text("""
                        INSERT INTO products (sku, name, description, active)
                        VALUES (:sku, :name, :description, :active)
                        ON CONFLICT (sku)
                        DO UPDATE
                        SET
                          name = EXCLUDED.name,
                          description = EXCLUDED.description,
                          active = EXCLUDED.active
                    """), batch)
                    db.commit()
                    batch.clear()

                # Report progress every 500 rows
                if processed % 500 == 0 or processed == total:
                    self.update_state(
                        state="PROGRESS",
                        meta={"current": processed, "total": total}
                    )

            # Flush remaining
            if batch:
                db.execute(text("""
                    INSERT INTO products (sku, name, description, active)
                    VALUES (:sku, :name, :description, :active)
                    ON CONFLICT (sku)
                    DO UPDATE
                    SET
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
