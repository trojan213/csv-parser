# app/tasks.py
from celery import Celery
from app import database
from sqlalchemy import text
import csv

celery_app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)

BATCH_SIZE = 2000   # 2k rows per batch works well for PostgreSQL on Windows

@celery_app.task(bind=True)
def import_products(self, csv_file_path: str):
    db = database.SessionLocal()

    try:
        with open(csv_file_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            total = len(rows)

            batch = []
            processed = 0

            for i, r in enumerate(rows, start=1):
                batch.append({
                    "sku": r["sku"].strip().lower(),
                    "name": r["name"].strip(),
                    "description": (r.get("description") or "").strip(),
                    "active": True
                })

                # Flush batch
                if len(batch) == BATCH_SIZE or i == total:
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

                # Update progress every 500 rows (keeps UI responsive)
                if i % 500 == 0 or i == total:
                    self.update_state(
                        state="PROGRESS",
                        meta={"current": i, "total": total}
                    )

            return {"current": total, "total": total, "status": "Completed"}

    finally:
        db.close()

if __name__ == "__main__":
    celery_app.worker_main()
