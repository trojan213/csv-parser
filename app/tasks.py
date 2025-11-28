from celery import Celery
from app import database
from sqlalchemy import text
import csv, os, io

REDIS_URL = os.getenv("REDIS_URL")

celery_app = Celery("tasks", broker=REDIS_URL)

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

        # Insert initial state
        db.execute(text("""
            INSERT INTO task_progress(task_id, current, total, state)
            VALUES (:id, 0, :total, 'STARTED')
            ON CONFLICT (task_id) DO NOTHING
        """), {"id": self.request.id, "total": total})
        db.commit()

        for row in reader:
            processed += 1

            batch.append({
                "sku": row["sku"].strip().lower(),
                "name": row["name"].strip(),
                "description": (row.get("description") or "").strip(),
                "active": True
            })

            if len(batch) == BATCH_SIZE:
                db.execute(text("""INSERT INTO products VALUES
                (:sku,:name,:description,:active)
                ON CONFLICT (sku) DO UPDATE
                SET name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    active = EXCLUDED.active"""), batch)
                db.commit()
                batch.clear()

            if processed % 500 == 0 or processed == total:
                db.execute(text("""
                    UPDATE task_progress
                    SET current=:c, total=:t, state='PROGRESS', updated_at=now()
                    WHERE task_id=:id
                """), {"c": processed, "t": total, "id": self.request.id})
                db.commit()

        if batch:
            db.execute(text("""INSERT INTO products VALUES
            (:sku,:name,:description,:active)
            ON CONFLICT (sku) DO UPDATE
            SET name = EXCLUDED.name,
                description = EXCLUDED.description,
                active = EXCLUDED.active"""), batch)
            db.commit()

        db.execute(text("""
            UPDATE task_progress
            SET current=:t, total=:t, state='SUCCESS'
            WHERE task_id=:id
        """), {"t": total, "id": self.request.id})
        db.commit()

        return "DONE"

    except Exception:
        db.execute(text("""
            UPDATE task_progress SET state='FAILED'
            WHERE task_id=:id
        """), {"id": self.request.id})
        db.commit()
        raise

    finally:
        db.close()
