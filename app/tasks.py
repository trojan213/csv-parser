from celery import Celery
from app import database
from sqlalchemy import text
import csv, os, io, logging

REDIS_URL = os.getenv("REDIS_URL")

celery_app = Celery("tasks", broker=REDIS_URL)

celery_app.conf.update(
    task_serializer="pickle",
    accept_content=["pickle"]
)

BATCH_SIZE = 2000

logging.basicConfig(level=logging.INFO)

@celery_app.task(bind=True, name="app.tasks.import_products")
def import_products(self, csv_bytes: bytes):
    logging.info("TASK STARTED")
    db = database.SessionLocal()

    try:
        # Decode CSV
        f = io.StringIO(csv_bytes.decode("utf-8"))
        rows = list(csv.DictReader(f))
        total = len(rows)
        processed = 0
        batch = []

        logging.info("CSV RECEIVED: %d rows", total)

        # Initialize progress row
        db.execute(text("""
            INSERT INTO task_progress(task_id, current, total, state)
            VALUES (:id, 0, :t, 'STARTED')
            ON CONFLICT (task_id) DO UPDATE
            SET current=0, total=:t, state='STARTED'
        """), {"id": self.request.id, "t": total})
        db.commit()

        for row in rows:
            processed += 1

            batch.append({
                "sku": row["sku"].strip().lower(),
                "name": row["name"].strip(),
                "description": (row.get("description") or "").strip(),
                "active": True
            })

            # Insert batch
            if len(batch) == BATCH_SIZE:
                db.execute(text("""
                    INSERT INTO products (sku, name, description, active)
                    VALUES (:sku, :name, :description, :active)
                    ON CONFLICT (sku) DO UPDATE
                    SET name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        active = EXCLUDED.active
                """), batch)
                db.commit()
                batch.clear()
                logging.info("Inserted %d rows", processed)

            # Update progress
            if processed % 500 == 0 or processed == total:
                db.execute(text("""
                    UPDATE task_progress
                    SET current=:c, state='PROGRESS', updated_at=now()
                    WHERE task_id=:id
                """), {"c": processed, "id": self.request.id})
                db.commit()

        # Flush remainder
        if batch:
            db.execute(text("""
                INSERT INTO products (sku, name, description, active)
                VALUES (:sku, :name, :description, :active)
                ON CONFLICT (sku) DO UPDATE
                SET name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    active = EXCLUDED.active
            """), batch)
            db.commit()

        # Mark success
        db.execute(text("""
            UPDATE task_progress
            SET current=:t, state='SUCCESS', updated_at=now()
            WHERE task_id=:id
        """), {"t": total, "id": self.request.id})
        db.commit()

        logging.info("IMPORT COMPLETE")

        return "DONE"

    except Exception as e:
        logging.error("IMPORT FAILED", exc_info=True)
        db.execute(text("""
            UPDATE task_progress
            SET state='FAILED', updated_at=now()
            WHERE task_id=:id
        """), {"id": self.request.id})
        db.commit()
        raise

    finally:
        db.close()


if __name__ == "__main__":
    celery_app.worker_main()
