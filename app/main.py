from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from app import database
from app.tasks import import_products
import base64

app = FastAPI()

templates = Jinja2Templates(directory="app/templates")


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    content = await file.read()
    encoded = base64.b64encode(content).decode("utf-8")
    print("ADDING Task")
    task = import_products.delay(encoded)
    return {"task_id": task.id}


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    db = database.SessionLocal()
    row = db.execute(text("""
        SELECT current, total, state
        FROM task_progress
        WHERE task_id = :id
    """), {"id": task_id}).fetchone()
    db.close()

    if not row:
        return {"state": "PENDING"}

    return {
        "state": row.state,
        "meta": {
            "current": row.current,
            "total": row.total
        }
    }


# --------------------
# PRODUCTS API
# --------------------

@app.get("/products")
def list_products(page: int = 1, size: int = 20, q: str = "", active: str = ""):
    db = database.SessionLocal()

    where = []
    params = {"limit": size, "offset": (page-1)*size}

    if q:
        where.append("(name ILIKE :q OR sku ILIKE :q)")
        params["q"] = f"%{q}%"

    if active:
        where.append("active = :a")
        params["a"] = active.lower() == "true"

    query = "SELECT * FROM products"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY sku LIMIT :limit OFFSET :offset"

    items = db.execute(text(query), params).fetchall()
    db.close()

    return {"items": [dict(r) for r in items]}


@app.post("/products")
def create_product(product: dict):
    db = database.SessionLocal()
    db.execute(text("""
        INSERT INTO products (sku, name, description, active)
        VALUES (:sku, :name, :description, :active)
        ON CONFLICT (sku) DO NOTHING
    """), product)
    db.commit()
    db.close()
    return {"status": "created"}


@app.delete("/products/{sku}")
def delete_product(sku: str):
    db = database.SessionLocal()
    db.execute(text("DELETE FROM products WHERE sku=:s"), {"s": sku})
    db.commit()
    db.close()
    return {"status": "deleted"}


@app.delete("/products/delete-all")
def delete_all():
    db = database.SessionLocal()
    db.execute(text("DELETE FROM products"))
    db.commit()
    db.close()
    return {"status": "all deleted"}


# --------------------
# WEBHOOK API
# --------------------

@app.get("/webhooks")
def list_webhooks():
    db = database.SessionLocal()
    rows = db.execute(text("SELECT * FROM webhooks ORDER BY id")).fetchall()
    db.close()
    return [dict(r) for r in rows]


@app.post("/webhooks")
def create_webhook(data: dict):
    db = database.SessionLocal()
    db.execute(text("""
        INSERT INTO webhooks(url, event, enabled)
        VALUES (:url, :event, :enabled)
    """), data)
    db.commit()
    db.close()
    return {"status": "created"}


@app.put("/webhooks/{id}")
def toggle_webhook(id: int, data: dict):
    db = database.SessionLocal()
    db.execute(text("""
        UPDATE webhooks SET enabled=:enabled WHERE id=:id
    """), {"id": id, "enabled": data["enabled"]})
    db.commit()
    db.close()
    return {"status": "updated"}


@app.delete("/webhooks/{id}")
def delete_webhook(id: int):
    db = database.SessionLocal()
    db.execute(text("DELETE FROM webhooks WHERE id=:id"), {"id": id})
    db.commit()
    db.close()
    return {"status": "deleted"}


@app.post("/webhooks/test/{id}")
def test_webhook(id: int):
    db = database.SessionLocal()
    row = db.execute(text("SELECT url FROM webhooks WHERE id=:id"), {"id": id}).fetchone()
    db.close()

    if not row:
        return {"code": "NOT_FOUND"}

    import requests
    try:
        r = requests.post(row.url, json={"test": True}, timeout=5)
        return {"code": r.status_code}
    except Exception:
        return {"code": "FAILED"}
