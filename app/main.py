from sqlalchemy import text
from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
from sqlalchemy import or_
from celery.result import AsyncResult
from pathlib import Path

from app.schemas import ProductCreate, ProductUpdate, WebhookCreate, WebhookUpdate
from app.models import Webhook
from app.webhooks import trigger_event
from app.tasks import import_products, celery_app
from app.database import Base, engine, SessionLocal
from app import models
 
print("..1.1. FASTAPI BOOTING")


app = FastAPI()


@app.on_event("startup")
def startup_db():
    Base.metadata.create_all(bind=engine)


templates = Jinja2Templates(directory="app/templates")

UPLOAD_DIR = Path("/tmp/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
 

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    path = UPLOAD_DIR / file.filename

    with open(path, "wb") as out:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)

    task = import_products.delay(str(path))
    return {"task_id": task.id}


@app.get("/tasks/{task_id}")
def task_status(task_id: str):
    task = AsyncResult(task_id, app=celery_app)
    return {"state": task.state, "meta": task.info}
 
@app.get("/products")
def list_products(
    page: int = Query(1, ge=1),
    size: int = Query(20, le=100),
    q: str | None = None,
    active: bool | None = None
):
    db = SessionLocal()
    query = db.query(models.Product)

    if q:
        query = query.filter(
            or_(
                models.Product.sku.ilike(f"%{q}%"),
                models.Product.name.ilike(f"%{q}%"),
                models.Product.description.ilike(f"%{q}%")
            )
        )

    if active is not None:
        query = query.filter(models.Product.active == active)

    total = query.count()
    items = query.offset((page - 1) * size).limit(size).all()

    db.close()
    return {"items": items, "total": total}


@app.post("/products")
def create_product(product: ProductCreate):
    db = SessionLocal()

    item = models.Product(**product.dict())
    db.add(item)
    db.commit()
    db.refresh(item)
    db.close()

    trigger_event("product.created", {
        "sku": item.sku,
        "name": item.name,
        "description": item.description,
        "active": item.active
    })

    return item


@app.put("/products/{sku}")
def update_product(sku: str, product: ProductUpdate):
    db = SessionLocal()
    item = db.query(models.Product).filter(models.Product.sku == sku.lower()).first()

    if not item:
        db.close()
        return {"error": "not found"}

    for k, v in product.dict(exclude_unset=True).items():
        setattr(item, k, v)

    db.commit()
    db.refresh(item)
    db.close()

    trigger_event("product.updated", {
        "sku": item.sku,
        "name": item.name,
        "description": item.description,
        "active": item.active
    })

    return item


@app.delete("/products/{sku}")
def delete_product(sku: str):
    db = SessionLocal()
    item = db.query(models.Product).filter(models.Product.sku == sku.lower()).first()

    if not item:
        db.close()
        return {"error": "not found"}

    db.delete(item)
    db.commit()
    db.close()

    trigger_event("product.deleted", {"sku": sku})
    return {"deleted": sku}


@app.delete("/products/delete-all")
def delete_all_products():
    db = SessionLocal()

    db.execute(text("DELETE FROM products"))
    db.execute(text("ALTER SEQUENCE products_id_seq RESTART WITH 1"))
    db.commit()
    db.close()

    trigger_event("bulk.deleted", {})
    return {"status": "all deleted"}

@app.get("/webhooks")
def list_webhooks():
    db = SessionLocal()
    data = db.query(Webhook).all()
    db.close()
    return data


@app.post("/webhooks")
def add_webhook(hook: WebhookCreate):
    db = SessionLocal()
    w = Webhook(**hook.dict())
    db.add(w)
    db.commit()
    db.refresh(w)
    db.close()
    return w


@app.put("/webhooks/{id}")
def update_webhook(id: int, hook: WebhookUpdate):
    db = SessionLocal()
    w = db.query(Webhook).filter(Webhook.id == id).first()

    if not w:
        db.close()
        return {"error": "not found"}

    for k, v in hook.dict(exclude_unset=True).items():
        setattr(w, k, v)

    db.commit()
    db.refresh(w)
    db.close()
    return w


@app.delete("/webhooks/{id}")
def delete_webhook(id: int):
    db = SessionLocal()
    w = db.query(Webhook).filter(Webhook.id == id).first()

    if not w:
        db.close()
        return {"error": "not found"}

    db.delete(w)
    db.commit()
    db.close()
    return {"deleted": id}


@app.post("/webhooks/test/{id}")
def test_webhook(id: int):
    import requests
    db = SessionLocal()
    w = db.query(Webhook).filter(Webhook.id == id).first()
    db.close()

    if not w:
        return {"error": "not found"}

    r = requests.post(w.url, json={"test": True})
    return {"status": "sent", "code": r.status_code}
