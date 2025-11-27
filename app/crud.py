from sqlalchemy.orm import Session
from . import models, schemas
from app.models import Webhook

def get_product_by_sku(db: Session, sku: str):
    return db.query(models.Product).filter(models.Product.sku == sku).first()

def create_product(db: Session, product: schemas.ProductCreate):
    obj = models.Product(**product.dict())
    db.add(obj)
    return obj

def update_product(db: Session, sku: str, product: schemas.ProductUpdate):
    obj = get_product_by_sku(db, sku)
    if obj:
        for k, v in product.dict().items():
            setattr(obj, k, v)
    return obj

def delete_product(db: Session, sku: str):
    obj = get_product_by_sku(db, sku)
    if obj:
        db.delete(obj)
    return obj

def bulk_delete(db: Session):
    db.query(models.Product).delete()

def create_webhook(db, webhook):
    w = Webhook(**webhook.dict())
    db.add(w)
    db.commit()
    db.refresh(w)
    return w

def get_webhooks(db):
    return db.query(Webhook).all()

def update_webhook(db, id, data):
    w = db.query(Webhook).filter(Webhook.id == id).first()
    if not w:
        return None

    for k, v in data.dict(exclude_unset=True).items():
        setattr(w, k, v)

    db.commit()
    return w

def delete_webhook(db, id):
    w = db.query(Webhook).filter(Webhook.id == id).first()
    if w:
        db.delete(w)
        db.commit()

    return w
