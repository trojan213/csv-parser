from sqlalchemy import Column, Integer, String, Boolean, Index
from app.database import Base
from sqlalchemy import Column, Integer, String, Boolean

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    sku = Column(String, nullable=False)
    name = Column(String)
    description = Column(String)
    active = Column(Boolean, default=True)

    __table_args__ = (
        Index("ix_sku_unique", "sku", unique=True, postgresql_using="btree"),
    )

class Webhook(Base):
    __tablename__ = "webhooks"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(String, nullable=False)
    event = Column(String, nullable=False)  
    enabled = Column(Boolean, default=True)