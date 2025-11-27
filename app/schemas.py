from pydantic import BaseModel
from pydantic import BaseModel, HttpUrl

class ProductBase(BaseModel):
    sku: str
    name: str
    description: str | None = None
    active: bool = True

class ProductCreate(ProductBase):
    pass

class ProductUpdate(ProductBase):
    pass

class Product(ProductBase):
    id: int

    class Config:
        from_attributes = True

class WebhookCreate(BaseModel):
    url: HttpUrl
    event: str
    enabled: bool = True

class WebhookUpdate(BaseModel):
    url: HttpUrl | None = None
    event: str | None = None
    enabled: bool | None = None