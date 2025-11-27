import requests
from datetime import datetime
from app.database import SessionLocal
from app.models import Webhook


def trigger_event(event: str, data: dict):
    payload = {
        "event": event,
        "timestamp": datetime.utcnow().isoformat(),
        "data": data
    }

    db = SessionLocal()
    hooks = db.query(Webhook).filter(Webhook.event == event, Webhook.enabled == True).all()
    db.close()

    for hook in hooks:
        try:
            requests.post(hook.url, json=payload, timeout=3)
        except:
            pass   
