import os, requests
from memory.structured.db import save_event
from config.logging_config import get_logger
from safety.guard import resolve_operator_delivery_chat

logger = get_logger("operator_alerts")

def send_operator_alert(message_text):
    token = os.environ.get("TELEGRAM_TOKEN")
    allowed = os.environ.get("ALLOWED_OPERATOR_IDS", "")
    if not token or not allowed:
        logger.error("Missing TELEGRAM_TOKEN or ALLOWED_OPERATOR_IDS for alert")
        return
    
    users = [u.strip() for u in allowed.split(",") if u.strip()]
    for uid in users:
        chat_id = resolve_operator_delivery_chat(uid)
        if not chat_id:
            logger.warning(f"Skipping alert for {uid}: no trusted operator chat is bound")
            continue
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        try:
            res = requests.post(url, json={"chat_id": chat_id, "text": message_text}, timeout=10)
            if res.status_code == 200:
                logger.info(f"Alert sent to {uid} via chat {chat_id}")
                save_event("alert_sent", {"user_id": uid, "chat_id": str(chat_id), "preview": message_text[:50]})
        except Exception as e:
            logger.error(f"Failed alert to {uid}: {e}")
