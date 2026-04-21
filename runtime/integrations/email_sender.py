"""
Email Sender — SMTP Integration for Autonomous Sales
"""
import os
import smtplib
import datetime
from email.message import EmailMessage
from email.utils import make_msgid, formatdate, formataddr
from config.logging_config import get_logger

logger = get_logger("email_sender")

# Environment variables
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER)
SENDING_DOMAIN = os.getenv("SENDING_DOMAIN", "reach.payflux.dev")

def send_outreach_email(to_email: str, subject: str, text_body: str) -> bool:
    """Send an outreach email to the discovered contact."""
    if not SMTP_USER or not SMTP_PASSWORD:
        logger.error("SMTP credentials missing. Set SMTP_USER and SMTP_PASSWORD.")
        return False

    try:
        msg = EmailMessage()
        msg.set_content(text_body)
        msg['Subject'] = subject
        msg['From'] = formataddr(("PayFlux", SMTP_FROM or SMTP_USER))
        msg['To'] = to_email
        msg['Reply-To'] = SMTP_FROM or SMTP_USER
        msg['List-Unsubscribe'] = f'<mailto:unsubscribe@{SENDING_DOMAIN}>'
        msg['List-Unsubscribe-Post'] = 'List-Unsubscribe=One-Click'
        msg['Message-ID'] = make_msgid(domain=SENDING_DOMAIN)
        msg['Date'] = formatdate(localtime=True)
        msg['X-Mailer'] = 'PayFlux/1.0'
        msg['Precedence'] = 'bulk'
        
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
            
        logger.info(f"Outreach email sent to {to_email}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email to {to_email}: {e}")
        return False
