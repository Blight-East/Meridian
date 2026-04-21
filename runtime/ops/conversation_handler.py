"""
Merchant Conversation Reply Handler
Simulates or processes incoming messages from merchants, parses intent, and auto-replies via the logic engine.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from integrations.email_sender import send_outreach_email
from conversation.chat_engine import generate_chat_response

logger = get_logger("conversation_handler")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def handle_reply(merchant_id: int, contact_email: str, incoming_message: str):
    """
    Process an inbound message from a targeted merchant.
    Appends the message to the conversation ledger, queries the LLM for a context-aware response,
    then executes the SMTP deployment of the follow-up response.
    """
    logger.info(f"Processing inbound message from {contact_email} (Merchant #{merchant_id}).")
    
    with engine.connect() as conn:
        # 1. Store the incoming message
        conn.execute(text("""
            INSERT INTO merchant_conversations (merchant_id, contact_email, message, direction)
            VALUES (:mid, :email, :msg, 'received')
        """), {
            "mid": merchant_id,
            "email": contact_email,
            "msg": incoming_message
        })
        conn.commit()

        # 2. Extract recent conversation context to feed into reasoning
        history = conn.execute(text("""
            SELECT message, direction FROM merchant_conversations
            WHERE merchant_id = :mid AND contact_email = :email
            ORDER BY timestamp DESC LIMIT 5
        """), {"mid": merchant_id, "email": contact_email}).fetchall()
        
        # 3. Format history for the LLM
        formatted_context = "CONVERSATION HISTORY:\n\n"
        # Reading backwards so chronological order is top-down
        for msg, direction in reversed(history):
            role = "AGENT" if direction == "sent" else "MERCHANT"
            formatted_context += f"--- {role} ---\n{msg}\n\n"
            
        # Get primary context around the merchant domain to provide deep system intelligence
        merchant_intel = conn.execute(text("""
            SELECT domain, processor, distress_score FROM merchants WHERE id = :mid
        """), {"mid": merchant_id}).fetchone()
        
        domain = merchant_intel[0] if merchant_intel else "Unknown"
        processor = merchant_intel[1] if merchant_intel else "Unknown"
        score = merchant_intel[2] if merchant_intel else 0
        
    SYSTEM_FOLLOW_UP_PROMPT = f"""
    You are Agent Flux, handling a sales conversation regarding {domain}.
    Their payment processor is {processor} and their vulnerability score is {score}.
    
    Your goal is to answer their objections calmly, professionally, and authoritatively, leading them to subscribe via the checkout link provided previously. 
    Keep it extremely concise (2-3 sentences max). Respond directly to the following context:
    
    {formatted_context}
    
    INBOUND MESSAGE: {incoming_message}
    """
    
    # 4. Generate the reply utilizing our internal chat backbone
    # Using a fake user ID for background autonomous routines "SALES_AGENT"
    try:
        reply_draft = generate_chat_response("SALES_AGENT", SYSTEM_FOLLOW_UP_PROMPT, max_tokens=300)
    except Exception as e:
        logger.error(f"Failed to generate reasoning logic for reply: {e}")
        return False
        
    # 5. Execute Outreach
    subject = f"Re: Fixing {processor} issues for {domain}"
    success = send_outreach_email(contact_email, subject, reply_draft)
    
    if success:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO merchant_conversations (merchant_id, contact_email, message, direction)
                VALUES (:mid, :email, :msg, 'sent')
            """), {
                "mid": merchant_id,
                "email": contact_email,
                "msg": reply_draft
            })
            conn.commit()
        logger.info(f"Successfully auto-replied to {contact_email}.")
        return True
    else:
        logger.warning(f"Failed to transmit the generated response to {contact_email}.")
        return False

# Manual test hook block
if __name__ == "__main__":
    if len(sys.argv) == 4:
        # Example usage: python3 conversation_handler.py 23 contact@merchant.com "How much does it cost?"
        handle_reply(int(sys.argv[1]), sys.argv[2], sys.argv[3])
