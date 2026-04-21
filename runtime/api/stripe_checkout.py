import os
import stripe
from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import create_engine, text
from memory.structured.db import save_event, save_event_canonical, record_outcome
from config.logging_config import get_logger

logger = get_logger("stripe_checkout")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID")

SUCCESS_URL = os.getenv("STRIPE_SUCCESS_URL", "https://payflux.dev/success")
CANCEL_URL = os.getenv("STRIPE_CANCEL_URL", "https://payflux.dev/cancel")

router = APIRouter()


@router.post("/create_checkout_session")
async def create_checkout_session(request: Request):
    # Recovery mode: Stripe will retry; do not create sessions.
    from runtime.safety.recovery_mode import is_recovery_mode
    if is_recovery_mode():
        raise HTTPException(status_code=503, detail="recovery_mode")

    if not stripe.api_key:
        logger.error("STRIPE_SECRET_KEY not configured")
        raise HTTPException(status_code=500, detail="Payment system not configured")

    if not STRIPE_PRICE_ID:
        logger.error("STRIPE_PRICE_ID not configured")
        raise HTTPException(status_code=500, detail="Product not configured")

    email = None
    opportunity_id = None
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            body = await request.json()
            email = body.get("email")
            opportunity_id = body.get("opportunity_id")
        except Exception:
            pass

    try:
        # opportunity_id flows through to the webhook so the conversion can be
        # linked back to the originating decision in learning_feedback_ledger_raw.
        meta = {
            "checkoutOrigin": "agent_flux_outreach",
            "source": "agent_flux_outreach",
            "originSystem": "agent_flux",
        }
        if opportunity_id is not None:
            meta["opportunity_id"] = str(opportunity_id)

        session_params = {
            "mode": "subscription",
            "payment_method_types": ["card"],
            "line_items": [{"price": STRIPE_PRICE_ID, "quantity": 1}],
            "success_url": SUCCESS_URL + "?session_id={CHECKOUT_SESSION_ID}",
            "cancel_url": CANCEL_URL,
            "metadata": meta,
            "subscription_data": {"metadata": meta},
        }
        if email:
            session_params["customer_email"] = email

        session = stripe.checkout.Session.create(**session_params)

        save_event_canonical("checkout_session_created", {
            "session_id": session.id,
            "url": session.url,
        }, idempotency_key=f"stripe:checkout_session_created:{session.id}",
           subsystem="system_recovery")
        logger.info(f"Checkout session created: {session.id}")

        return {"checkout_url": session.url, "session_id": session.id}

    except stripe.error.StripeError as e:
        logger.error(f"Stripe error creating session: {e}")
        raise HTTPException(status_code=502, detail="Payment provider error")


@router.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    if not sig_header:
        raise HTTPException(status_code=400, detail="Missing signature")

    if not STRIPE_WEBHOOK_SECRET:
        logger.error("STRIPE_WEBHOOK_SECRET not configured")
        raise HTTPException(status_code=500, detail="Webhook not configured")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except ValueError:
        logger.warning("Invalid webhook payload")
        raise HTTPException(status_code=400, detail="Invalid payload")
    except stripe.error.SignatureVerificationError:
        logger.warning("Invalid webhook signature")
        raise HTTPException(status_code=400, detail="Invalid signature")

    # Recovery mode: ack with 503 so Stripe retries after exit.
    from runtime.safety.recovery_mode import is_recovery_mode
    if is_recovery_mode():
        raise HTTPException(status_code=503, detail="recovery_mode")

    # Hard idempotency on Stripe event id (requires migration 007).
    with engine.connect() as conn:
        first_seen = conn.execute(text("""
            INSERT INTO processed_stripe_events (event_id, event_type)
            VALUES (:id, :t)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
        """), {"id": event["id"], "t": event["type"]}).fetchone()
        conn.commit()
        if not first_seen:
            logger.info(f"Stripe duplicate suppressed: {event['id']}")
            return {"status": "ok", "duplicate": True}

        # ── ALL DOWNSTREAM WRITES INSIDE THIS BLOCK ──
        # This fixes the closed-connection bug where conversion tracking
        # ran after the `with` block exited (conn was already returned to pool).

        if event["type"] == "checkout.session.completed":
            session = event["data"]["object"]
            customer_email = (session.get("customer_details") or {}).get("email")
            stripe_customer_id = session.get("customer")
            session_id = session.get("id")

            logger.info(f"Checkout completed: {customer_email} (session: {session_id})")

            conn.execute(text("""
                INSERT INTO customers (email, stripe_customer_id, stripe_session_id, subscription_status)
                VALUES (:email, :cust_id, :sess_id, 'active')
                ON CONFLICT (stripe_session_id) DO NOTHING
            """), {
                "email": customer_email or "unknown",
                "cust_id": stripe_customer_id,
                "sess_id": session_id,
            })

            # Conversion tracking — now inside the same connection block
            conn.execute(text("""
                UPDATE conversion_opportunities
                SET status = 'converted'
                WHERE checkout_session_id = :sid AND status != 'converted'
            """), {"sid": session_id})

            conn.execute(text("""
                UPDATE merchant_opportunities
                SET status = 'converted'
                WHERE checkout_url LIKE :pat AND status != 'converted'
            """), {"pat": f"%{session_id}%"})
            conn.commit()

            save_event_canonical(
                "customer_converted",
                {"session_id": session_id, "email": customer_email,
                 "stripe_customer_id": stripe_customer_id},
                idempotency_key=f"stripe:customer_converted:{session_id}",
                subsystem="system_recovery",
            )

            # Close the learning loop: link the conversion outcome back to the
            # originating decision row in learning_feedback_ledger_raw.
            opp_id_meta = (session.get("metadata") or {}).get("opportunity_id")
            if opp_id_meta:
                try:
                    amount_cents = session.get("amount_total") or 0
                    record_outcome(
                        opportunity_id=int(opp_id_meta),
                        outcome_type="stripe_conversion",
                        outcome_value=float(amount_cents) / 100.0,
                        outcome_confidence=1.0,
                        reward_score=1.0,
                        notes=f"stripe_session={session_id}",
                        hook_name="stripe_checkout_webhook",
                    )
                except Exception as outcome_err:
                    logger.warning(f"record_outcome (stripe_conversion) failed: {outcome_err}")

            try:
                from runtime.ops.alerts import send_operator_alert
                send_operator_alert(
                    f"💰 New PayFlux Customer Converted!\n"
                    f"Email: {customer_email}\nSession: {session_id}\n"
                    f"Revenue: $499/month activated."
                )
            except Exception as alert_err:
                logger.warning(f"Operator alert failed: {alert_err}")

        elif event["type"] == "customer.subscription.deleted":
            sub = event["data"]["object"]
            stripe_customer_id = sub.get("customer")
            conn.execute(text("""
                UPDATE customers SET subscription_status = 'cancelled'
                WHERE stripe_customer_id = :cid
            """), {"cid": stripe_customer_id})
            conn.commit()

            save_event_canonical(
                "subscription_cancelled",
                {"stripe_customer_id": stripe_customer_id},
                idempotency_key=f"stripe:subscription_cancelled:{event['id']}",
                subsystem="system_recovery",
            )
            logger.info(f"Subscription cancelled: {stripe_customer_id}")

        else:
            logger.info(f"Unhandled webhook event type: {event['type']}")

    return {"status": "ok"}
