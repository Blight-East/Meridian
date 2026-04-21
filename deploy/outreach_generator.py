"""
Outreach Draft Generator
Generates personalized email drafts for merchant outreach.
"""


def generate_outreach(merchant_name, processor, domain, topic, checkout_url):
    """
    Generate a personalized outreach email draft.
    No LLM needed — uses battle-tested template with dynamic insertion.
    """
    subject = f"Quick solution for {processor} payout issues"

    if processor and processor != "unknown":
        processor_line = f"I noticed several merchants reporting {topic.lower()} with {processor}."
    else:
        processor_line = f"I noticed several merchants reporting {topic.lower()} issues."

    body = f"""Subject: {subject}

Hi {merchant_name},

{processor_line}

We built PayFlux to give merchants reserve transparency and payout forecasting so these disruptions don't kill cash flow.

If you're exploring alternatives or want better visibility into your reserves, here's a quick overview:

{checkout_url}

Happy to answer any questions — we've helped several merchants in similar situations.

Best,
PayFlux Team
"""
    return body.strip()
