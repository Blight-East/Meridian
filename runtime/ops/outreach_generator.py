"""
Outreach Draft Generator
Generates personalized email drafts for merchant outreach.
"""


def generate_outreach(merchant_name, processor, domain, topic, checkout_url):
    """
    Generate a personalized outreach email draft.
    No LLM needed — uses battle-tested template with dynamic insertion.
    """
    subject = f"Processor pressure at {merchant_name or domain}"

    if processor and processor != "unknown":
        processor_line = f"I’m reaching out because the signal around {merchant_name or domain} suggests {topic.lower()} with {processor} may be active."
    else:
        processor_line = f"I’m reaching out because the signal around {merchant_name or domain} suggests {topic.lower()} may be active."

    body = f"""Subject: {subject}

Hi {merchant_name},

{processor_line}

PayFlux is built for merchants who need to see whether processor pressure is getting worse, what changed, and what to do next before it turns into a cash-flow problem.

Two quick questions so I do not guess:
1. Is the issue mainly payout delay, reserve / review pressure, or full processing disruption?
2. Are you trying to stabilize the current processor, or evaluate an alternative quickly?

If useful, I can send back the shortest next-step read for your case. If you want a lighter first step, there is also a free snapshot before live monitoring:

{checkout_url}

Best,
PayFlux
"""
    return body.strip()
