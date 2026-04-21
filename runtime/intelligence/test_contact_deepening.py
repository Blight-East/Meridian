"""
Regression checks for merchant_contact_deepening.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from runtime.intelligence.contact_discovery import (
    _build_candidate,
    _build_priority_plan,
    _candidate_is_high_confidence,
    _candidate_rank_key,
    _extract_mailto_records,
    extract_emails_from_html,
)


def run_tests():
    visible_text = "Reach finance@merchant.com or support@merchant.com. Ignore noreply@merchant.com."
    emails = extract_emails_from_html(visible_text)
    assert emails == ["finance@merchant.com", "support@merchant.com"], emails

    mailto_html = '<a href="mailto:finance@merchant.com">Finance Team</a><a href="mailto:support@merchant.com">support@merchant.com</a>'
    mailto_records = _extract_mailto_records(mailto_html)
    assert mailto_records == [
        {"email": "finance@merchant.com", "label": "Finance Team"},
        {"email": "support@merchant.com", "label": "support@merchant.com"},
    ], mailto_records

    strong_candidate = _build_candidate(
        email="finance@merchant.com",
        merchant_domain="merchant.com",
        page_type="contact_page",
        page_url="https://merchant.com/contact",
        crawl_step=1,
        source="website:contact_page",
        context_text="Finance Team",
        email_verified=True,
        target_prefixes=["finance", "payments", "operations", "founder"],
    )
    weak_candidate = _build_candidate(
        email="support@merchant.com",
        merchant_domain="merchant.com",
        page_type="legal_page",
        page_url="https://merchant.com/legal",
        crawl_step=3,
        source="website:legal_page",
        context_text="Support",
        email_verified=False,
        target_prefixes=["finance", "payments", "operations", "founder"],
    )
    assert _candidate_is_high_confidence(strong_candidate) is True
    assert _candidate_is_high_confidence(weak_candidate) is False
    assert _candidate_rank_key(strong_candidate) > _candidate_rank_key(weak_candidate)

    homepage_html = """
    <html>
      <body>
        <a href="/about">About</a>
        <a href="/contact">Contact</a>
        <a href="/privacy">Privacy</a>
        <a href="/support">Support</a>
      </body>
    </html>
    """
    plan = _build_priority_plan(
        homepage_html=homepage_html,
        base_url="https://merchant.com",
        merchant_domain="merchant.com",
    )
    ordered_types = [item["page_type"] for item in plan[:5]]
    assert ordered_types[0] == "contact_page", ordered_types
    assert ordered_types[1] in {"about_page", "team_page"}, ordered_types
    assert "legal_page" in ordered_types[:4], ordered_types
    assert any(page_type in {"support_page", "finance_page", "payment_page"} for page_type in ordered_types[:5]), ordered_types

    print("merchant_contact_deepening checks passed")


if __name__ == "__main__":
    run_tests()
