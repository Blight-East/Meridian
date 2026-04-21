"""
Contact Discovery Unit Tests
Validates email, linkedin extraction and confidence scoring.
"""
import sys, os, importlib.util
sys.path.insert(0, os.path.dirname(__file__))

# Import pure functions directly
spec = importlib.util.spec_from_file_location("contact_discovery", os.path.join(os.path.dirname(__file__), "contact_discovery.py"))
loader = spec.loader

_source = open(os.path.join(os.path.dirname(__file__), "contact_discovery.py")).read()
import re
_globals = {
    "re": re, 
    "__name__": "contact_discovery", 
    "__file__": os.path.abspath(os.path.join(os.path.dirname(__file__), "contact_discovery.py")),
    "os": os
}
_lines = _source.split("\n")
_filtered = []
for line in _lines:
    if "from sqlalchemy" in line or "from config." in line or "requests" in line or "time" in line or "urllib" in line:
        continue
    if line.startswith("engine =") or line.startswith("logger ="):
        continue
    _filtered.append(line)
exec("\n".join(_filtered), _globals)

extract_emails = _globals["extract_emails_from_html"]
extract_linkedin = _globals["extract_linkedin_from_html"]
score_contact = _globals["score_contact"]

PASS = 0
FAIL = 0

def assert_eq(label, actual, expected):
    global PASS, FAIL
    if actual == expected:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ {label}: expected {expected!r}, got {actual!r}")

def test_email_extraction():
    print("\n── Email Extraction ──")
    html = 'Contact us at <a href="mailto:founder@coolstore.net">CEO</a> or support@coolstore.net. Do not reply to noreply@coolstore.net.'
    emails = extract_emails(html)
    assert_eq("extracts core emails, ignores noreply", emails, ["founder@coolstore.net", "support@coolstore.net"])
    
    html_noise = 'Some wixpress.com pixel. Email me: John.Doe@store.co.uk.'
    emails = extract_emails(html_noise)
    assert_eq("extracts standard subdomains, handles casing", emails, ["john.doe@store.co.uk"])

def test_linkedin_extraction():
    print("\n── LinkedIn Extraction ──")
    html = 'Find us on <a href="https://linkedin.com/company/store-name/">LinkedIn</a> and personal profile https://www.linkedin.com/in/john-smith-123.'
    urls = extract_linkedin(html)
    assert_eq("extracts company and personal profiles", urls, ["https://linkedin.com/company/store-name", "https://www.linkedin.com/in/john-smith-123"])

def test_scoring():
    print("\n── Confidence Scoring ──")
    assert_eq("ceo@ gets 0.8", score_contact(email="ceo@coolstore.com"), 0.8)
    assert_eq("founder@ gets 0.8", score_contact(email="founder@store.net"), 0.8)
    assert_eq("jane.doe@ gets 0.7", score_contact(email="jane.doe@brand.co"), 0.7)
    assert_eq("john@ gets 0.7", score_contact(email="john@brand.co"), 0.7)
    assert_eq("support@ gets 0.3", score_contact(email="support@something.com"), 0.3)
    assert_eq("no-reply@ gets 0.1", score_contact(email="no-reply@brand.com"), 0.1)
    
    assert_eq("/in/ gets 0.8", score_contact(linkedin_url="https://linkedin.com/in/mark"), 0.8)
    assert_eq("/company/ gets 0.5", score_contact(linkedin_url="https://linkedin.com/company/stuff"), 0.5)

if __name__ == "__main__":
    print("=" * 50)
    print("CONTACT DISCOVERY TEST SUITE")
    print("=" * 50)
    test_email_extraction()
    test_linkedin_extraction()
    test_scoring()
    print(f"\n{'=' * 50}")
    print(f"Results: {PASS} passed, {FAIL} failed")
    print(f"{'=' * 50}")
    sys.exit(1 if FAIL > 0 else 0)
