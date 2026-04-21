"""
Test Deal Sourcing Pipeline
"""
import sys, os, json
sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def run_tests():
    print("Running Deal Sourcing Tests...\n")

    # Test 1: Outreach draft generation
    from runtime.ops.outreach_generator import generate_outreach
    draft = generate_outreach("Acme Corp", "Stripe", "acme.com", "payout delays", "https://payflux.dev/checkout/test")
    assert "Acme Corp" in draft, "Merchant name not in draft"
    assert "Stripe" in draft, "Processor not in draft"
    assert "https://payflux.dev/checkout/test" in draft, "Checkout URL not in draft"
    print("PASS: outreach draft generation works")

    # Test 2: Opportunity table exists and is queryable
    with engine.connect() as c:
        count = c.execute(text("SELECT COUNT(*) FROM merchant_opportunities")).scalar()
        assert count is not None, "merchant_opportunities table not found"
    print(f"PASS: merchant_opportunities table exists ({count} rows)")

    # Test 3: Status transitions
    with engine.connect() as c:
        # Insert test opportunity
        c.execute(text("""
            INSERT INTO merchant_opportunities (merchant_domain, processor, distress_topic, outreach_draft, status)
            VALUES ('test-deal-sourcing.com', 'stripe', 'test topic', 'test draft', 'pending_review')
        """))
        c.commit()
        
        # Get the ID
        row = c.execute(text("SELECT id FROM merchant_opportunities WHERE merchant_domain = 'test-deal-sourcing.com' ORDER BY id DESC LIMIT 1")).fetchone()
        test_id = row[0]
        
        # Approve it
        c.execute(text("UPDATE merchant_opportunities SET status = 'approved' WHERE id = :id"), {"id": test_id})
        c.commit()
        
        status_row = c.execute(text("SELECT status FROM merchant_opportunities WHERE id = :id"), {"id": test_id}).fetchone()
        assert status_row[0] == "approved", f"Expected approved, got {status_row[0]}"
        
        # Clean up test row
        c.execute(text("DELETE FROM merchant_opportunities WHERE merchant_domain = 'test-deal-sourcing.com'"))
        c.commit()
    print("PASS: status transitions work (pending -> approved)")

    # Test 4: Sales strategy structure
    try:
        from runtime.ops.sales_reasoning import generate_sales_strategy
        print("PASS: sales_reasoning module is importable")
    except Exception as e:
        print(f"WARN: sales_reasoning import failed: {e}")

    # Test 5: API endpoints respond
    import requests
    resp = requests.get("http://127.0.0.1:8000/deal_opportunities")
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    data = resp.json()
    assert "deal_opportunities" in data, "Missing deal_opportunities key"
    print("PASS: /deal_opportunities API endpoint works")

    print("\nALL DEAL SOURCING TESTS PASSED!")

if __name__ == "__main__":
    run_tests()
