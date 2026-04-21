"""
Test Multi-Source Ingestion Adapters
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

def run_tests():
    print("Running Multi-Source Ingestion Tests...\n")

    # Test 1: Twitter adapter normalizes properly
    from runtime.ingestion.sources.twitter_signals import normalize_signal
    sig = normalize_signal("stripe froze my account help", "@merchant1", "https://x.com/1", "2026-03-09", "stripe froze")
    assert sig["source"] == "twitter", f"Expected twitter, got {sig['source']}"
    assert sig["author"] == "@merchant1"
    assert sig["metadata"]["source_platform"] == "twitter"
    print("PASS: twitter normalizer works")

    # Test 2: Trustpilot adapter normalizes properly
    from runtime.ingestion.sources.trustpilot_signals import normalize_signal as tp_norm
    sig = tp_norm("They froze my funds for 3 months", "John D", "https://trustpilot.com/review/test", "stripe", 1)
    assert sig["source"] == "trustpilot"
    assert "[Trustpilot review for stripe]" in sig["content"]
    assert sig["metadata"]["processor"] == "stripe"
    print("PASS: trustpilot normalizer works")

    # Test 3: Shopify forum adapter normalizes properly
    from runtime.ingestion.sources.shopify_forum_signals import normalize_signal as sf_norm
    sig = sf_norm("Payouts suspended", "My payouts are stuck", "shopify_user", "https://community.shopify.com/test", 5)
    assert sig["source"] == "shopify_forum"
    assert "[Shopify Community]" in sig["content"]
    assert sig["metadata"]["engagement"] == 5
    print("PASS: shopify forum normalizer works")

    # Test 4: Stripe forum adapter normalizes properly
    from runtime.ingestion.sources.stripe_forum_signals import normalize_signal as stf_norm
    sig = stf_norm("Account under review", "My account got flagged", "stripe_user", "https://support.stripe.com/q/1", 3, "risk review")
    assert sig["source"] == "stripe_forum"
    assert "[Stripe Support]" in sig["content"]
    print("PASS: stripe forum normalizer works")

    # Test 5: Deduplication — same content should not create duplicate signals
    from runtime.intelligence.ingestion import process_and_store_signal
    content = "TEST_DEDUP_SIGNAL: stripe froze my test account dedup check 12345"
    result1 = process_and_store_signal("twitter", content)
    result2 = process_and_store_signal("twitter", content)
    assert result2 is None, "Expected None for duplicate signal"
    print("PASS: deduplication prevents duplicate insertion")

    # Test 6: Signal inserts correctly (result1 should be an ID)
    assert result1 is not None, "Expected a signal ID for first insert"
    assert isinstance(result1, int), f"Expected int, got {type(result1)}"
    print(f"PASS: signal inserted with ID {result1}")

    print("\nALL MULTI-SOURCE INGESTION TESTS PASSED!")

if __name__ == "__main__":
    run_tests()
