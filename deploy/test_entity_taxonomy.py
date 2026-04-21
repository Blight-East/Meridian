"""
Entity Taxonomy Unit Tests
Validates entity classification, scoring, and lead validity gate logic.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from entity_taxonomy import (
    classify_entity, score_entity_type, is_valid_lead,
    ENTITY_TYPE_PROCESSOR, ENTITY_TYPE_AFFILIATE_NETWORK,
    ENTITY_TYPE_PLATFORM, ENTITY_TYPE_CONSUMER_SERVICE,
    ENTITY_TYPE_UNKNOWN, ENTITY_TYPE_BANK,
    CLASS_CONSUMER_COMPLAINT, CLASS_AFFILIATE_DISTRESS,
    CLASS_PROCESSOR_DISTRESS, CLASS_MERCHANT_DISTRESS,
)

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


def test_processor_classification():
    print("\n── Processor Classification ──")
    info = classify_entity("Stripe froze my merchant account and all payouts are held")
    assert_eq("Stripe → entity_name", info["entity_name"], "Stripe")
    assert_eq("Stripe → entity_type", info["entity_type"], ENTITY_TYPE_PROCESSOR)
    assert_eq("Stripe → classification", info["classification"], CLASS_MERCHANT_DISTRESS)

    info = classify_entity("PayPal holding my business funds for 180 days")
    assert_eq("PayPal → entity_name", info["entity_name"], "PayPal")
    assert_eq("PayPal → entity_type", info["entity_type"], ENTITY_TYPE_PROCESSOR)

    info = classify_entity("Square payout delayed again, this is killing my business")
    assert_eq("Square → entity_name", info["entity_name"], "Square")
    assert_eq("Square → entity_type", info["entity_type"], ENTITY_TYPE_PROCESSOR)


def test_rakuten_subcases():
    print("\n── Rakuten Sub-Cases ──")

    info = classify_entity("Rakuten cashback not showing up in my account")
    assert_eq("Rakuten cashback → entity_name", info["entity_name"], "Rakuten")
    assert_eq("Rakuten cashback → entity_type", info["entity_type"], ENTITY_TYPE_AFFILIATE_NETWORK)
    assert_eq("Rakuten cashback → classification", info["classification"], CLASS_CONSUMER_COMPLAINT)

    info = classify_entity("Rakuten affiliate payout delayed for three months")
    assert_eq("Rakuten affiliate → entity_name", info["entity_name"], "Rakuten")
    assert_eq("Rakuten affiliate → entity_type", info["entity_type"], ENTITY_TYPE_AFFILIATE_NETWORK)
    assert_eq("Rakuten affiliate → classification", info["classification"], CLASS_AFFILIATE_DISTRESS)

    info = classify_entity("Rakuten Pay processing failed on checkout")
    assert_eq("Rakuten Pay → entity_name", info["entity_name"], "Rakuten")
    assert_eq("Rakuten Pay → entity_type", info["entity_type"], ENTITY_TYPE_AFFILIATE_NETWORK)
    assert_eq("Rakuten Pay → classification", info["classification"], CLASS_PROCESSOR_DISTRESS)


def test_platform_classification():
    print("\n── Platform Classification ──")
    info = classify_entity("Shopify terminated my store without warning, all revenue frozen")
    assert_eq("Shopify → entity_name", info["entity_name"], "Shopify")
    assert_eq("Shopify → entity_type", info["entity_type"], ENTITY_TYPE_PLATFORM)
    assert_eq("Shopify → classification", info["classification"], CLASS_MERCHANT_DISTRESS)


def test_consumer_service_classification():
    print("\n── Consumer Service Classification ──")
    info = classify_entity("Venmo refund not received, want my money back")
    assert_eq("Venmo → entity_name", info["entity_name"], "Venmo")
    assert_eq("Venmo → entity_type", info["entity_type"], ENTITY_TYPE_CONSUMER_SERVICE)
    assert_eq("Venmo → classification", info["classification"], CLASS_CONSUMER_COMPLAINT)


def test_unknown_entity():
    print("\n── Unknown Entity ──")
    info = classify_entity("random text with no known entity references")
    assert_eq("Unknown → entity_name", info["entity_name"], None)
    assert_eq("Unknown → entity_type", info["entity_type"], ENTITY_TYPE_UNKNOWN)
    assert_eq("Unknown → classification", info["classification"], None)


def test_empty_input():
    print("\n── Empty / None Input ──")
    info = classify_entity("")
    assert_eq("Empty string → entity_type", info["entity_type"], ENTITY_TYPE_UNKNOWN)
    info = classify_entity(None)
    assert_eq("None → entity_type", info["entity_type"], ENTITY_TYPE_UNKNOWN)


def test_scoring():
    print("\n── Entity Type Scoring ──")
    assert_eq("processor score", score_entity_type(ENTITY_TYPE_PROCESSOR), 40)
    assert_eq("bank score", score_entity_type(ENTITY_TYPE_BANK), 35)
    assert_eq("affiliate_network score", score_entity_type(ENTITY_TYPE_AFFILIATE_NETWORK), 10)
    assert_eq("consumer_service score", score_entity_type(ENTITY_TYPE_CONSUMER_SERVICE), -50)
    assert_eq("unknown score", score_entity_type(ENTITY_TYPE_UNKNOWN), 0)


def test_lead_validity_gate():
    print("\n── Lead Validity Gate ──")
    assert_eq("consumer_complaint → blocked", is_valid_lead(CLASS_CONSUMER_COMPLAINT, ENTITY_TYPE_AFFILIATE_NETWORK), False)
    assert_eq("consumer_service → blocked", is_valid_lead(None, ENTITY_TYPE_CONSUMER_SERVICE), False)
    assert_eq("processor distress → allowed", is_valid_lead(CLASS_PROCESSOR_DISTRESS, ENTITY_TYPE_PROCESSOR), True)
    assert_eq("merchant distress → allowed", is_valid_lead(CLASS_MERCHANT_DISTRESS, ENTITY_TYPE_PROCESSOR), True)
    assert_eq("affiliate distress → allowed", is_valid_lead(CLASS_AFFILIATE_DISTRESS, ENTITY_TYPE_AFFILIATE_NETWORK), True)
    assert_eq("platform distress → allowed", is_valid_lead(CLASS_MERCHANT_DISTRESS, ENTITY_TYPE_PLATFORM), True)


def test_end_to_end_scenarios():
    print("\n── End-to-End Validation Scenarios ──")

    # Scenario 1: Rakuten cashback → NOT a lead
    info = classify_entity("Rakuten cashback complaint - never got my cashback from purchase")
    valid = is_valid_lead(info["classification"], info["entity_type"])
    assert_eq("Rakuten cashback → lead NOT generated", valid, False)

    # Scenario 2: Stripe payout freeze → IS a lead
    info = classify_entity("Stripe payout frozen, my business account has funds held")
    valid = is_valid_lead(info["classification"], info["entity_type"])
    assert_eq("Stripe payout freeze → lead qualifies", valid, True)

    # Scenario 3: Shopify store termination → IS a lead
    info = classify_entity("Shopify terminated my store, all payment processing stopped")
    valid = is_valid_lead(info["classification"], info["entity_type"])
    assert_eq("Shopify termination → lead qualifies", valid, True)


if __name__ == "__main__":
    print("=" * 50)
    print("ENTITY TAXONOMY TEST SUITE")
    print("=" * 50)

    test_processor_classification()
    test_rakuten_subcases()
    test_platform_classification()
    test_consumer_service_classification()
    test_unknown_entity()
    test_empty_input()
    test_scoring()
    test_lead_validity_gate()
    test_end_to_end_scenarios()

    print(f"\n{'=' * 50}")
    print(f"Results: {PASS} passed, {FAIL} failed")
    print(f"{'=' * 50}")

    sys.exit(1 if FAIL > 0 else 0)
