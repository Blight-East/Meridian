"""
Test Merchant Attribution Module
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def run_tests():
    print("Running Merchant Attribution Tests...\n")

    from merchant_attribution import detect_platform, detect_processor, extract_brand, attribute_merchant

    # Test 1: Platform detection
    assert detect_platform("Stripe froze my Shopify store payouts") == "shopify"
    assert detect_platform("My WooCommerce site had payments blocked") == "woocommerce"
    assert detect_platform("BigCommerce merchant account suspended") == "bigcommerce"
    assert detect_platform("random text about weather") is None
    print("PASS: platform detection works")

    # Test 2: Processor detection
    assert detect_processor("Stripe froze my account") == "stripe"
    assert detect_processor("PayPal held my funds") == "paypal"
    assert detect_processor("Square payments are delayed") == "square"
    print("PASS: processor detection works")

    # Test 3: Brand extraction from "my store X" patterns
    brand = extract_brand("My store GlowUp Cosmetics got frozen by Stripe")
    assert brand is not None, f"Expected brand, got None"
    print(f"PASS: brand extraction from 'my store X' -> '{brand}'")

    # Test 4: Brand from "I run X" pattern
    brand2 = extract_brand("I run TechVault and PayPal limited my account")
    assert brand2 is not None, f"Expected brand, got None"
    print(f"PASS: brand from 'I run X' -> '{brand2}'")

    # Test 5: Brand from quoted name
    brand3 = extract_brand('My business "NovaPets" had its Stripe account terminated')
    assert brand3 is not None, f"Expected brand, got None"
    print(f"PASS: brand from quoted name -> '{brand3}'")

    # Test 6: Full attribution pipeline
    result = attribute_merchant(
        "Stripe froze my Shopify store payouts, my business LuxeWear has been down for weeks",
        author="luxewear_official"
    )
    assert result is not None, "Expected attribution result"
    assert result["platform"] == "shopify", f"Expected shopify, got {result['platform']}"
    assert result["processor"] == "stripe", f"Expected stripe, got {result['processor']}"
    assert result["brand"] is not None, f"Expected brand"
    assert result["confidence"] > 0, f"Expected confidence > 0"
    print(f"PASS: full attribution -> platform={result['platform']}, processor={result['processor']}, brand={result['brand']}, confidence={result['confidence']}")

    # Test 7: No attribution for generic text
    result2 = attribute_merchant("The weather is nice today")
    assert result2 is None
    print("PASS: no false positive on generic text")

    # Test 8: Platform-only attribution
    result3 = attribute_merchant("Shopify payments are delayed again")
    assert result3 is not None
    assert result3["platform"] == "shopify"
    print(f"PASS: platform-only attribution -> platform={result3['platform']}, confidence={result3['confidence']}")

    print("\nALL MERCHANT ATTRIBUTION TESTS PASSED!")

if __name__ == "__main__":
    run_tests()
