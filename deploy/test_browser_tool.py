"""
Test Browser Tool (Playwright)
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def run_tests():
    print("Running Browser Tool Tests...\n")

    from tools.browser_tool import browser_fetch, browser_screenshot, close_browser

    # Test 1: Fetch a simple page
    result = browser_fetch("https://example.com")
    assert result["status"] == "ok", f"Expected ok, got {result}"
    assert "Example Domain" in result.get("title", ""), f"Expected title, got {result.get('title')}"
    assert len(result.get("html", "")) > 100, "Expected HTML content"
    print(f"PASS: browser_fetch works (title: {result['title']}, html: {len(result['html'])} bytes)")

    # Test 2: Fetch a JS-heavy page (Trustpilot)
    result2 = browser_fetch("https://www.trustpilot.com/review/stripe.com")
    assert result2["status"] == "ok", f"Trustpilot fetch failed: {result2}"
    html_len = len(result2.get("html", ""))
    print(f"PASS: Trustpilot JS-rendered fetch (html: {html_len} bytes)")

    # Test 3: Screenshot
    result3 = browser_screenshot("https://example.com")
    assert result3["status"] == "ok", f"Screenshot failed: {result3}"
    assert os.path.exists(result3["path"]), f"Screenshot file not found: {result3['path']}"
    print(f"PASS: screenshot saved to {result3['path']}")

    # Test 4: Dynamic web_fetch flag
    from tools.web_fetch import web_fetch
    static_result = web_fetch("https://example.com", dynamic=False)
    assert static_result.get("status") == 200, f"Static fetch failed: {static_result}"
    print("PASS: web_fetch static mode works")

    dynamic_result = web_fetch("https://example.com", dynamic=True)
    assert dynamic_result.get("status") == 200, f"Dynamic fetch failed: {dynamic_result}"
    print("PASS: web_fetch dynamic mode works")

    close_browser()
    print("\nALL BROWSER TOOL TESTS PASSED!")

if __name__ == "__main__":
    run_tests()
