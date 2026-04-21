from config.logging_config import get_logger

logger = get_logger("browser_tool")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def _http_fallback(url: str, timeout_ms: int = 30000):
    """Fetch a URL without browser automation."""
    try:
        import httpx

        timeout_s = max(timeout_ms / 1000.0, 1.0)
        response = httpx.get(
            url,
            timeout=timeout_s,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
        return {
            "status": "ok",
            "url": str(response.url),
            "title": "",
            "text": response.text[:5000],
            "html": response.text,
        }
    except Exception as e:
        logger.error(f"browser_fetch fallback failed: {e}")
        return {"status": "error", "error": str(e), "url": url}


def browser_fetch(url: str, wait_for: str = "networkidle", timeout: int = 45000):
    """Fetch a URL with Playwright + Stealth when available, otherwise use plain HTTP."""
    try:
        from playwright.sync_api import sync_playwright
        try:
            from playwright_stealth import stealth_sync as _stealth_sync
        except Exception:
            _stealth_sync = None
        from playwright_stealth import stealth_sync
    except Exception:
        return _http_fallback(url, timeout)

    browser = None
    context = None
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            context = browser.new_context(
                viewport={"width": 1440, "height": 1080},
                user_agent=USER_AGENT,
                locale="en-US",
                timezone_id="America/New_York",
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            page = context.new_page()
            if _stealth_sync is not None:
                try:
                    _stealth_sync(page)
                except Exception as _stealth_err:
                    logger.warning(f"playwright-stealth failed to apply: {_stealth_err}")
            stealth_sync(page)
            
            # Initial navigation
            page.goto(url, wait_until=wait_for, timeout=timeout)
            
            # Check for interstitial challenges (AWS WAF, Cloudflare)
            title = page.title()
            if any(term in title for term in ["Verifying Connection", "Just a moment", "Checking your browser"]):
                logger.info(f"Challenge detected on {url}, waiting for resolution...")
                page.wait_for_timeout(8000) # Give it 8s to solve
            
            page.wait_for_timeout(2000)
            html = page.content()
            title = page.title()
            text = page.inner_text("body")
            return {
                "status": "ok",
                "url": page.url,
                "title": title,
                "text": text[:5000],
                "html": html,
            }
    except Exception as e:
        logger.warning(f"Playwright browser_fetch failed for {url}: {e}")
        return _http_fallback(url, timeout)
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass
        if browser:
            try:
                browser.close()
            except Exception:
                pass


def browser_screenshot(url: str):
    """Screenshot support is only available in the deploy browser tool."""
    logger.warning("browser_screenshot not available in runtime browser tool")
    return {"status": "error", "error": "Screenshot support not configured", "url": url}


def browser_interact(url: str, actions=None):
    """Interaction support is only available in the deploy browser tool."""
    logger.warning("browser_interact not available in runtime browser tool")
    return {"status": "error", "error": "Interaction support not configured", "url": url}
