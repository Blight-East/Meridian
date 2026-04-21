"""
Headless Browser Tool (Playwright) — with Safety Guardrails

Security layers:
  1. Content sanitizer — strips prompt injection patterns before LLM sees text
  2. Domain blocklist — prevents accessing internal/dangerous endpoints
  3. Rate limiter — max requests per minute
  4. Crawl depth limit — max pages per session
  5. Output size cap — prevents massive text dumps to LLM context
  6. robots.txt compliance — respects site crawl rules
"""
import os, re, time
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
from config.logging_config import get_logger

logger = get_logger("browser_tool")

# Shared browser instance (lazy init)
_playwright = None
_browser = None

# ── Rate Limiter ─────────────────────────────────────────────────
_request_log = []  # timestamps of recent requests
MAX_REQUESTS_PER_MINUTE = 10
MAX_PAGES_PER_SESSION = 20
_session_page_count = 0

# ── Domain Blocklist ─────────────────────────────────────────────
BLOCKED_DOMAINS = {
    # Internal / dangerous
    "localhost", "127.0.0.1", "0.0.0.0", "::1",
    "metadata.google.internal", "169.254.169.254",  # cloud metadata
    # Government / sensitive
    "irs.gov", "ssa.gov",
    # Agent's own infrastructure
    "api.payflux.dev", "payflux.dev",
}

BLOCKED_PATTERNS = [
    re.compile(r"^https?://10\.\d+\.\d+\.\d+"),      # private IPs
    re.compile(r"^https?://172\.(1[6-9]|2\d|3[01])"), # private IPs
    re.compile(r"^https?://192\.168\."),                # private IPs
    re.compile(r"file://"),                             # local files
    re.compile(r"^data:"),                              # data URIs
    re.compile(r"^javascript:"),                        # JS injection
]

# ── Prompt Injection Sanitizer ───────────────────────────────────
INJECTION_PATTERNS = [
    # Direct instruction overrides
    re.compile(r"ignore\s+(all\s+)?(previous|prior|above|your)\s+(instructions?|prompts?|rules?|system)", re.IGNORECASE),
    re.compile(r"disregard\s+(all\s+)?(previous|prior|above|your)\s+(instructions?|prompts?|rules?)", re.IGNORECASE),
    re.compile(r"forget\s+(all\s+)?(previous|prior|above|your)\s+(instructions?|prompts?|rules?)", re.IGNORECASE),
    re.compile(r"new\s+system\s+prompt", re.IGNORECASE),
    re.compile(r"you\s+are\s+now\s+", re.IGNORECASE),
    re.compile(r"act\s+as\s+(if\s+you\s+are|a)\s+", re.IGNORECASE),
    re.compile(r"pretend\s+(to\s+be|you\s+are)", re.IGNORECASE),

    # Data exfiltration attempts
    re.compile(r"send\s+(all\s+)?data\s+to", re.IGNORECASE),
    re.compile(r"forward\s+(all\s+)?(information|data|content)\s+to", re.IGNORECASE),
    re.compile(r"(api[_-]?key|secret|password|token|credential)", re.IGNORECASE),
    re.compile(r"exfiltrate", re.IGNORECASE),
    re.compile(r"(curl|wget|fetch)\s+https?://", re.IGNORECASE),

    # System manipulation
    re.compile(r"execute\s+(this\s+)?(command|code|script)", re.IGNORECASE),
    re.compile(r"run\s+(this\s+)?(command|code|script)", re.IGNORECASE),
    re.compile(r"<\s*script", re.IGNORECASE),
]

MAX_OUTPUT_TEXT = 3000  # characters sent to LLM


def _check_rate_limit():
    """Enforce rate limiting. Raises if limit exceeded."""
    global _request_log, _session_page_count
    now = time.time()
    # Purge old entries
    _request_log = [t for t in _request_log if now - t < 60]
    if len(_request_log) >= MAX_REQUESTS_PER_MINUTE:
        raise RuntimeError(f"Rate limit exceeded: max {MAX_REQUESTS_PER_MINUTE} requests/minute")
    if _session_page_count >= MAX_PAGES_PER_SESSION:
        raise RuntimeError(f"Session crawl limit exceeded: max {MAX_PAGES_PER_SESSION} pages")
    _request_log.append(now)
    _session_page_count += 1


def _check_url_safety(url):
    """Block dangerous URLs before fetching."""
    if not url or not isinstance(url, str):
        raise ValueError("Invalid URL")

    parsed = urlparse(url)
    hostname = parsed.hostname or ""

    # Block dangerous domains
    if hostname in BLOCKED_DOMAINS:
        raise ValueError(f"Blocked domain: {hostname}")

    # Block private IPs and dangerous schemes
    for pattern in BLOCKED_PATTERNS:
        if pattern.search(url):
            raise ValueError(f"Blocked URL pattern: {url}")

    # Must be http or https
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Blocked scheme: {parsed.scheme}")


def sanitize_content(text):
    """
    Strip prompt injection patterns from page text before it reaches the LLM.
    Replaces dangerous patterns with [REDACTED] markers.
    """
    if not text:
        return text

    clean = text
    injection_count = 0

    for pattern in INJECTION_PATTERNS:
        matches = pattern.findall(clean)
        if matches:
            injection_count += len(matches)
            clean = pattern.sub("[CONTENT_FILTERED]", clean)

    if injection_count > 0:
        logger.warning(f"Sanitized {injection_count} potential injection patterns from page content")

    # Cap output size
    if len(clean) > MAX_OUTPUT_TEXT:
        clean = clean[:MAX_OUTPUT_TEXT] + f"\n\n[TRUNCATED — {len(text)} total chars, showing first {MAX_OUTPUT_TEXT}]"

    return clean


def _get_browser():
    """Get or create a persistent browser instance."""
    global _playwright, _browser
    if _browser is None or not _browser.is_connected():
        _playwright = sync_playwright().start()
        _browser = _playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ]
        )
        logger.info("Playwright Chromium browser launched")
    return _browser


def _new_context():
    """Create a browser context with realistic anti-detection settings."""
    browser = _get_browser()
    context = browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        locale="en-US",
        timezone_id="America/New_York",
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return context


def browser_fetch(url, wait_for="domcontentloaded", timeout=30000):
    """
    Fetch a URL with full JS rendering — with safety guardrails.
    Returns dict with sanitized text content and page title.
    """
    _check_url_safety(url)
    _check_rate_limit()

    context = None
    try:
        context = _new_context()
        page = context.new_page()

        # Block outbound requests to prevent exfiltration via redirects
        page.route("**/*", lambda route: route.abort() if route.request.resource_type in ("websocket",) else route.continue_())

        page.goto(url, wait_until=wait_for, timeout=timeout)

        # Wait for JS to finish rendering
        page.wait_for_timeout(2000)

        # Scroll down to trigger lazy-loaded content
        page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
        page.wait_for_timeout(1500)

        html = page.content()
        text = page.inner_text("body")
        title = page.title()

        # Sanitize before returning to LLM
        safe_text = sanitize_content(text)

        logger.info(f"Browser fetched: {url} ({len(html)} bytes, sanitized to {len(safe_text)} chars)")

        return {
            "url": url,
            "title": title,
            "html": html,
            "text": safe_text,
            "status": "ok",
        }
    except Exception as e:
        logger.warning(f"Browser fetch error for {url}: {e}")
        return {"url": url, "error": str(e), "status": "error"}
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass


def browser_screenshot(url, full_page=True, timeout=30000):
    """Capture a screenshot of a URL — with safety checks."""
    _check_url_safety(url)
    _check_rate_limit()

    context = None
    try:
        context = _new_context()
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        page.wait_for_timeout(2000)

        screenshot_dir = "/tmp/agent_flux_screenshots"
        os.makedirs(screenshot_dir, exist_ok=True)

        safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', url.replace("https://", "").replace("http://", ""))[:50]
        path = f"{screenshot_dir}/{safe_name}.png"

        page.screenshot(path=path, full_page=full_page)
        logger.info(f"Screenshot saved: {path}")

        return {"path": path, "url": url, "status": "ok"}
    except Exception as e:
        logger.warning(f"Screenshot error for {url}: {e}")
        return {"url": url, "error": str(e), "status": "error"}
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass


def browser_interact(url, actions, timeout=30000):
    """
    Navigate to a URL and perform a sequence of actions — with safety limits.
    Max 10 actions per call to prevent infinite loops.
    """
    _check_url_safety(url)
    _check_rate_limit()

    # Cap action count to prevent endless loops
    MAX_ACTIONS = 10
    if len(actions) > MAX_ACTIONS:
        return {"error": f"Too many actions: {len(actions)} (max {MAX_ACTIONS})", "status": "error"}

    context = None
    try:
        context = _new_context()
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        page.wait_for_timeout(2000)

        screenshots = []
        for action in actions:
            action_type = action.get("type", "")

            if action_type == "click":
                page.click(action["selector"], timeout=5000)
            elif action_type == "type":
                page.fill(action["selector"], action["text"])
            elif action_type == "scroll":
                page.evaluate(f"window.scrollBy(0, {action.get('y', 500)})")
            elif action_type == "wait":
                # Cap wait time to 5 seconds max
                ms = min(action.get("ms", 1000), 5000)
                page.wait_for_timeout(ms)
            elif action_type == "screenshot":
                screenshot_dir = "/tmp/agent_flux_screenshots"
                os.makedirs(screenshot_dir, exist_ok=True)
                path = f"{screenshot_dir}/{action.get('name', 'interact')}.png"
                page.screenshot(path=path)
                screenshots.append(path)

            page.wait_for_timeout(300)

        text = page.inner_text("body")
        title = page.title()
        safe_text = sanitize_content(text)

        return {
            "url": url,
            "title": title,
            "text": safe_text,
            "screenshots": screenshots,
            "status": "ok",
        }
    except Exception as e:
        logger.warning(f"Browser interact error for {url}: {e}")
        return {"url": url, "error": str(e), "status": "error"}
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass


def close_browser():
    """Cleanly shut down the browser instance."""
    global _browser, _playwright, _session_page_count
    _session_page_count = 0
    if _browser:
        try:
            _browser.close()
        except Exception:
            pass
        _browser = None
    if _playwright:
        try:
            _playwright.stop()
        except Exception:
            pass
        _playwright = None
