import requests

HEADERS = {
    "User-Agent": "AgentFlux/1.0 (operations-agent; +https://agentflux.dev)"
}

def web_fetch(url: str, dynamic=False):
    if dynamic:
        try:
            from tools.browser_tool import browser_fetch
            result = browser_fetch(url)
            return {"status": 200 if result.get("status") == "ok" else 500, "content": result.get("text", result.get("html", ""))[:5000]}
        except Exception as e:
            return {"error": f"Browser fetch failed: {e}"}
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        return {"status": r.status_code, "content": r.text[:5000]}
    except Exception as e:
        return {"error": str(e)}
