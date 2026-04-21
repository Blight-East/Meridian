import json

def sanitize_tool_result(data, max_chars=1200):
    try:
        s = data if isinstance(data, str) else json.dumps(data, separators=(",", ":"))
        if len(s) > max_chars:
            return s[:max_chars] + "... [TRUNCATED]"
        return s
    except Exception as e:
        return f"Sanitization error: {str(e)}"
