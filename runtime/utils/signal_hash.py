import hashlib
import re


def normalize_signal_text(text):
    if not text:
        return ""

    normalized = text.lower().strip()
    normalized = re.sub(r"^\[[^\]]+\]\s*", "", normalized)
    normalized = re.sub(r"\[score:[^\]]+\]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()

def generate_signal_hash(text):
    normalized_text = normalize_signal_text(text)
    if not normalized_text:
        return ""
    return hashlib.sha256(normalized_text.encode('utf-8')).hexdigest()
