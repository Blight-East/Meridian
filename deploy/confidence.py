import re

def compute_signal_confidence(signal_text, source_url, is_duplicate=False):
    confidence = 0.0
    text_lower = signal_text.lower()
    
    # Source weights
    if 'reddit' in text_lower or 'reddit.com' in source_url:
        confidence += 0.7
    elif 'hackernews' in text_lower or 'news.ycombinator.com' in source_url:
        confidence += 0.8
    else:
        confidence += 0.9  # e.g., forum
        
    # Engagement weights
    match = re.search(r'\[score:(\d+)\|comments:(\d+)\]', text_lower)
    if match:
        score = int(match.group(1))
        comments = int(match.group(2))
        confidence += min(score * 0.01, 0.5)
        confidence += min(comments * 0.02, 0.5)

    if is_duplicate:
        confidence += 1.0  # Duplication confirms signal reality

    return round(confidence, 2)
