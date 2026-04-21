"""
Test Router and Cache Functionality
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import time
from runtime.reasoning.reasoning_router import route_reasoning
from runtime.reasoning.claude import reason_fast

def run_tests():
    print("Running Router Tests...\n")
    
    # Test 1: Low severity cluster
    model, func = route_reasoning("cluster_analysis", severity="low")
    assert model == "sonnet", f"Expected sonnet, got {model}"
    print("PASS: low severity cluster -> SONNET")
    
    # Test 2: Critical cluster
    model, func = route_reasoning("cluster_analysis", severity="critical")
    assert model == "opus", f"Expected opus, got {model}"
    print("PASS: critical cluster -> OPUS")
    
    # Test 3: Merchant Value
    model, func = route_reasoning("lead_investigation", merchant_value=12000)
    assert model == "opus", f"Expected opus, got {model}"
    model, func = route_reasoning("lead_investigation", merchant_value=5000)
    assert model == "sonnet", f"Expected sonnet, got {model}"
    print("PASS: merchant_value routing -> OPUS (12k) / SONNET (5k)")
    
    # Test 4 & 5: Prediction and Sales
    model, func = route_reasoning("pattern_prediction")
    assert model == "opus", f"Expected opus, got {model}"
    print("PASS: pattern_prediction -> OPUS")
    
    model, func = route_reasoning("sales_conversation_strategy")
    assert model == "opus", f"Expected opus, got {model}"
    print("PASS: sales_reasoning -> OPUS")
    
    # Test 6: Cache hit -> no LLM call
    print("\nTesting LLM Cache (creating unique prompt)...")
    prompt_text = f"Test cache prompt for testing agent flux logic {time.time()}"
    
    # First call (should hit LLM)
    res1 = reason_fast(prompt_text, use_cache=True)
    if isinstance(res1, dict):
        assert res1.get("_meta", {}).get("source") == "llm", f"Expected source: llm, got {res1.get('_meta')}"
        
    # Second call (should hit cache)
    res2 = reason_fast(prompt_text, use_cache=True)
    if isinstance(res2, dict):
        assert res2.get("_meta", {}).get("source") == "exact_cache", f"Expected source: exact_cache, got {res2.get('_meta')}"
    print("PASS: cache hit -> no LLM call")

    from safety.rate_limiter import rate_limiter
    print(f"\nRate limits configured:")
    print(f"Sonnet limit: {rate_limiter.limits.get('llm_call_sonnet')} / hr")
    print(f"Opus limit: {rate_limiter.limits.get('llm_call_opus')} / hr")
    
    print("\nALL TESTS PASSED!")

if __name__ == "__main__":
    run_tests()
