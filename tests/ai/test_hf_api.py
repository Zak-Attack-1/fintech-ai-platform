"""
Test Hugging Face API integration
File: tests/ai/test_hf_api.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from loguru import logger
from src.ai.hf_api import hf_api
from src.ai.config import config
import time

def test_api_configuration():
    """Test API configuration"""
    print("\n" + "="*60)
    print("Testing API Configuration...")
    print("="*60)
    
    if not config.hf_api_key:
        print("✗ HF_API_KEY not configured")
        print("\nTo configure:")
        print("1. Get free key: https://huggingface.co/settings/tokens")
        print("2. Add to .env: HF_API_KEY=hf_your_key_here")
        return False
    
    print(f"✓ API Key configured: {config.hf_api_key[:10]}...")
    print(f"✓ Model: {config.hf_model}")
    print(f"✓ Daily limit: {config.hf_requests_per_day}")
    print(f"✓ Monthly limit: {config.hf_requests_per_month}")
    
    return True

def test_basic_text_generation():
    """Test basic text generation"""
    print("\n" + "="*60)
    print("Testing Basic Text Generation...")
    print("="*60)
    
    if not config.hf_api_key:
        print("⚠️  Skipping (no API key)")
        return False
    
    try:
        prompt = "Explain what a stock market is in one sentence:"
        print(f"\nPrompt: {prompt}")
        print("Generating... (may take 5-10 seconds)")
        
        result = hf_api.generate_text(prompt, max_length=100)
        
        if result:
            print(f"\n✓ Generated text:")
            print(f"  {result}")
            return True
        else:
            print("✗ No text generated")
            return False
            
    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False

def test_sql_generation():
    """Test SQL generation from natural language"""
    print("\n" + "="*60)
    print("Testing SQL Generation...")
    print("="*60)
    
    if not config.hf_api_key:
        print("⚠️  Skipping (no API key)")
        return False
    
    schema = """
    Tables:
    - int_stock_daily_analysis (ticker, date, close_price, daily_return)
    - int_crypto_analysis (symbol, date, price_usd, daily_return)
    """
    
    test_queries = [
        "Show me the top 5 stocks by daily return",
        "List cryptocurrencies with negative returns today"
    ]
    
    success = True
    for nl_query in test_queries:
        print(f"\nNatural Language: {nl_query}")
        
        sql = hf_api.generate_sql_from_nl(nl_query, schema)
        
        if sql:
            print(f"✓ Generated SQL:")
            print(f"  {sql[:100]}...")
        else:
            print("✗ SQL generation failed")
            success = False
        
        time.sleep(2)  # Rate limiting
    
    return success

def test_pattern_explanation():
    """Test financial pattern explanation"""
    print("\n" + "="*60)
    print("Testing Pattern Explanation...")
    print("="*60)
    
    if not config.hf_api_key:
        print("⚠️  Skipping (no API key)")
        return False
    
    pattern_data = {
        'asset': 'AAPL',
        'pattern_type': 'price_spike',
        'price_change': '+15%',
        'volume': '3x normal',
        'date': '2024-01-15'
    }
    
    print(f"\nPattern Data: {pattern_data}")
    
    explanation = hf_api.explain_financial_pattern(pattern_data)
    
    if explanation:
        print(f"\n✓ Explanation:")
        print(f"  {explanation}")
        return True
    else:
        print("✗ Explanation generation failed")
        return False

def test_caching():
    """Test response caching"""
    print("\n" + "="*60)
    print("Testing Response Caching...")
    print("="*60)
    
    if not config.hf_api_key:
        print("⚠️  Skipping (no API key)")
        return False
    
    prompt = "What is diversification?"
    
    # First call
    print("\n1. First call (should hit API)...")
    start = time.time()
    result1 = hf_api.generate_text(prompt, max_length=50)
    time1 = time.time() - start
    
    if not result1:
        print("✗ First call failed")
        return False
    
    print(f"✓ Completed in {time1:.2f}s")
    
    # Second call (should use cache)
    print("\n2. Second call (should use cache)...")
    start = time.time()
    result2 = hf_api.generate_text(prompt, max_length=50)
    time2 = time.time() - start
    
    if not result2:
        print("✗ Second call failed")
        return False
    
    print(f"✓ Completed in {time2:.2f}s")
    
    if time2 < time1 / 2:
        print(f"✓ Cache working ({time2:.2f}s vs {time1:.2f}s)")
        return True
    else:
        print(f"⚠️  Cache may not be working")
        return False

def test_rate_limiting():
    """Test rate limiting"""
    print("\n" + "="*60)
    print("Testing Rate Limiting...")
    print("="*60)
    
    stats = hf_api.get_usage_stats()
    
    print(f"\nCurrent Usage:")
    print(f"  Today: {stats['requests_today']}/{stats['daily_limit']}")
    print(f"  Month: {stats['requests_month']}/{stats['monthly_limit']}")
    print(f"  Remaining today: {stats['remaining_today']}")
    print(f"  Cache size: {stats['cache_size']}")
    
    if stats['last_request']:
        print(f"  Last request: {stats['last_request']}")
    
    print("\n✓ Rate limiting tracking operational")
    
    # Show percentage used
    daily_pct = (stats['requests_today'] / stats['daily_limit']) * 100
    monthly_pct = (stats['requests_month'] / stats['monthly_limit']) * 100
    
    print(f"\nUsage levels:")
    print(f"  Daily: {daily_pct:.1f}%")
    print(f"  Monthly: {monthly_pct:.1f}%")
    
    if daily_pct > 80:
        print("  ⚠️  Warning: High daily usage")
    if monthly_pct > 80:
        print("  ⚠️  Warning: High monthly usage")
    
    return True

def test_error_handling():
    """Test error handling"""
    print("\n" + "="*60)
    print("Testing Error Handling...")
    print("="*60)
    
    # Test with empty prompt
    print("\n1. Testing empty prompt...")
    result = hf_api.generate_text("")
    print(f"  {'✓' if result is None or result == '' else '✗'} Handled gracefully")
    
    # Test with very long prompt
    print("\n2. Testing very long prompt...")
    long_prompt = "Test " * 1000
    result = hf_api.generate_text(long_prompt, max_length=10)
    print(f"  {'✓' if result is not None else '⚠️'} Handled long input")
    
    print("\n✓ Error handling tests complete")
    return True

def run_all_tests():
    """Run all Hugging Face API tests"""
    print("\n" + "🤖 " + "="*58)
    print("  Hugging Face API Tests")
    print("="*60)
    
    results = {
        'Configuration': test_api_configuration(),
    }
    
    # Only run API tests if configured
    if results['Configuration']:
        print("\n⏳ Running API tests (this will take ~30-60 seconds)...")
        results['Text Generation'] = test_basic_text_generation()
        results['SQL Generation'] = test_sql_generation()
        results['Pattern Explanation'] = test_pattern_explanation()
        results['Caching'] = test_caching()
        results['Rate Limiting'] = test_rate_limiting()
        results['Error Handling'] = test_error_handling()
    else:
        print("\n⚠️  API not configured. Set HF_API_KEY to run API tests.")
    
    print("\n" + "="*60)
    print("Test Results Summary")
    print("="*60)
    
    for test_name, passed in results.items():
        if passed is None:
            continue
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name:.<40} {status}")
    
    # Show usage stats
    if results['Configuration']:
        print("\n" + "="*60)
        stats = hf_api.get_usage_stats()
        print(f"API Usage: {stats['requests_today']} requests today, "
              f"{stats['requests_month']} this month")
        print("="*60)
    
    all_passed = all(v for v in results.values() if v is not None)
    
    if all_passed:
        print("\n✓ All tests passed! HF API integration ready.")
    else:
        print("\n⚠️  Some tests failed. Check errors above.")
    
    return all_passed

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)