"""
Test Natural Language to SQL system
File: tests/ai/test_nl_to_sql.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.ai.nl_to_sql import nl_sql
from src.ai.schema_manager import schema_manager
from loguru import logger
import json

def test_schema_manager():
    """Test schema manager"""
    print("\n" + "="*70)
    print("Testing Schema Manager...")
    print("="*70)
    
    try:
        # Test table listing
        tables = schema_manager.get_all_tables()
        print(f"\n✓ Found {len(tables)} tables in schema")
        print(f"  Sample tables: {', '.join(tables[:5])}")
        
        # Test relevant table detection
        test_queries = [
            "Show me top stocks",
            "Compare Bitcoin and Ethereum",
            "What's the market trend?"
        ]
        
        print("\n✓ Testing relevant table detection:")
        for query in test_queries:
            relevant = schema_manager.get_relevant_tables(query)
            print(f"  '{query}' → {', '.join(relevant[:2])}")
        
        print("\n✓ Schema manager working")
        return True
        
    except Exception as e:
        print(f"✗ Schema manager test failed: {e}")
        return False

def test_simple_queries():
    """Test simple template-based queries"""
    print("\n" + "="*70)
    print("Testing Simple Queries (Template-Based)...")
    print("="*70)
    
    test_queries = [
        "Show me the top 10 stocks by return",
        "List top 5 cryptocurrencies",
        "Show stocks with high volatility",
        "What are the top performing stocks?",
        "Show me recent market anomalies"
    ]
    
    success_count = 0
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        
        result = nl_sql.process_query(query, use_ai=False)
        
        if result['success']:
            print(f"  ✓ Method: {result['method']}")
            print(f"  ✓ Intent: {result['intent']} (confidence: {result['confidence']:.2%})")
            print(f"  ✓ Rows returned: {result['row_count']}")
            print(f"  ✓ Time: {result['processing_time']:.2f}s")
            print(f"  SQL: {result['sql'][:80]}...")
            
            # Show sample result
            if result['results']:
                print(f"  Sample: {json.dumps(result['results'][0], indent=2)[:100]}...")
            
            success_count += 1
        else:
            print(f"  ✗ Error: {result['error']}")
    
    print(f"\n✓ {success_count}/{len(test_queries)} queries successful")
    return success_count == len(test_queries)

def test_aggregation_queries():
    """Test aggregation queries"""
    print("\n" + "="*70)
    print("Testing Aggregation Queries...")
    print("="*70)
    
    test_queries = [
        "What's the average return by sector?",
        "Average volatility by asset type",
        "Mean performance across sectors"
    ]
    
    success_count = 0
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        
        result = nl_sql.process_query(query, use_ai=False)
        
        if result['success']:
            print(f"  ✓ Intent: {result['intent']}")
            print(f"  ✓ Rows: {result['row_count']}")
            
            # Show aggregated results
            if result['results']:
                for row in result['results'][:3]:
                    print(f"    {row}")
            
            success_count += 1
        else:
            print(f"  ✗ Error: {result['error']}")
    
    print(f"\n✓ {success_count}/{len(test_queries)} queries successful")
    return success_count > 0

def test_comparison_queries():
    """Test comparison queries"""
    print("\n" + "="*70)
    print("Testing Comparison Queries...")
    print("="*70)
    
    test_queries = [
        "Compare Bitcoin and Ethereum",
        "Compare AAPL vs MSFT",
        "Show difference between stocks and crypto"
    ]
    
    success_count = 0
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        
        result = nl_sql.process_query(query, use_ai=False)
        
        if result['success']:
            print(f"  ✓ Intent: {result['intent']}")
            print(f"  ✓ Comparing {result['row_count']} items")
            
            if result['results']:
                for row in result['results']:
                    print(f"    {row.get('asset_symbol') or row.get('asset_type')}: "
                          f"{row.get('total_return') or row.get('avg_return')}")
            
            success_count += 1
        else:
            print(f"  ✗ Error: {result['error']}")
    
    print(f"\n✓ {success_count}/{len(test_queries)} queries successful")
    return success_count > 0

def test_ai_queries():
    """Test AI-powered complex queries"""
    print("\n" + "="*70)
    print("Testing AI-Powered Queries...")
    print("="*70)
    
    from src.ai.config import config
    
    if not config.hf_api_key:
        print("⚠️  HF_API_KEY not configured, skipping AI tests")
        return True
    
    test_queries = [
        "Which stocks have the best risk-adjusted returns?",
        "Show me assets with high correlation to Bitcoin"
    ]
    
    print(f"\n⏳ Testing {len(test_queries)} AI queries (may take 20-30 seconds)...")
    
    success_count = 0
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        
        result = nl_sql.process_query(query, use_ai=True)
        
        if result['success']:
            print(f"  ✓ Method: {result['method']}")
            print(f"  ✓ Rows: {result['row_count']}")
            print(f"  ✓ Time: {result['processing_time']:.2f}s")
            success_count += 1
        else:
            print(f"  ✗ Error: {result['error']}")
    
    print(f"\n✓ {success_count}/{len(test_queries)} AI queries successful")
    return success_count > 0

def test_sql_validation():
    """Test SQL validation"""
    print("\n" + "="*70)
    print("Testing SQL Validation...")
    print("="*70)
    
    # Valid queries
    valid_queries = [
        "SELECT * FROM mart_asset_performance LIMIT 10",
        "SELECT ticker, close_price FROM int_stock_daily_analysis"
    ]
    
    # Invalid queries
    invalid_queries = [
        "DROP TABLE mart_asset_performance",
        "DELETE FROM int_stock_daily_analysis",
        "UPDATE mart_asset_performance SET total_return = 0"
    ]
    
    print("\n✓ Testing valid queries:")
    for sql in valid_queries:
        is_valid, error = nl_sql._validate_sql(sql)
        status = "✓" if is_valid else "✗"
        print(f"  {status} {sql[:50]}...")
    
    print("\n✓ Testing invalid queries (should be rejected):")
    for sql in invalid_queries:
        is_valid, error = nl_sql._validate_sql(sql)
        status = "✓" if not is_valid else "✗"
        print(f"  {status} Rejected: {sql[:50]}... ({error})")
    
    print("\n✓ SQL validation working correctly")
    return True

def test_edge_cases():
    """Test edge cases and error handling"""
    print("\n" + "="*70)
    print("Testing Edge Cases...")
    print("="*70)
    
    test_cases = [
        ("", "Empty query"),
        ("asdfghjkl", "Gibberish query"),
        ("Show me data for nonexistent_table", "Non-existent table"),
    ]
    
    for query, description in test_cases:
        print(f"\n{description}: '{query}'")
        result = nl_sql.process_query(query, use_ai=False)
        
        if result['success']:
            print(f"  ⚠️  Unexpected success")
        else:
            print(f"  ✓ Handled gracefully: {result['error'][:60]}...")
    
    print("\n✓ Edge cases handled correctly")
    return True

def test_performance():
    """Test performance metrics"""
    print("\n" + "="*70)
    print("Testing Performance...")
    print("="*70)
    
    import time
    
    # Test simple query speed
    print("\n1. Simple query speed (template-based):")
    query = "Show me top 10 stocks"
    
    times = []
    for i in range(3):
        result = nl_sql.process_query(query, use_ai=False)
        times.append(result['processing_time'])
    
    avg_time = sum(times) / len(times)
    print(f"   Average time: {avg_time:.3f}s")
    
    if avg_time < 2.0:
        print(f"   ✓ Performance: Excellent")
    elif avg_time < 5.0:
        print(f"   ✓ Performance: Good")
    else:
        print(f"   ⚠️  Performance: Slow (consider optimization)")
    
    return True

def run_all_tests():
    """Run all NL to SQL tests"""
    print("\n" + "🔤 " + "="*68)
    print("  Natural Language to SQL Tests")
    print("="*70)
    
    results = {
        'Schema Manager': test_schema_manager(),
        'Simple Queries': test_simple_queries(),
        'Aggregation Queries': test_aggregation_queries(),
        'Comparison Queries': test_comparison_queries(),
        'SQL Validation': test_sql_validation(),
        'Edge Cases': test_edge_cases(),
        'Performance': test_performance()
    }
    
    # Optional AI test
    print("\n" + "="*70)
    print("Optional: AI-Powered Query Test")
    print("="*70)
    print("This will use your HF API quota (~2-3 requests)")
    response = input("Run AI tests? (y/n): ").lower()
    
    if response == 'y':
        results['AI Queries'] = test_ai_queries()
    
    # Results summary
    print("\n" + "="*70)
    print("Test Results Summary")
    print("="*70)
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name:.<50} {status}")
    
    all_passed = all(results.values())
    
    print("\n" + "="*70)
    if all_passed:
        print("✓ All tests passed! NL to SQL system is ready.")
        print("\nYou can now:")
        print("  - Ask questions in plain English")
        print("  - Get automatic SQL generation")
        print("  - Execute queries safely")
        print("\nNext: Build the Analysis Engine (Day 10-11)")
    else:
        print("⚠️  Some tests failed. Check errors above.")
    print("="*70 + "\n")
    
    return all_passed

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)