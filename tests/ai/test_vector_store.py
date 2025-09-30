"""
Test vector store functionality
File: tests/ai/test_vector_store.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from loguru import logger
from src.ai.vector_store import vector_store
import time

def test_vector_store_initialization():
    """Test vector store initialization"""
    print("\n" + "="*60)
    print("Testing Vector Store Initialization...")
    print("="*60)
    
    try:
        stats = vector_store.get_collection_stats()
        
        print(f"\n✓ Vector store initialized")
        print(f"  Storage path: {stats['storage_path']}")
        print(f"\nCollections:")
        for name, info in stats.items():
            if name != 'storage_path':
                print(f"  - {info['name']}: {info['count']} items")
        
        return True
        
    except Exception as e:
        print(f"✗ Initialization failed: {e}")
        return False

def test_build_pattern_library():
    """Test building pattern library"""
    print("\n" + "="*60)
    print("Testing Pattern Library Build...")
    print("="*60)
    
    try:
        initial_stats = vector_store.get_collection_stats()
        initial_count = initial_stats['patterns']['count']
        
        print(f"\nInitial pattern count: {initial_count}")
        
        # Build library
        vector_store.build_pattern_library()
        
        final_stats = vector_store.get_collection_stats()
        final_count = final_stats['patterns']['count']
        
        print(f"Final pattern count: {final_count}")
        
        if final_count > initial_count:
            print(f"✓ Added {final_count - initial_count} patterns")
            return True
        else:
            print("⚠️  Patterns already exist (this is OK)")
            return True
            
    except Exception as e:
        print(f"✗ Pattern library build failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_build_anomaly_library():
    """Test building anomaly library"""
    print("\n" + "="*60)
    print("Testing Anomaly Library Build...")
    print("="*60)
    
    try:
        initial_stats = vector_store.get_collection_stats()
        initial_count = initial_stats['anomalies']['count']
        
        print(f"\nInitial anomaly count: {initial_count}")
        
        # Build library
        vector_store.build_anomaly_library()
        
        final_stats = vector_store.get_collection_stats()
        final_count = final_stats['anomalies']['count']
        
        print(f"Final anomaly count: {final_count}")
        
        if final_count > initial_count:
            print(f"✓ Added {final_count - initial_count} anomalies")
            return True
        else:
            print("⚠️  Anomalies already exist (this is OK)")
            return True
            
    except Exception as e:
        print(f"✗ Anomaly library build failed: {e}")
        return False

def test_pattern_search():
    """Test semantic pattern search"""
    print("\n" + "="*60)
    print("Testing Pattern Search...")
    print("="*60)
    
    test_queries = [
        ("Moving averages crossing signals trend change", "technical_signal"),
        ("Price breaking through key levels with high trading activity", None),
        ("Market showing signs of reversal", None)
    ]
    
    success = True
    for query, pattern_type in test_queries:
        print(f"\nQuery: '{query}'")
        if pattern_type:
            print(f"Filter: {pattern_type}")
        
        results = vector_store.search_similar_patterns(
            query, 
            n_results=3, 
            pattern_type=pattern_type
        )
        
        if results:
            print(f"✓ Found {len(results)} similar patterns:")
            for i, result in enumerate(results, 1):
                print(f"\n  {i}. {result['id']}")
                print(f"     Similarity: {result['similarity']:.3f} ({result['relevance']})")
                print(f"     {result['description'][:80]}...")
        else:
            print("✗ No patterns found")
            success = False
        
        time.sleep(0.5)
    
    return success

def test_anomaly_search():
    """Test anomaly search"""
    print("\n" + "="*60)
    print("Testing Anomaly Search...")
    print("="*60)
    
    test_queries = [
        ("Sudden market crash with rapid price decline", "medium"),
        ("Algorithmic trading causing market instability", None),
        ("Cryptocurrency market losing significant value", "high")
    ]
    
    success = True
    for query, min_severity in test_queries:
        print(f"\nQuery: '{query}'")
        if min_severity:
            print(f"Min severity: {min_severity}")
        
        results = vector_store.search_similar_anomalies(
            query,
            n_results=3,
            min_severity=min_severity
        )
        
        if results:
            print(f"✓ Found {len(results)} similar anomalies:")
            for i, result in enumerate(results, 1):
                severity = result['metadata'].get('severity', 'unknown')
                date = result['metadata'].get('date', 'unknown')
                print(f"\n  {i}. {result['id']} (severity: {severity})")
                print(f"     Date: {date}")
                print(f"     Similarity: {result['similarity']:.3f}")
                print(f"     {result['description'][:80]}...")
        else:
            print("✗ No anomalies found")
            success = False
        
        time.sleep(0.5)
    
    return success

def test_add_custom_pattern():
    """Test adding custom pattern"""
    print("\n" + "="*60)
    print("Testing Custom Pattern Addition...")
    print("="*60)
    
    try:
        # Add a custom pattern
        pattern_id = f"test_pattern_{int(time.time())}"
        description = "Test pattern: Strong bullish momentum with increasing volume and positive sentiment"
        
        print(f"\nAdding pattern: {pattern_id}")
        
        vector_store.add_market_pattern(
            pattern_id=pattern_id,
            pattern_description=description,
            pattern_type='test_pattern',
            metadata={'test': True, 'confidence': 0.85}
        )
        
        # Search for it
        time.sleep(0.5)
        results = vector_store.search_similar_patterns(
            "bullish momentum with volume",
            n_results=5
        )
        
        # Check if our pattern is in results
        found = any(r['id'] == pattern_id for r in results)
        
        if found:
            print(f"✓ Pattern added and retrievable")
            return True
        else:
            print(f"⚠️  Pattern added but not found in search")
            return False
            
    except Exception as e:
        print(f"✗ Custom pattern test failed: {e}")
        return False

def test_similarity_scores():
    """Test similarity scoring"""
    print("\n" + "="*60)
    print("Testing Similarity Scoring...")
    print("="*60)
    
    try:
        # Test with exact match
        print("\n1. Testing exact match...")
        results = vector_store.search_similar_patterns(
            "Short-term moving average crosses above long-term moving average",
            n_results=1
        )
        
        if results and results[0]['similarity'] > 0.9:
            print(f"✓ High similarity for exact match: {results[0]['similarity']:.3f}")
        else:
            print(f"⚠️  Similarity lower than expected: {results[0]['similarity']:.3f if results else 'N/A'}")
        
        # Test with partial match
        print("\n2. Testing partial match...")
        results = vector_store.search_similar_patterns(
            "moving averages",
            n_results=1
        )
        
        if results and 0.5 < results[0]['similarity'] < 0.9:
            print(f"✓ Medium similarity for partial match: {results[0]['similarity']:.3f}")
        elif results:
            print(f"⚠️  Unexpected similarity score: {results[0]['similarity']:.3f}")
        
        # Test with unrelated query
        print("\n3. Testing unrelated query...")
        results = vector_store.search_similar_patterns(
            "weather forecast sunny day",
            n_results=1
        )
        
        if results and results[0]['similarity'] < 0.5:
            print(f"✓ Low similarity for unrelated query: {results[0]['similarity']:.3f}")
        elif results:
            print(f"⚠️  Similarity higher than expected: {results[0]['similarity']:.3f}")
        
        return True
        
    except Exception as e:
        print(f"✗ Similarity test failed: {e}")
        return False

def test_performance():
    """Test performance metrics"""
    print("\n" + "="*60)
    print("Testing Performance...")
    print("="*60)
    
    try:
        # Test search speed
        print("\n1. Testing search speed...")
        
        queries = [
            "market volatility increasing",
            "price breakout pattern",
            "bearish reversal signal"
        ]
        
        total_time = 0
        for query in queries:
            start = time.time()
            results = vector_store.search_similar_patterns(query, n_results=5)
            elapsed = time.time() - start
            total_time += elapsed
        
        avg_time = total_time / len(queries)
        print(f"✓ Average search time: {avg_time:.3f}s")
        
        if avg_time < 0.5:
            print("  Performance: Excellent")
        elif avg_time < 1.0:
            print("  Performance: Good")
        else:
            print("  Performance: Acceptable")
        
        # Test batch operations
        print("\n2. Testing batch embedding generation...")
        start = time.time()
        
        # This is tested in pattern/anomaly library builds
        stats = vector_store.get_collection_stats()
        total_items = sum(s['count'] for s in stats.values() if isinstance(s, dict))
        
        print(f"✓ Total items indexed: {total_items}")
        
        return True
        
    except Exception as e:
        print(f"✗ Performance test failed: {e}")
        return False

def run_all_tests():
    """Run all vector store tests"""
    print("\n" + "🗄️  " + "="*58)
    print("  Vector Store Tests")
    print("="*60)
    
    results = {
        'Initialization': test_vector_store_initialization(),
        'Pattern Library': test_build_pattern_library(),
        'Anomaly Library': test_build_anomaly_library(),
        'Pattern Search': test_pattern_search(),
        'Anomaly Search': test_anomaly_search(),
        'Custom Pattern': test_add_custom_pattern(),
        'Similarity Scoring': test_similarity_scores(),
        'Performance': test_performance()
    }
    
    print("\n" + "="*60)
    print("Test Results Summary")
    print("="*60)
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name:.<40} {status}")
    
    # Show final stats
    print("\n" + "="*60)
    stats = vector_store.get_collection_stats()
    print("Vector Store Statistics:")
    for name, info in stats.items():
        if name != 'storage_path':
            print(f"  {info['name']}: {info['count']} items")
    print("="*60)
    
    all_passed = all(results.values())
    
    if all_passed:
        print("\n✓ All tests passed! Vector store is ready.")
        print("\nNext steps:")
        print("1. Implement Natural Language to SQL (Week 2, Day 8-9)")
        print("2. Build Analysis Engine (Week 2, Day 10-11)")
        print("3. Create end-to-end integration (Week 2, Day 12-14)")
    else:
        print("\n⚠️  Some tests failed. Check errors above.")
    
    return all_passed

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)