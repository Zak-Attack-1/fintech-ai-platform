"""
Build pattern and anomaly libraries for vector store
File: build_libraries.py (run once)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.ai.vector_store import vector_store
from loguru import logger

def main():
    print("\n" + "="*70)
    print("Building Pattern & Anomaly Libraries")
    print("="*70)
    
    # Build pattern library
    print("\n1. Building pattern library...")
    vector_store.build_pattern_library()
    
    # Build anomaly library
    print("\n2. Building anomaly library...")
    vector_store.build_anomaly_library()
    
    # Show statistics
    print("\n" + "="*70)
    print("Library Statistics")
    print("="*70)
    stats = vector_store.get_collection_stats()
    
    for name, info in stats.items():
        if name != 'storage_path':
            print(f"  ✓ {info['name']}: {info['count']} items")
    
    print("\n" + "="*70)
    print("✓ Libraries built successfully!")
    print("="*70)
    
    # Test search
    print("\n3. Testing pattern search...")
    results = vector_store.search_similar_patterns(
        "price breaking resistance with volume",
        n_results=3
    )
    
    if results:
        print(f"\n✓ Found {len(results)} patterns:")
        for i, result in enumerate(results, 1):
            print(f"  {i}. {result['id']} (similarity: {result['similarity']:.2%})")
    
    print("\n✓ All done! Vector store is ready.\n")

if __name__ == "__main__":
    main()