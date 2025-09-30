"""
Test AI system setup
File: tests/ai/test_setup.py
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from loguru import logger
from src.ai.config import config
from src.ai.local_models import local_models

def test_configuration():
    """Test configuration loading"""
    print("\n" + "="*60)
    print("Testing Configuration...")
    print("="*60)
    
    try:
        print(f"✓ HF API Key present: {bool(config.hf_api_key)}")
        print(f"✓ HF Model: {config.hf_model}")
        print(f"✓ Embedding Model: {config.embedding_model}")
        print(f"✓ Database: {config.db_name}@{config.db_host}:{config.db_port}")
        print(f"✓ Chroma Directory: {config.chroma_persist_dir}")
        return True
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False

def test_local_models():
    """Test local model loading"""
    print("\n" + "="*60)
    print("Testing Local Models...")
    print("="*60)
    
    try:
        # Test embedding generation
        print("\n1. Testing embedding generation...")
        test_texts = ["Bitcoin price rising", "Stock market volatility"]
        embeddings = local_models.generate_embeddings(test_texts)
        print(f"✓ Generated embeddings: shape {embeddings.shape}")
        
        # Test sentiment analysis
        print("\n2. Testing sentiment analysis...")
        sentiment = local_models.analyze_sentiment("The stock market is performing exceptionally well")
        print(f"✓ Sentiment: {sentiment['label']} (confidence: {sentiment['score']:.2f})")
        
        # Test query classification
        print("\n3. Testing query classification...")
        intent = local_models.classify_query_intent(
            "Show me the top stocks",
            ["data_retrieval", "aggregation", "comparison"]
        )
        print(f"✓ Intent: {intent['intent']} (confidence: {intent['confidence']:.2f})")
        
        # Test similarity computation
        print("\n4. Testing similarity computation...")
        query_emb = embeddings[0]
        corpus_emb = embeddings
        similarities = local_models.compute_similarity(query_emb, corpus_emb)
        print(f"✓ Computed similarities: {similarities}")
        
        # Model info
        print("\n5. Model information...")
        model_info = local_models.get_model_info()
        for model_type, info in model_info.items():
            print(f"   {model_type}: {info['name']} (loaded: {info['loaded']})")
        
        return True
        
    except Exception as e:
        print(f"✗ Local models test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_memory_usage():
    """Test memory usage of models"""
    print("\n" + "="*60)
    print("Memory Usage Check...")
    print("="*60)
    
    try:
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        print(f"Current memory usage: {memory_mb:.2f} MB")
        
        if memory_mb > 2000:
            print("⚠️  Warning: High memory usage. Consider using smaller batch sizes.")
        else:
            print("✓ Memory usage is acceptable")
        
        return True
        
    except ImportError:
        print("⚠️  psutil not installed. Skipping memory check.")
        print("   Install with: pip install psutil")
        return True
    except Exception as e:
        print(f"⚠️  Memory check failed: {e}")
        return True

def run_all_tests():
    """Run all setup tests"""
    print("\n" + "🤖 " + "="*58)
    print("  Phase 3 AI System - Setup Tests")
    print("="*60)
    
    results = {
        'Configuration': test_configuration(),
        'Local Models': test_local_models(),
        'Memory Usage': test_memory_usage()
    }
    
    print("\n" + "="*60)
    print("Test Results Summary")
    print("="*60)
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name:.<40} {status}")
    
    all_passed = all(results.values())
    
    print("\n" + "="*60)
    if all_passed:
        print("✓ All tests passed! AI system is ready.")
        print("\nNext steps:")
        print("1. Implement Hugging Face API integration (Day 3-4)")
        print("2. Build vector store (Day 5-7)")
        print("3. Create NL to SQL system (Week 2)")
    else:
        print("✗ Some tests failed. Please check the errors above.")
    print("="*60 + "\n")
    
    return all_passed

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)