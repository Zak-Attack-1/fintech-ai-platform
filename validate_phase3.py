"""
Complete Phase 3 validation script
Run this to verify everything is set up correctly
File: validate_phase3.py
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

import os
from datetime import datetime
from loguru import logger

# Configure logger
logger.remove()
logger.add(sys.stdout, level="INFO")

def check_dependencies():
    """Check all required packages are installed"""
    print("\n" + "="*70)
    print("Checking Dependencies...")
    print("="*70)
    
    required_packages = [
        'transformers',
        'sentence_transformers',
        'chromadb',
        'langchain',
        'huggingface_hub',
        'faiss',
        'sqlparse',
        'numpy',
        'pandas',
        'psycopg2',
        'loguru',
        'dotenv'
    ]
    
    missing = []
    installed = []
    
    for package in required_packages:
        try:
            if package == 'dotenv':
                __import__('dotenv')
            else:
                __import__(package.replace('-', '_'))
            installed.append(package)
            print(f"  ✓ {package}")
        except ImportError:
            missing.append(package)
            print(f"  ✗ {package} - NOT FOUND")
    
    if missing:
        print(f"\n⚠️  Missing {len(missing)} packages:")
        print(f"   pip install {' '.join(missing)}")
        return False
    
    print(f"\n✓ All {len(installed)} dependencies installed")
    return True

def check_directory_structure():
    """Check directory structure exists"""
    print("\n" + "="*70)
    print("Checking Directory Structure...")
    print("="*70)
    
    required_dirs = [
        'src/ai',
        'tests/ai',
        'data/chroma'
    ]
    
    required_files = [
        'src/ai/__init__.py',
        'src/ai/config.py',
        'src/ai/local_models.py',
        'src/ai/hf_api.py',
        'src/ai/vector_store.py',
        'tests/ai/__init__.py',
        'tests/ai/test_setup.py',
        'tests/ai/test_hf_api.py',
        'tests/ai/test_vector_store.py'
    ]
    
    all_exist = True
    
    # Check directories
    for dir_path in required_dirs:
        if os.path.exists(dir_path):
            print(f"  ✓ {dir_path}/")
        else:
            print(f"  ✗ {dir_path}/ - MISSING")
            all_exist = False
    
    # Check files
    for file_path in required_files:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"  ✓ {file_path} ({size:,} bytes)")
        else:
            print(f"  ✗ {file_path} - MISSING")
            all_exist = False
    
    if all_exist:
        print(f"\n✓ All directories and files exist")
    else:
        print(f"\n⚠️  Some files/directories are missing")
    
    return all_exist

def check_configuration():
    """Check configuration is valid"""
    print("\n" + "="*70)
    print("Checking Configuration...")
    print("="*70)
    
    try:
        from src.ai.config import config
        
        checks = {
            'HF API Key': bool(config.hf_api_key),
            'Database Host': bool(config.db_host),
            'Database Port': bool(config.db_port),
            'Database Name': bool(config.db_name),
            'Database User': bool(config.db_user),
            'Database Password': bool(config.db_password),
            'Chroma Directory': bool(config.chroma_persist_dir)
        }
        
        all_valid = True
        for check_name, valid in checks.items():
            status = "✓" if valid else "✗"
            print(f"  {status} {check_name}")
            if not valid:
                all_valid = False
        
        if not config.hf_api_key:
            print("\n⚠️  HF_API_KEY not set!")
            print("   Get free key: https://huggingface.co/settings/tokens")
            print("   Add to .env: HF_API_KEY=hf_your_key_here")
        
        if all_valid:
            print(f"\n✓ Configuration valid")
        else:
            print(f"\n⚠️  Configuration incomplete")
        
        return all_valid
        
    except Exception as e:
        print(f"  ✗ Configuration error: {e}")
        return False

def check_local_models():
    """Check local models can load"""
    print("\n" + "="*70)
    print("Checking Local Models...")
    print("="*70)
    
    try:
        from src.ai.local_models import local_models
        
        print("  Testing embedding model...")
        embeddings = local_models.generate_embeddings(["test"])
        print(f"    ✓ Embeddings: shape {embeddings.shape}")
        
        print("  Testing sentiment model...")
        sentiment = local_models.analyze_sentiment("positive market")
        print(f"    ✓ Sentiment: {sentiment['label']}")
        
        print("  Testing classifier...")
        intent = local_models.classify_query_intent(
            "show data",
            ["data_retrieval", "aggregation"]
        )
        print(f"    ✓ Classification: {intent['intent']}")
        
        print(f"\n✓ All local models working")
        return True
        
    except Exception as e:
        print(f"  ✗ Local models error: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_hf_api():
    """Check Hugging Face API"""
    print("\n" + "="*70)
    print("Checking Hugging Face API...")
    print("="*70)
    
    try:
        from src.ai.hf_api import hf_api
        from src.ai.config import config
        
        if not config.hf_api_key:
            print("  ⚠️  API key not configured (optional for testing)")
            return True
        
        print("  Testing API connection...")
        stats = hf_api.get_usage_stats()
        print(f"    ✓ API connected")
        print(f"    Requests today: {stats['requests_today']}")
        print(f"    Requests month: {stats['requests_month']}")
        
        print(f"\n✓ HF API ready")
        return True
        
    except Exception as e:
        print(f"  ✗ HF API error: {e}")
        return False

def check_vector_store():
    """Check vector store"""
    print("\n" + "="*70)
    print("Checking Vector Store...")
    print("="*70)
    
    try:
        from src.ai.vector_store import vector_store
        
        stats = vector_store.get_collection_stats()
        
        print(f"  Storage path: {stats['storage_path']}")
        print(f"\n  Collections:")
        for name, info in stats.items():
            if name != 'storage_path':
                print(f"    ✓ {info['name']}: {info['count']} items")
        
        if stats['patterns']['count'] == 0:
            print(f"\n  ⚠️  No patterns loaded. Run:")
            print(f"     vector_store.build_pattern_library()")
        
        if stats['anomalies']['count'] == 0:
            print(f"  ⚠️  No anomalies loaded. Run:")
            print(f"     vector_store.build_anomaly_library()")
        
        print(f"\n✓ Vector store initialized")
        return True
        
    except Exception as e:
        print(f"  ✗ Vector store error: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_database_connection():
    """Check database connection"""
    print("\n" + "="*70)
    print("Checking Database Connection...")
    print("="*70)
    
    try:
        from src.ai.config import config
        import psycopg2
        
        conn_params = config.get_db_connection_params()
        print(f"  Connecting to {conn_params['host']}:{conn_params['port']}/{conn_params['database']}...")
        
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"    ✓ Connected: {version[:50]}...")
        
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
        table_count = cursor.fetchone()[0]
        print(f"    ✓ Tables found: {table_count}")
        
        cursor.close()
        conn.close()
        
        print(f"\n✓ Database connection working")
        return True
        
    except Exception as e:
        print(f"  ✗ Database connection failed: {e}")
        print(f"\n  Make sure Phase 2 database is running:")
        print(f"    docker ps")
        return False

def run_quick_tests():
    """Run quick integration test"""
    print("\n" + "="*70)
    print("Running Quick Integration Test...")
    print("="*70)
    
    try:
        from src.ai.local_models import local_models
        from src.ai.vector_store import vector_store
        
        # Test 1: Embedding + Search
        print("\n  Test 1: Embedding generation and search...")
        query = "market volatility increasing rapidly"
        results = vector_store.search_similar_patterns(query, n_results=3)
        
        if results:
            print(f"    ✓ Found {len(results)} similar patterns")
            print(f"    Top match: {results[0]['id']} (similarity: {results[0]['similarity']:.3f})")
        else:
            print(f"    ⚠️  No patterns found (build library first)")
        
        # Test 2: Sentiment analysis
        print("\n  Test 2: Sentiment analysis...")
        sentiment = local_models.analyze_sentiment("The market is showing strong bullish momentum")
        print(f"    ✓ Sentiment: {sentiment['label']} (confidence: {sentiment['score']:.2f})")
        
        print(f"\n✓ Integration test passed")
        return True
        
    except Exception as e:
        print(f"  ✗ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def generate_report(results):
    """Generate final report"""
    print("\n" + "="*70)
    print("PHASE 3 VALIDATION REPORT")
    print("="*70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("")
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name:.<50} {status}")
    
    total_tests = len(results)
    passed_tests = sum(1 for p in results.values() if p)
    
    print("="*70)
    print(f"Results: {passed_tests}/{total_tests} tests passed ({passed_tests/total_tests*100:.0f}%)")
    print("="*70)
    
    if all(results.values()):
        print("\n🎉 SUCCESS! Phase 3 Week 1 is complete!")
        print("\nYou are ready for Week 2:")
        print("  1. Natural Language to SQL (Day 8-9)")
        print("  2. Analysis Engine (Day 10-11)")
        print("  3. Integration & Testing (Day 12-14)")
        print("\nNext command: Start implementing nl_to_sql.py")
    else:
        print("\n⚠️  Some tests failed. Please fix issues above.")
        print("\nCommon fixes:")
        print("  - Install missing packages: pip install <package>")
        print("  - Set HF_API_KEY in .env file")
        print("  - Start Phase 2 database: docker-compose up -d")
        print("  - Build pattern libraries: Run test scripts")

def main():
    """Main validation function"""
    print("\n" + "🤖 " + "="*68)
    print("  PHASE 3 - AI SYSTEM VALIDATION")
    print("  Week 1 Completion Check")
    print("="*70)
    
    results = {}
    
    # Run all checks
    results['Dependencies'] = check_dependencies()
    results['Directory Structure'] = check_directory_structure()
    results['Configuration'] = check_configuration()
    results['Local Models'] = check_local_models()
    results['HF API'] = check_hf_api()
    results['Vector Store'] = check_vector_store()
    results['Database Connection'] = check_database_connection()
    results['Integration Test'] = run_quick_tests()
    
    # Generate report
    generate_report(results)
    
    return all(results.values())

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)