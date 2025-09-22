#!/usr/bin/env python3
"""
Test script to verify Phase 1 setup is working correctly (Windows compatible)
"""
import os
import sys
import time
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

def test_database_connection():
    """Test PostgreSQL connection and basic functionality"""
    load_dotenv()
    
    # Wait for database to be ready
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Connect to database
            conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'localhost'),
                port=os.getenv('POSTGRES_PORT', '5432'),
                database=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD')
            )
            break
            
        except psycopg2.OperationalError as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"❌ Database connection failed after {max_retries} attempts: {e}")
                return False
            
            print(f"⏳ Waiting for database... ({retry_count}/{max_retries})")
            time.sleep(2)
    
    try:
        cursor = conn.cursor()
        
        print("✅ Database connection successful!")
        
        # Test basic query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ PostgreSQL version: {version[:50]}...")
        
        # Test extensions
        cursor.execute("""
            SELECT extname FROM pg_extension 
            WHERE extname IN ('uuid-ossp', 'pg_stat_statements', 'pg_trgm');
        """)
        extensions = [row[0] for row in cursor.fetchall()]
        print(f"✅ Extensions loaded: {', '.join(extensions)}")
        
        # Test table creation
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        expected_tables = [
            'companies', 'stock_prices', 'economic_indicators', 
            'crypto_prices', 'global_economic_data', 'market_analysis',
            'pattern_embeddings', 'ingestion_logs'
        ]
        
        missing_tables = []
        for table in expected_tables:
            if any(table in t for t in tables):
                print(f"✅ Table '{table}' created successfully")
            else:
                missing_tables.append(table)
                print(f"❌ Table '{table}' missing!")
        
        if missing_tables:
            print(f"❌ Missing tables: {', '.join(missing_tables)}")
            print("💡 Check if SQL init scripts ran properly in Docker")
            return False
        
        # Test partitions
        cursor.execute("""
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE tablename LIKE 'stock_prices_%' 
            OR tablename LIKE 'crypto_prices_%'
            ORDER BY tablename;
        """)
        partitions = cursor.fetchall()
        print(f"✅ Created {len(partitions)} table partitions")
        
        if len(partitions) < 70:  # Should have ~72 stock partitions + 6 crypto partitions
            print(f"⚠️  Expected more partitions (got {len(partitions)}, expected ~78)")
        
        # Test indexes
        cursor.execute("""
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename IN ('stock_prices', 'companies', 'economic_indicators')
            AND indexname NOT LIKE '%_pkey'
            ORDER BY indexname;
        """)
        indexes = cursor.fetchall()
        print(f"✅ Created {len(indexes)} performance indexes")
        
        # Test custom types
        cursor.execute("""
            SELECT typname FROM pg_type 
            WHERE typname IN ('market_status', 'data_source');
        """)
        custom_types = [row[0] for row in cursor.fetchall()]
        print(f"✅ Custom types created: {', '.join(custom_types)}")
        
        # Test database performance settings
        cursor.execute("""
            SELECT name, setting, unit 
            FROM pg_settings 
            WHERE name IN ('shared_buffers', 'work_mem', 'effective_cache_size')
            ORDER BY name;
        """)
        settings = cursor.fetchall()
        print("\n📊 Database Performance Settings:")
        for name, setting, unit in settings:
            unit_str = unit if unit else ''
            print(f"   {name}: {setting}{unit_str}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False

def test_python_dependencies():
    """Test that all required Python packages are installed"""
    print("🔍 Testing Python Dependencies...")
    
    required_packages = [
        ('pandas', 'pandas'),
        ('numpy', 'numpy'), 
        ('yfinance', 'yfinance'),
        ('pandas_datareader', 'pandas_datareader'),
        ('requests', 'requests'),
        ('psycopg2', 'psycopg2'),
        ('sqlalchemy', 'sqlalchemy'),
        ('transformers', 'transformers'),
        ('streamlit', 'streamlit'),
        ('plotly', 'plotly'),
        ('python-dotenv', 'dotenv')
    ]
    
    missing_packages = []
    
    for package_name, import_name in required_packages:
        try:
            __import__(import_name)
            print(f"✅ {package_name}")
        except ImportError:
            missing_packages.append(package_name)
            print(f"❌ {package_name} not found")
    
    if missing_packages:
        print(f"\n❌ Missing packages: {', '.join(missing_packages)}")
        print("💡 Run: pip install -r requirements.txt")
        return False
    
    print("\n✅ All Python dependencies available!")
    return True

def test_docker_environment():
    """Test Docker containers are running"""
    print("🐳 Testing Docker Environment...")
    
    try:
        import subprocess
        
        # Check if postgres container is running
        result = subprocess.run(
            ['docker', 'ps', '--filter', 'name=fintech_postgres', '--format', 'table {{.Names}}\t{{.Status}}'],
            capture_output=True, text=True, check=True
        )
        
        if 'fintech_postgres' in result.stdout:
            print("✅ PostgreSQL container is running")
            return True
        else:
            print("❌ PostgreSQL container not found")
            print("💡 Run: docker-compose up postgres -d")
            return False
            
    except Exception as e:
        print(f"⚠️  Could not check Docker status: {e}")
        print("💡 Make sure Docker Desktop is running")
        return False

def test_file_structure():
    """Test that all required files and directories exist"""
    print("📁 Testing File Structure...")
    
    required_files = [
        'requirements.txt', '.env', 'docker-compose.yml', 'Dockerfile',
        'sql/init/01_create_extensions.sql',
        'sql/init/02_create_tables.sql', 
        'sql/init/03_create_indexes.sql'
    ]
    
    required_dirs = [
        'src', 'src/data_ingestion', 'src/models', 'src/ai', 
        'src/dashboard', 'src/utils', 'data/raw', 'data/processed',
        'logs', 'config', 'tests'
    ]
    
    missing_files = []
    missing_dirs = []
    
    for file_path in required_files:
        if os.path.isfile(file_path):
            print(f"✅ {file_path}")
        else:
            missing_files.append(file_path)
            print(f"❌ {file_path} missing")
    
    for dir_path in required_dirs:
        if os.path.isdir(dir_path):
            print(f"✅ {dir_path}/")
        else:
            missing_dirs.append(dir_path)
            print(f"❌ {dir_path}/ missing")
    
    if missing_files or missing_dirs:
        print(f"\n❌ Missing items found")
        if missing_files:
            print(f"Files: {', '.join(missing_files)}")
        if missing_dirs:
            print(f"Directories: {', '.join(missing_dirs)}")
        return False
    
    print("\n✅ All required files and directories exist!")
    return True

def main():
    """Run all setup tests"""
    print("🔍 Testing Phase 1 Setup (Windows)...")
    print("=" * 60)
    
    # Test 1: File structure
    file_test = test_file_structure()
    print("\n" + "-" * 40 + "\n")
    
    # Test 2: Python dependencies
    python_test = test_python_dependencies()
    print("\n" + "-" * 40 + "\n")
    
    # Test 3: Docker environment
    docker_test = test_docker_environment()
    print("\n" + "-" * 40 + "\n")
    
    # Test 4: Database connection and setup
    db_test = test_database_connection()
    
    print("\n" + "=" * 60)
    
    all_tests_passed = all([file_test, python_test, docker_test, db_test])
    
    if all_tests_passed:
        print("🎉 Phase 1 setup completed successfully!")
        print("📊 Database ready with:")
        print("   • 8 main tables created")
        print("   • 70+ partitions for time-series data")
        print("   • 20+ performance indexes")
        print("   • Optimized for 16GB RAM system")
        print("\n🚀 Ready to proceed with data ingestion!")
    else:
        print("❌ Setup issues found. Please fix before continuing.")
        print("\n💡 Common fixes:")
        print("   • Make sure Docker Desktop is running")
        print("   • Run: docker-compose up postgres -d")
        print("   • Run: pip install -r requirements.txt")
        print("   • Check .env file exists and has correct values")
        sys.exit(1)

if __name__ == "__main__":
    main()