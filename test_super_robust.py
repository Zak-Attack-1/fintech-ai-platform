"""
Test the super robust ingestion with all fixes
"""
import subprocess
import sys

def update_packages():
    """Update to latest yfinance and install additional packages"""
    print("🔄 Updating packages...")
    
    packages = [
        "yfinance==0.2.54",
        "requests-cache==1.1.1", 
        "ratelimit==2.2.1"
    ]
    
    for package in packages:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"✅ Installed {package}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install {package}: {e}")

def test_super_robust():
    """Test the super robust ingestion"""
    from src.data_ingestion.stock_ingestion_fixed import run_super_robust_ingestion
    from src.data_ingestion.base_ingestion import setup_logging
    
    setup_logging()
    
    print("🚀 Testing Super Robust Stock Ingestion...")
    
    result = run_super_robust_ingestion()
    
    if result.records_processed > 0:
        print("🎯 SUCCESS! Ready to scale up to more data.")
        
        # Verify in database
        from src.models.database import db_manager
        from sqlalchemy import text
        
        with db_manager.get_session() as session:
            count = session.execute(text("SELECT COUNT(*) FROM stock_prices")).fetchone()[0]
            tickers = session.execute(text("SELECT DISTINCT ticker FROM stock_prices ORDER BY ticker")).fetchall()
            
            print(f"📊 Database verification:")
            print(f"   • Total records: {count:,}")
            print(f"   • Unique tickers: {len(tickers)}")
            print(f"   • Tickers: {[t[0] for t in tickers[:10]]}")
    else:
        print("⚠️  No data ingested - may need to wait for APIs to recover")

if __name__ == "__main__":
    # Step 1: Update packages
    update_packages()
    
    # Step 2: Test ingestion
    test_super_robust()