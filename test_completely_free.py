"""
Test 100% free real data ingestion
"""
def test_completely_free_data():
    """Test the completely free data ingestion"""
    from src.data_ingestion.completely_free_stock_data import run_completely_free_ingestion
    from src.data_ingestion.base_ingestion import setup_logging
    
    setup_logging()
    
    print("🚀 Testing 100% Free Real Stock Data Ingestion...")
    print("=" * 70)
    print("📋 Sources being tested:")
    print("   1. Stooq (Polish Exchange) - Most reliable")
    print("   2. Yahoo Finance Web Endpoints - Public data")
    print("   3. Financial Modeling Prep Free - No API key")
    print("   4. European Central Bank - Economic data")
    print("=" * 70)
    
    result = run_completely_free_ingestion()
    
    if result.records_processed > 0:
        print("\n🎯 SUCCESS! Real data obtained for free!")
        
        # Verify in database
        try:
            from src.models.database import db_manager
            from sqlalchemy import text
            
            with db_manager.get_session() as session:
                # Get total count
                count = session.execute(text("SELECT COUNT(*) FROM stock_prices")).fetchone()[0]
                
                # Get ticker breakdown
                tickers = session.execute(text("""
                    SELECT ticker, COUNT(*) as records,
                           MIN(date) as first_date, MAX(date) as last_date
                    FROM stock_prices 
                    GROUP BY ticker 
                    ORDER BY records DESC
                """)).fetchall()
                
                print(f"\n📊 Database Verification:")
                print(f"   • Total stock records: {count:,}")
                print(f"   • Number of tickers: {len(tickers)}")
                print(f"   • Ticker breakdown:")
                
                for ticker, record_count, first_date, last_date in tickers:
                    print(f"     - {ticker}: {record_count:,} records ({first_date} to {last_date})")
                
                print(f"\n✅ SUCCESS! {count:,} real financial records obtained 100% free!")
                print(f"📈 Ready to proceed with economic data (Day 6)!")
                
        except Exception as e:
            print(f"⚠️  Database verification error: {e}")
    
    else:
        print(f"\n⚠️  No data obtained - may need to try different approach")
        print(f"💡 Check if sources are accessible from your location")

if __name__ == "__main__":
    test_completely_free_data()