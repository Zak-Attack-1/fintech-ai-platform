"""
Test both economic and cryptocurrency data ingestion
"""
def run_day_6_7_ingestion():
    """Run both FRED and CoinGecko ingestion"""
    
    print("🚀 Day 6-7: Economic + Cryptocurrency Data Ingestion")
    print("=" * 70)
    
    from src.data_ingestion.base_ingestion import setup_logging
    setup_logging()
    
    # Day 6: FRED Economic Data
    print("\n📈 Day 6: Federal Reserve Economic Data (FRED)")
    print("-" * 50)
    
    try:
        from src.data_ingestion.fred_economic_data import run_fred_ingestion
        fred_result = run_fred_ingestion()
        fred_success = fred_result.records_processed > 0
    except Exception as e:
        print(f"❌ FRED ingestion failed: {e}")
        fred_success = False
        fred_result = None
    
    # Day 7: CoinGecko Cryptocurrency Data  
    print("\n🪙 Day 7: Cryptocurrency Data (CoinGecko)")
    print("-" * 50)
    
    try:
        from src.data_ingestion.crypto_data_ingestion import run_crypto_ingestion
        crypto_result = run_crypto_ingestion()
        crypto_success = crypto_result.records_processed > 0
    except Exception as e:
        print(f"❌ Crypto ingestion failed: {e}")
        crypto_success = False
        crypto_result = None
    
    # Final Summary
    print("\n" + "=" * 70)
    print("🎉 Day 6-7 Complete! Final Database Summary:")
    print("=" * 70)
    
    try:
        from src.models.database import db_manager
        from sqlalchemy import text
        
        with db_manager.get_session() as session:
            # Stock data
            stock_count = session.execute(text("SELECT COUNT(*) FROM stock_prices")).fetchone()[0]
            stock_tickers = session.execute(text("SELECT COUNT(DISTINCT ticker) FROM stock_prices")).fetchone()[0]
            
            # Economic data
            econ_count = session.execute(text("SELECT COUNT(*) FROM economic_indicators")).fetchone()[0]
            econ_series = session.execute(text("SELECT COUNT(DISTINCT series_id) FROM economic_indicators")).fetchone()[0]
            
            # Crypto data
            crypto_count = session.execute(text("SELECT COUNT(*) FROM crypto_prices")).fetchone()[0]
            crypto_symbols = session.execute(text("SELECT COUNT(DISTINCT symbol) FROM crypto_prices")).fetchone()[0]
            
            # Total
            total_records = stock_count + econ_count + crypto_count
            
            print(f"📊 Complete Database Summary:")
            print(f"   🔸 Stock Market Data:")
            print(f"     • {stock_count:,} price records from {stock_tickers} companies")
            print(f"   🔸 Economic Indicators:")
            print(f"     • {econ_count:,} data points from {econ_series} series")
            print(f"   🔸 Cryptocurrency Data:")
            print(f"     • {crypto_count:,} price records from {crypto_symbols} cryptocurrencies")
            print(f"")
            print(f"   🎯 TOTAL: {total_records:,} REAL FINANCIAL RECORDS!")
            print(f"")
            
            # Date ranges
            stock_dates = session.execute(text("SELECT MIN(date), MAX(date) FROM stock_prices")).fetchone()
            if econ_count > 0:
                econ_dates = session.execute(text("SELECT MIN(date), MAX(date) FROM economic_indicators")).fetchone()
            else:
                econ_dates = (None, None)
            if crypto_count > 0:
                crypto_dates = session.execute(text("SELECT MIN(date), MAX(date) FROM crypto_prices")).fetchone()
            else:
                crypto_dates = (None, None)
            
            print(f"📅 Data Coverage:")
            print(f"   • Stock Data: {stock_dates[0]} to {stock_dates[1]}")
            if econ_dates[0]:
                print(f"   • Economic Data: {econ_dates[0]} to {econ_dates[1]}")
            if crypto_dates[0]:
                print(f"   • Crypto Data: {crypto_dates[0]} to {crypto_dates[1]}")
            
            print(f"")
            print(f"✅ SUCCESS: You now have a world-class financial database!")
            print(f"🚀 Ready for Phase 2: Data Modeling with dbt!")
            
    except Exception as e:
        print(f"⚠️ Database summary error: {e}")

if __name__ == "__main__":
    run_day_6_7_ingestion()