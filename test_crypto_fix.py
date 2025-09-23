"""
Test the fixed cryptocurrency ingestion
"""
def test_fixed_crypto():
    """Test the fixed CoinGecko ingestion"""
    
    print("🔧 Testing Fixed CoinGecko Cryptocurrency Ingestion...")
    print("=" * 60)
    print("🛠️  Fixes Applied:")
    print("   • Fixed datetime comparison errors")
    print("   • Updated crypto IDs to working ones")  
    print("   • More conservative rate limiting")
    print("   • Better error handling")
    print("=" * 60)
    
    from src.data_ingestion.crypto_data_ingestion_fixed import run_fixed_crypto_ingestion
    from src.data_ingestion.base_ingestion import setup_logging
    
    setup_logging()
    
    result = run_fixed_crypto_ingestion()
    
    if result.records_processed > 0:
        print(f"\n🎯 SUCCESS! Fixed crypto ingestion working!")
        
        # Verify in database
        try:
            from src.models.database import db_manager
            from sqlalchemy import text
            
            with db_manager.get_session() as session:
                # Get crypto data
                crypto_count = session.execute(text("SELECT COUNT(*) FROM crypto_prices")).fetchone()[0]
                crypto_symbols = session.execute(text("""
                    SELECT symbol, COUNT(*) as records,
                           MIN(date) as first_date, MAX(date) as last_date,
                           ROUND(AVG(price_usd::numeric), 2) as avg_price
                    FROM crypto_prices 
                    GROUP BY symbol 
                    ORDER BY records DESC
                """)).fetchall()
                
                print(f"\n📊 Cryptocurrency Database Summary:")
                print(f"   • Total crypto records: {crypto_count:,}")
                print(f"   • Number of cryptocurrencies: {len(crypto_symbols)}")
                print(f"   • Crypto breakdown:")
                
                for symbol, record_count, first_date, last_date, avg_price in crypto_symbols[:10]:
                    print(f"     - {symbol}: {record_count:,} records (${avg_price} avg) ({first_date} to {last_date})")
                
                # Update total database summary
                stock_count = session.execute(text("SELECT COUNT(*) FROM stock_prices")).fetchone()[0]
                econ_count = session.execute(text("SELECT COUNT(*) FROM economic_indicators")).fetchone()[0]
                total_records = stock_count + econ_count + crypto_count
                
                print(f"\n🎯 COMPLETE DATABASE SUMMARY:")
                print(f"   📈 Stock Market: {stock_count:,} records")
                print(f"   📊 Economic Data: {econ_count:,} records")  
                print(f"   🪙 Cryptocurrency: {crypto_count:,} records")
                print(f"   🔥 TOTAL: {total_records:,} REAL FINANCIAL RECORDS!")
                
        except Exception as e:
            print(f"⚠️  Database verification error: {e}")
    
    else:
        print(f"\n⚠️  Still having issues - may need to try different approach")

if __name__ == "__main__":
    test_fixed_crypto()