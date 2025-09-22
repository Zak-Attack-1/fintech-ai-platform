"""
Test script for stock data ingestion
"""
from src.data_ingestion.stock_ingestion import SP500DataIngestion
from src.data_ingestion.base_ingestion import setup_logging

def test_small_batch():
    """Test with just a few stocks first"""
    setup_logging()
    
    print("🔍 Testing stock ingestion with small batch...")
    
    # Test with just 5 major stocks
    test_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA']
    
    ingestion = SP500DataIngestion()
    
    # Test for just the last 30 days to start
    from datetime import date, timedelta
    start_date = (date.today() - timedelta(days=30)).strftime('%Y-%m-%d')
    end_date = date.today().strftime('%Y-%m-%d')
    
    result = ingestion.run_ingestion(
        tickers=test_tickers,
        start_date=start_date,
        end_date=end_date,
        include_fundamentals=True
    )
    
    print(f"""
    ✅ Test Complete!
    
    📊 Results:
    • Tickers: {test_tickers}
    • Date Range: {start_date} to {end_date}
    • Records Processed: {result.records_processed:,}
    • Records Inserted: {result.records_inserted:,}
    • Duration: {result.duration_seconds:.2f} seconds
    • Success Rate: {result.success_rate:.1%}
    
    🎯 Next Step: Run full S&P 500 ingestion
    """)
    
    return result

if __name__ == "__main__":
    test_small_batch()