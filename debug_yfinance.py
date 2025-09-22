"""
Debug script to test different yfinance approaches and find working solution
"""
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time

def test_yfinance_methods():
    """Test different ways to get Yahoo Finance data"""
    
    test_ticker = "AAPL"
    print(f"🔍 Testing different methods for {test_ticker}...")
    print("=" * 50)
    
    # Method 1: Standard approach
    print("Method 1: Standard history() call")
    try:
        stock = yf.Ticker(test_ticker)
        hist = stock.history(period="1mo")
        if not hist.empty:
            print(f"✅ Success: {len(hist)} records")
            print(f"Date range: {hist.index.min()} to {hist.index.max()}")
            print(f"Columns: {list(hist.columns)}")
        else:
            print("❌ Empty data")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "-" * 30 + "\n")
    
    # Method 2: Bulk download
    print("Method 2: Bulk download")
    try:
        data = yf.download(test_ticker, period="1mo", group_by=None)
        if not data.empty:
            print(f"✅ Success: {len(data)} records")
            print(f"Date range: {data.index.min()} to {data.index.max()}")
            print(f"Columns: {list(data.columns)}")
        else:
            print("❌ Empty data")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "-" * 30 + "\n")
    
    # Method 3: Different period
    print("Method 3: Different time periods")
    periods = ["5d", "1mo", "3mo", "6mo", "1y"]
    
    for period in periods:
        try:
            stock = yf.Ticker(test_ticker)
            hist = stock.history(period=period)
            if not hist.empty:
                print(f"✅ Period {period}: {len(hist)} records")
            else:
                print(f"❌ Period {period}: Empty")
        except Exception as e:
            print(f"❌ Period {period}: Error - {e}")
    
    print("\n" + "-" * 30 + "\n")
    
    # Method 4: Test multiple tickers
    print("Method 4: Multiple tickers")
    tickers = ["AAPL", "MSFT", "GOOGL"]
    
    try:
        data = yf.download(tickers, period="1mo", group_by='ticker')
        if not data.empty:
            print(f"✅ Multi-ticker success: {data.shape}")
            print(f"Tickers found: {data.columns.levels[1].tolist() if hasattr(data.columns, 'levels') else 'Single ticker'}")
        else:
            print("❌ Multi-ticker: Empty data")
    except Exception as e:
        print(f"❌ Multi-ticker error: {e}")
    
    print("\n" + "-" * 30 + "\n")
    
    # Method 5: Basic info test
    print("Method 5: Basic ticker info")
    try:
        stock = yf.Ticker(test_ticker)
        info = stock.info
        if info and len(info) > 5:
            print(f"✅ Info available: {len(info)} fields")
            print(f"Company: {info.get('longName', 'Unknown')}")
            print(f"Market Cap: {info.get('marketCap', 'Unknown')}")
        else:
            print("❌ No info available")
    except Exception as e:
        print(f"❌ Info error: {e}")

def test_alternative_data_source():
    """Test pandas_datareader as alternative"""
    print("\n" + "=" * 50)
    print("🔄 Testing Alternative: pandas_datareader")
    print("=" * 50)
    
    try:
        import pandas_datareader as pdr
        
        # Test Yahoo via pandas_datareader
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        data = pdr.get_data_yahoo("AAPL", start=start_date, end=end_date)
        
        if not data.empty:
            print(f"✅ pandas_datareader Success: {len(data)} records")
            print(f"Date range: {data.index.min()} to {data.index.max()}")
            print(f"Columns: {list(data.columns)}")
            return True
        else:
            print("❌ pandas_datareader: Empty data")
            return False
            
    except ImportError:
        print("❌ pandas_datareader not installed")
        print("Install with: pip install pandas-datareader")
        return False
    except Exception as e:
        print(f"❌ pandas_datareader error: {e}")
        return False

def create_sample_data():
    """Create sample data if Yahoo Finance is completely down"""
    print("\n" + "=" * 50)
    print("🔧 Creating Sample Data for Testing")
    print("=" * 50)
    
    # Generate realistic sample data
    import numpy as np
    
    tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
    start_date = datetime.now() - timedelta(days=30)
    dates = pd.date_range(start=start_date, end=datetime.now(), freq='B')  # Business days only
    
    all_data = []
    
    for ticker in tickers:
        # Generate realistic stock prices
        base_price = {"AAPL": 180, "MSFT": 300, "GOOGL": 140, "AMZN": 150, "NVDA": 400}[ticker]
        
        # Random walk for prices
        price_changes = np.random.normal(0, 0.02, len(dates))  # 2% daily volatility
        prices = [base_price]
        
        for change in price_changes[:-1]:
            new_price = prices[-1] * (1 + change)
            prices.append(max(new_price, 1))  # Ensure price stays positive
        
        # Create OHLCV data
        for i, date in enumerate(dates):
            close = prices[i]
            high = close * (1 + abs(np.random.normal(0, 0.01)))
            low = close * (1 - abs(np.random.normal(0, 0.01)))
            open_price = prices[i-1] if i > 0 else close
            volume = np.random.randint(1000000, 10000000)
            
            all_data.append({
                'date': date.date(),
                'ticker': ticker,
                'open_price': round(open_price, 2),
                'high_price': round(high, 2),
                'low_price': round(low, 2),
                'close_price': round(close, 2),
                'adj_close_price': round(close, 2),
                'volume': volume,
                'dividends': 0.0,
                'stock_splits': 0.0
            })
    
    df = pd.DataFrame(all_data)
    print(f"✅ Generated {len(df)} sample records")
    print(f"Tickers: {df['ticker'].unique().tolist()}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    
    return df

if __name__ == "__main__":
    print("🚀 Yahoo Finance Debugging Tool")
    print("=" * 50)
    
    # Test yfinance methods
    test_yfinance_methods()
    
    # Test alternative
    alt_success = test_alternative_data_source()
    
    # If nothing works, show sample data option
    if not alt_success:
        print("\n💡 If Yahoo Finance is down, we can use sample data for development:")
        sample_df = create_sample_data()
        
        print(f"\nSample data preview:")
        print(sample_df.head())
        
        print(f"\n🔧 To use sample data in your ingestion:")
        print(f"1. Save this data to CSV: sample_df.to_csv('data/sample_stock_data.csv', index=False)")
        print(f"2. Modify your ingestion to read from CSV when Yahoo Finance fails")
    
    print(f"\n✅ Debug complete!")