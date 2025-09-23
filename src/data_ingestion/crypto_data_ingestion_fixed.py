"""
Fixed cryptocurrency data ingestion from CoinGecko
Addresses datetime issues and incorrect crypto IDs
"""
import requests
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, date, timedelta
import time
import json

from src.data_ingestion.base_ingestion import BaseDataIngestion, DataQualityValidator, get_date_range
from src.models.database import db_manager, CryptoPrice
from loguru import logger

class CoinGeckoDataIngestionFixed(BaseDataIngestion):
    """Fixed cryptocurrency data ingestion from CoinGecko"""
    
    def __init__(self):
        super().__init__(
            job_name="coingecko_crypto_data_fixed",
            data_source="coingecko",
            rate_limit=5  # More conservative rate limiting
        )
        
        # Verified working crypto IDs (tested)
        self.working_cryptos = [
            'bitcoin', 'ethereum', 'dogecoin', 'cardano', 'solana',
            'chainlink', 'polkadot', 'litecoin', 'bitcoin-cash',
            'uniswap', 'avalanche-2', 'shiba-inu', 'near-protocol'
        ]
        
        # CoinGecko base URL
        self.base_url = "https://api.coingecko.com/api/v3"
        
        # Setup session with better headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9'
        })
    
    def extract_data(self, crypto_ids: List[str] = None, start_date: str = None, 
                    end_date: str = None, days_back: int = 90) -> pd.DataFrame:
        """Extract cryptocurrency data with fixed datetime handling"""
        
        if crypto_ids is None:
            crypto_ids = self.working_cryptos[:10]  # Start with 10 working cryptos
        
        # Fix datetime handling - ensure we're working with date objects
        if start_date is None and end_date is None:
            end_date_dt = datetime.now()
            start_date_dt = end_date_dt - timedelta(days=days_back)
            start_date = start_date_dt.date()
            end_date = end_date_dt.date()
        else:
            start_date, end_date = get_date_range(start_date, end_date, default_years=1)
        
        logger.info(f"Extracting CoinGecko data for {len(crypto_ids)} cryptocurrencies from {start_date} to {end_date}")
        
        all_data = []
        successful_count = 0
        
        for crypto_id in crypto_ids:
            try:
                logger.debug(f"CoinGecko: Processing {crypto_id}")
                
                # Get historical market data
                url = f"{self.base_url}/coins/{crypto_id}/market_chart"
                params = {
                    'vs_currency': 'usd',
                    'days': min(days_back, 90),  # Limit to 90 days to avoid issues
                    'interval': 'daily'
                }
                
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        
                        if 'prices' in data and data['prices']:
                            # Process the data with fixed datetime handling
                            df = self._process_crypto_data(data, crypto_id, start_date, end_date)
                            
                            if not df.empty:
                                all_data.append(df)
                                successful_count += 1
                                logger.debug(f"✅ CoinGecko {crypto_id}: {len(df)} records")
                        else:
                            logger.warning(f"No price data for {crypto_id}")
                        
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON for {crypto_id}: {e}")
                    except Exception as e:
                        logger.warning(f"Failed to process data for {crypto_id}: {e}")
                
                elif response.status_code == 429:
                    logger.warning(f"Rate limited by CoinGecko, waiting longer...")
                    time.sleep(120)  # Wait 2 minutes if rate limited
                elif response.status_code == 404:
                    logger.warning(f"Crypto ID '{crypto_id}' not found (404) - skipping")
                else:
                    logger.warning(f"Failed to fetch {crypto_id}: HTTP {response.status_code}")
                
                # Conservative rate limiting
                time.sleep(12)  # 5 requests per minute = 12 seconds between requests
                
            except Exception as e:
                logger.error(f"Error processing crypto {crypto_id}: {e}")
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"CoinGecko extraction complete: {len(combined_df)} records from {successful_count} cryptos")
            return combined_df
        
        logger.warning("No CoinGecko data extracted")
        return pd.DataFrame()
    
    def _process_crypto_data(self, data: dict, crypto_id: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Process cryptocurrency data with proper datetime handling"""
        
        try:
            prices = data['prices']
            market_caps = data.get('market_caps', [])
            volumes = data.get('total_volumes', [])
            
            df_data = []
            
            for i, (timestamp, price) in enumerate(prices):
                # Convert timestamp to date (fixed datetime handling)
                date_obj = datetime.fromtimestamp(timestamp / 1000).date()
                
                # Filter date range properly - compare date objects only
                if date_obj < start_date or date_obj > end_date:
                    continue
                
                # Get corresponding market cap and volume
                market_cap = market_caps[i][1] if i < len(market_caps) else None
                volume = volumes[i][1] if i < len(volumes) else None
                
                # Clean symbol name
                symbol = self._get_clean_symbol(crypto_id)
                
                df_data.append({
                    'date': date_obj,  # Already a date object
                    'symbol': symbol,
                    'name': crypto_id.replace('-', ' ').title(),
                    'price_usd': float(price) if price is not None else None,
                    'market_cap': int(market_cap) if market_cap is not None else None,
                    'volume_24h': int(volume) if volume is not None else None,
                    'data_source': 'coingecko',
                    'created_at': datetime.utcnow()
                })
            
            if df_data:
                df = pd.DataFrame(df_data)
                
                # Add missing columns with defaults
                df['circulating_supply'] = None
                df['total_supply'] = None
                df['max_supply'] = None
                df['market_cap_rank'] = None
                
                # Calculate 24h price changes
                if len(df) > 1:
                    df = df.sort_values('date').reset_index(drop=True)
                    df['price_change_24h'] = df['price_usd'].diff()
                    df['price_change_percentage_24h'] = (df['price_usd'].pct_change() * 100).round(4)
                else:
                    df['price_change_24h'] = None
                    df['price_change_percentage_24h'] = None
                
                return df
            
        except Exception as e:
            logger.warning(f"Error processing crypto data for {crypto_id}: {e}")
        
        return pd.DataFrame()
    
    def _get_clean_symbol(self, crypto_id: str) -> str:
        """Get clean trading symbol for crypto"""
        symbol_map = {
            'bitcoin': 'BTC',
            'ethereum': 'ETH',
            'dogecoin': 'DOGE',
            'cardano': 'ADA',
            'solana': 'SOL',
            'chainlink': 'LINK',
            'polkadot': 'DOT',
            'litecoin': 'LTC',
            'bitcoin-cash': 'BCH',
            'uniswap': 'UNI',
            'avalanche-2': 'AVAX',
            'shiba-inu': 'SHIB',
            'near-protocol': 'NEAR'
        }
        return symbol_map.get(crypto_id, crypto_id.upper()[:10])
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform cryptocurrency data with proper data types"""
        logger.info("Transforming CoinGecko cryptocurrency data...")
        
        if df.empty:
            return df
        
        original_count = len(df)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['symbol', 'date'], keep='last')
        
        # Data quality checks
        df = df.dropna(subset=['date', 'symbol', 'price_usd'])
        
        # Remove negative or zero prices
        df = df[df['price_usd'] > 0]
        
        # Convert numeric columns
        numeric_columns = [
            'price_usd', 'market_cap', 'volume_24h', 'circulating_supply',
            'total_supply', 'max_supply', 'price_change_24h', 'price_change_percentage_24h'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Round prices to appropriate precision
        if 'price_usd' in df.columns:
            df['price_usd'] = df['price_usd'].round(8)
        
        # Ensure date column is proper date type (should already be)
        if 'date' in df.columns:
            # Convert to datetime first, then to date if needed
            df['date'] = pd.to_datetime(df['date']).dt.date
        
        cleaned_count = len(df)
        removed_count = original_count - cleaned_count
        
        logger.info(f"Crypto transformation complete: {removed_count} records removed, {cleaned_count} records retained")
        return df
    
    def load_data(self, df: pd.DataFrame) -> Tuple[int, int, int]:
        """Load cryptocurrency data into database"""
        logger.info("Loading CoinGecko cryptocurrency data into database...")
        
        if df.empty:
            return 0, 0, 0
        
        inserted_count = 0
        failed_count = 0
        
        # Process in smaller batches
        batch_size = 100
        total_batches = len(df) // batch_size + (1 if len(df) % batch_size != 0 else 0)
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(df))
            batch_df = df.iloc[start_idx:end_idx]
            
            logger.debug(f"Loading crypto batch {batch_num + 1}/{total_batches} ({len(batch_df)} records)")
            
            try:
                with self.db_manager.get_session() as session:
                    for _, row in batch_df.iterrows():
                        try:
                            # Convert row to dict and handle NaN values
                            row_dict = row.to_dict()
                            for key, value in row_dict.items():
                                if pd.isna(value):
                                    row_dict[key] = None
                            
                            # Create CryptoPrice object
                            crypto_price = CryptoPrice(**row_dict)
                            session.merge(crypto_price)
                            inserted_count += 1
                            
                        except Exception as e:
                            failed_count += 1
                            logger.warning(f"Failed to insert crypto record: {e}")
            
            except Exception as e:
                logger.error(f"Crypto batch {batch_num + 1} failed: {e}")
                failed_count += len(batch_df)
        
        logger.info(f"Crypto load complete: {inserted_count} inserted, {failed_count} failed")
        return inserted_count, 0, failed_count

def run_fixed_crypto_ingestion():
    """Run fixed CoinGecko cryptocurrency data ingestion"""
    ingestion = CoinGeckoDataIngestionFixed()
    
    result = ingestion.run_ingestion(days_back=90)  # Get 3 months of data
    
    print(f"""
    🎉 Fixed CoinGecko Cryptocurrency Data Ingestion Complete!
    
    📊 Results:
    • Data Source: CoinGecko (100% Free - Fixed)
    • Records Processed: {result.records_processed:,}
    • Records Inserted: {result.records_inserted:,}
    • Records Failed: {result.records_failed:,}
    • Duration: {result.duration_seconds:.2f} seconds
    • Success Rate: {result.success_rate:.1%}
    
    ✅ Fixed DateTime Issues & Invalid Crypto IDs!
    """)
    
    return result

if __name__ == "__main__":
    from src.data_ingestion.base_ingestion import setup_logging
    setup_logging()
    
    run_fixed_crypto_ingestion()