"""
Cryptocurrency data ingestion from CoinGecko
100% free API with generous limits
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

class CoinGeckoDataIngestion(BaseDataIngestion):
    """Ingest cryptocurrency data from CoinGecko (100% free)"""
    
    def __init__(self):
        super().__init__(
            job_name="coingecko_crypto_data",
            data_source="coingecko",
            rate_limit=10  # CoinGecko allows 10-50 requests per minute for free
        )
        
        # Top cryptocurrencies by market cap
        self.top_cryptos = [
            'bitcoin', 'ethereum', 'tether', 'bnb', 'solana', 'usdc',
            'steth', 'xrp', 'dogecoin', 'toncoin', 'cardano', 'avalanche-2',
            'shiba-inu', 'chainlink', 'bitcoin-cash', 'polkadot', 'near',
            'uniswap', 'litecoin', 'internet-computer', 'polygon', 'dai',
            'wrapped-bitcoin', 'ethereum-classic', 'artificial-superintelligence-alliance',
            'kaspa', 'monero', 'render-token', 'arbitrum', 'stellar'
        ]
        
        # CoinGecko base URL (no API key needed)
        self.base_url = "https://api.coingecko.com/api/v3"
        
        # Setup session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        })
    
    def extract_data(self, crypto_ids: List[str] = None, start_date: str = None, 
                    end_date: str = None, days_back: int = 365) -> pd.DataFrame:
        """Extract cryptocurrency data from CoinGecko"""
        
        if crypto_ids is None:
            crypto_ids = self.top_cryptos[:20]  # Top 20 cryptos
        
        # CoinGecko historical data
        if start_date is None and end_date is None:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
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
                    'days': min(days_back, 365),  # CoinGecko free tier limit
                    'interval': 'daily'
                }
                
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        
                        if 'prices' in data and data['prices']:
                            # CoinGecko returns: prices, market_caps, total_volumes
                            prices = data['prices']
                            market_caps = data.get('market_caps', [])
                            volumes = data.get('total_volumes', [])
                            
                            # Convert to DataFrame
                            df_data = []
                            for i, (timestamp, price) in enumerate(prices):
                                # Convert timestamp to date
                                date_obj = datetime.fromtimestamp(timestamp / 1000).date()
                                
                                # Get corresponding market cap and volume
                                market_cap = market_caps[i][1] if i < len(market_caps) else None
                                volume = volumes[i][1] if i < len(volumes) else None
                                
                                df_data.append({
                                    'date': date_obj,
                                    'symbol': crypto_id.upper().replace('-', ''),  # Clean symbol
                                    'name': crypto_id.replace('-', ' ').title(),
                                    'price_usd': price,
                                    'market_cap': market_cap,
                                    'volume_24h': volume,
                                    'data_source': 'coingecko'
                                })
                            
                            if df_data:
                                df = pd.DataFrame(df_data)
                                
                                # Filter date range
                                df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
                                
                                # Add additional info if available
                                if not df.empty:
                                    df = self._enrich_crypto_data(df, crypto_id)
                                    all_data.append(df)
                                    successful_count += 1
                                    logger.debug(f"✅ CoinGecko {crypto_id}: {len(df)} records")
                        
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON for {crypto_id}: {e}")
                    except Exception as e:
                        logger.warning(f"Failed to process data for {crypto_id}: {e}")
                
                elif response.status_code == 429:
                    logger.warning(f"Rate limited by CoinGecko, waiting longer...")
                    time.sleep(60)  # Wait 1 minute if rate limited
                else:
                    logger.warning(f"Failed to fetch {crypto_id}: HTTP {response.status_code}")
                
                # Rate limiting - CoinGecko allows 10-50 requests per minute
                time.sleep(6)  # 10 requests per minute = 6 seconds between requests
                
            except Exception as e:
                logger.error(f"Error processing crypto {crypto_id}: {e}")
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"CoinGecko extraction complete: {len(combined_df)} records from {successful_count} cryptos")
            return combined_df
        
        logger.warning("No CoinGecko data extracted")
        return pd.DataFrame()
    
    def _enrich_crypto_data(self, df: pd.DataFrame, crypto_id: str) -> pd.DataFrame:
        """Enrich cryptocurrency data with additional information"""
        
        # Add missing columns with defaults
        df['circulating_supply'] = None
        df['total_supply'] = None
        df['max_supply'] = None
        df['price_change_24h'] = None
        df['price_change_percentage_24h'] = None
        df['market_cap_rank'] = None
        df['created_at'] = datetime.utcnow()
        
        # Calculate 24h price changes where possible
        if len(df) > 1:
            df = df.sort_values('date').reset_index(drop=True)
            df['price_change_24h'] = df['price_usd'].diff()
            df['price_change_percentage_24h'] = (df['price_usd'].pct_change() * 100).round(4)
        
        return df
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and clean cryptocurrency data"""
        logger.info("Transforming CoinGecko cryptocurrency data...")
        
        original_count = len(df)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['symbol', 'date'], keep='last')
        
        # Data quality checks
        df = df.dropna(subset=['date', 'symbol', 'price_usd'])
        
        # Remove negative prices (should not happen but safety check)
        df = df[df['price_usd'] > 0]
        
        # Remove impossible market caps or volumes (negative values)
        if 'market_cap' in df.columns:
            df.loc[df['market_cap'] < 0, 'market_cap'] = None
        
        if 'volume_24h' in df.columns:
            df.loc[df['volume_24h'] < 0, 'volume_24h'] = None
        
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
            df['price_usd'] = df['price_usd'].round(8)  # Crypto needs more precision
        
        # Ensure required columns exist with proper data types
        required_columns = ['symbol', 'date', 'price_usd']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return pd.DataFrame()
        
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
        
        # Process in batches
        batch_size = 500
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
                            session.merge(crypto_price)  # Use merge for upsert
                            inserted_count += 1
                            
                        except Exception as e:
                            failed_count += 1
                            logger.warning(f"Failed to insert crypto record: {e}")
            
            except Exception as e:
                logger.error(f"Crypto batch {batch_num + 1} failed: {e}")
                failed_count += len(batch_df)
        
        logger.info(f"Crypto load complete: {inserted_count} inserted, {failed_count} failed")
        return inserted_count, 0, failed_count

def run_crypto_ingestion():
    """Run CoinGecko cryptocurrency data ingestion"""
    ingestion = CoinGeckoDataIngestion()
    
    result = ingestion.run_ingestion(days_back=365)  # Get 1 year of data
    
    print(f"""
    🎉 CoinGecko Cryptocurrency Data Ingestion Complete!
    
    📊 Results:
    • Data Source: CoinGecko (100% Free)
    • Records Processed: {result.records_processed:,}
    • Records Inserted: {result.records_inserted:,}
    • Records Failed: {result.records_failed:,}
    • Duration: {result.duration_seconds:.2f} seconds
    • Success Rate: {result.success_rate:.1%}
    
    ✅ 100% Free Cryptocurrency Data!
    """)
    
    return result

if __name__ == "__main__":
    from src.data_ingestion.base_ingestion import setup_logging
    setup_logging()
    
    run_crypto_ingestion()