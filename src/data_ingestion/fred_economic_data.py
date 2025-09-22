"""
Federal Reserve Economic Data (FRED) ingestion
100% free government data with no limits
"""
import pandas as pd
import requests
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, date, timedelta
import time

from src.data_ingestion.base_ingestion import BaseDataIngestion, DataQualityValidator, get_date_range
from src.models.database import db_manager, EconomicIndicator
from loguru import logger

class FREDEconomicDataIngestion(BaseDataIngestion):
    """Ingest economic data from Federal Reserve (FRED)"""
    
    def __init__(self):
        super().__init__(
            job_name="fred_economic_data",
            data_source="fred",
            rate_limit=30  # Be conservative with government API
        )
        
        # Key economic indicators - these are the most important ones
        self.key_indicators = {
            # Core Economic Indicators
            'GDP': 'GDP',  # Gross Domestic Product
            'GDPC1': 'Real GDP',  # Real Gross Domestic Product
            'CPIAUCSL': 'Consumer Price Index',  # Inflation
            'CPILFESL': 'Core CPI',  # Core inflation (excluding food & energy)
            'UNRATE': 'Unemployment Rate',
            'CIVPART': 'Labor Force Participation Rate',
            'PAYEMS': 'Total Nonfarm Payrolls',
            
            # Federal Reserve & Interest Rates
            'FEDFUNDS': 'Federal Funds Rate',
            'DGS10': '10-Year Treasury Rate',
            'DGS2': '2-Year Treasury Rate',
            'DGS30': '30-Year Treasury Rate',
            'TB3MS': '3-Month Treasury Bill',
            
            # Housing & Real Estate
            'CSUSHPINSA': 'Case-Shiller Home Price Index',
            'HOUST': 'Housing Starts',
            'HSN1F': 'New One Family Houses Sold',
            
            # Business & Manufacturing
            'INDPRO': 'Industrial Production Index',
            'UMCSENT': 'University of Michigan Consumer Sentiment',
            'RSXFS': 'Advance Retail Sales',
            'TOTALSL': 'Total Vehicle Sales',
            
            # Money Supply & Credit
            'M1SL': 'M1 Money Stock',
            'M2SL': 'M2 Money Stock',
            'DEXUSEU': 'US / Euro Foreign Exchange Rate',
            'DEXJPUS': 'Japan / US Foreign Exchange Rate'
        }
        
        # FRED base URL (no API key needed for basic data)
        self.base_url = "https://fred.stlouisfed.org/graph/fredgraph.csv"
        
        # Setup session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        })
    
    def extract_data(self, series_ids: List[str] = None, start_date: str = None, 
                    end_date: str = None, years_back: int = 10) -> pd.DataFrame:
        """Extract economic data from FRED"""
        
        if series_ids is None:
            series_ids = list(self.key_indicators.keys())
        
        # Use longer time period for economic data (it's monthly/quarterly)
        if start_date is None and end_date is None:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=years_back * 365)
        else:
            start_date, end_date = get_date_range(start_date, end_date, default_years=years_back)
        
        logger.info(f"Extracting FRED data for {len(series_ids)} indicators from {start_date} to {end_date}")
        
        all_data = []
        successful_count = 0
        
        for series_id in series_ids:
            try:
                logger.debug(f"FRED: Processing {series_id} ({self.key_indicators.get(series_id, 'Unknown')})")
                
                # FRED CSV download URL
                params = {
                    'id': series_id,
                    'cosd': start_date.strftime('%Y-%m-%d'),
                    'coed': end_date.strftime('%Y-%m-%d'),
                    'fq': 'Daily',  # Let FRED determine frequency
                    'fam': 'avg',   # Average aggregation method
                    'fgst': 'lin',  # Linear scale
                    'fgsnd': '2020-02-01',  # Base date
                    'line_index': 1,
                    'transformation': 'lin',  # No transformation
                    'vintage_date': end_date.strftime('%Y-%m-%d'),
                    'revision_date': end_date.strftime('%Y-%m-%d'),
                    'nd': start_date.strftime('%Y-%m-%d')
                }
                
                response = self.session.get(self.base_url, params=params, timeout=30)
                
                if response.status_code == 200 and len(response.content) > 100:
                    try:
                        # Parse CSV data
                        from io import StringIO
                        df = pd.read_csv(StringIO(response.text))
                        
                        if not df.empty and len(df.columns) >= 2:
                            # FRED CSV format: DATE, SERIES_ID
                            df.columns = ['date', 'value']
                            
                            # Clean the data
                            df['series_id'] = series_id
                            df['series_name'] = self.key_indicators.get(series_id, series_id)
                            
                            # Convert date
                            df['date'] = pd.to_datetime(df['date'], errors='coerce')
                            
                            # Handle "." values (missing data in FRED)
                            df['value'] = pd.to_numeric(df['value'], errors='coerce')
                            
                            # Remove rows with missing values
                            df = df.dropna(subset=['date', 'value'])
                            
                            # Convert date to date object
                            df['date'] = df['date'].dt.date
                            
                            # Add metadata
                            df['data_source'] = 'fred'
                            df['frequency'] = self._determine_frequency(df)
                            df['units'] = self._get_series_units(series_id)
                            df['created_at'] = datetime.utcnow()
                            
                            if not df.empty:
                                all_data.append(df)
                                successful_count += 1
                                logger.debug(f"✅ FRED {series_id}: {len(df)} records")
                            else:
                                logger.warning(f"No valid data for {series_id}")
                        
                    except Exception as e:
                        logger.warning(f"Failed to parse FRED data for {series_id}: {e}")
                else:
                    logger.warning(f"Failed to fetch FRED data for {series_id}: HTTP {response.status_code}")
                
                # Be respectful to FRED servers
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing FRED series {series_id}: {e}")
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"FRED extraction complete: {len(combined_df)} records from {successful_count} series")
            return combined_df
        
        logger.warning("No FRED data extracted")
        return pd.DataFrame()
    
    def _determine_frequency(self, df: pd.DataFrame) -> str:
        """Determine the frequency of the data"""
        if len(df) < 2:
            return 'Unknown'
        
        # Calculate average days between observations
        dates = pd.to_datetime(df['date'])
        avg_days = (dates.max() - dates.min()).days / (len(dates) - 1)
        
        if avg_days <= 7:
            return 'Daily'
        elif avg_days <= 10:
            return 'Weekly'
        elif avg_days <= 35:
            return 'Monthly'
        elif avg_days <= 100:
            return 'Quarterly'
        else:
            return 'Annual'
    
    def _get_series_units(self, series_id: str) -> str:
        """Get units for the series (basic mapping)"""
        units_map = {
            'UNRATE': 'Percent',
            'FEDFUNDS': 'Percent',
            'CPIAUCSL': 'Index 1982-84=100',
            'GDP': 'Billions of Dollars',
            'GDPC1': 'Billions of Chained 2012 Dollars',
            'DGS10': 'Percent',
            'DGS2': 'Percent',
            'DGS30': 'Percent',
            'TB3MS': 'Percent',
            'INDPRO': 'Index 2017=100',
            'UMCSENT': 'Index 1966:Q1=100',
            'DEXUSEU': 'US Dollars to One Euro',
            'DEXJPUS': 'Japanese Yen to One US Dollar'
        }
        return units_map.get(series_id, 'Unknown')
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and clean economic data"""
        logger.info("Transforming FRED economic data...")
        
        # Data quality checks
        original_count = len(df)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['series_id', 'date'], keep='last')
        
        # Remove obvious outliers (very crude check)
        numeric_columns = ['value']
        for col in numeric_columns:
            if col in df.columns:
                # Remove values that are more than 5 standard deviations from mean
                mean_val = df[col].mean()
                std_val = df[col].std()
                if std_val > 0:
                    df = df[abs(df[col] - mean_val) <= 5 * std_val]
        
        # Ensure all required columns exist
        required_columns = ['series_id', 'date', 'value']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return pd.DataFrame()
        
        # Add any missing optional columns
        optional_columns = {
            'series_name': 'Unknown',
            'frequency': 'Unknown', 
            'units': 'Unknown',
            'data_source': 'fred',
            'created_at': datetime.utcnow()
        }
        
        for col, default_value in optional_columns.items():
            if col not in df.columns:
                df[col] = default_value
        
        cleaned_count = len(df)
        removed_count = original_count - cleaned_count
        
        logger.info(f"FRED transformation complete: {removed_count} records removed, {cleaned_count} records retained")
        return df
    
    def load_data(self, df: pd.DataFrame) -> Tuple[int, int, int]:
        """Load economic data into database"""
        logger.info("Loading FRED economic data into database...")
        
        if df.empty:
            return 0, 0, 0
        
        inserted_count = 0
        updated_count = 0
        failed_count = 0
        
        # Process in batches
        batch_size = 500
        total_batches = len(df) // batch_size + (1 if len(df) % batch_size != 0 else 0)
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(df))
            batch_df = df.iloc[start_idx:end_idx]
            
            logger.debug(f"Loading FRED batch {batch_num + 1}/{total_batches} ({len(batch_df)} records)")
            
            try:
                with self.db_manager.get_session() as session:
                    for _, row in batch_df.iterrows():
                        try:
                            # Convert row to dict and handle NaN values
                            row_dict = row.to_dict()
                            for key, value in row_dict.items():
                                if pd.isna(value):
                                    row_dict[key] = None
                            
                            # Create EconomicIndicator object
                            economic_indicator = EconomicIndicator(**row_dict)
                            session.merge(economic_indicator)  # Use merge for upsert
                            inserted_count += 1
                            
                        except Exception as e:
                            failed_count += 1
                            logger.warning(f"Failed to insert FRED record: {e}")
            
            except Exception as e:
                logger.error(f"FRED batch {batch_num + 1} failed: {e}")
                failed_count += len(batch_df)
        
        logger.info(f"FRED load complete: {inserted_count} inserted, {updated_count} updated, {failed_count} failed")
        return inserted_count, updated_count, failed_count

def run_fred_ingestion():
    """Run FRED economic data ingestion"""
    ingestion = FREDEconomicDataIngestion()
    
    result = ingestion.run_ingestion(years_back=20)  # Get 20 years of data
    
    print(f"""
    🎉 FRED Economic Data Ingestion Complete!
    
    📊 Results:
    • Data Source: Federal Reserve Economic Data (FRED)
    • Records Processed: {result.records_processed:,}
    • Records Inserted: {result.records_inserted:,}
    • Records Failed: {result.records_failed:,}
    • Duration: {result.duration_seconds:.2f} seconds
    • Success Rate: {result.success_rate:.1%}
    
    ✅ 100% Free Government Economic Data!
    """)
    
    return result

if __name__ == "__main__":
    from src.data_ingestion.base_ingestion import setup_logging
    setup_logging()
    
    run_fred_ingestion()