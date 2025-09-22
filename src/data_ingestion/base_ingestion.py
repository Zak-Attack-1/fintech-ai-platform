"""
Base classes and utilities for data ingestion pipeline
"""
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date, timedelta
from dataclasses import dataclass
import pandas as pd
from sqlalchemy.exc import IntegrityError
from loguru import logger
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

@dataclass
class IngestionResult:
    """Result of data ingestion operation"""
    job_name: str
    data_source: str
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_failed: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    @property
    def duration_seconds(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def success_rate(self) -> float:
        if self.records_processed == 0:
            return 0.0
        return (self.records_inserted + self.records_updated) / self.records_processed
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'job_name': self.job_name,
            'data_source': self.data_source,
            'records_processed': self.records_processed,
            'records_inserted': self.records_inserted,
            'records_updated': self.records_updated,
            'records_failed': self.records_failed,
            'duration_seconds': self.duration_seconds,
            'success_rate': self.success_rate,
            'error_message': self.error_message,
            'metadata': self.metadata or {}
        }

class RateLimitedSession:
    """HTTP session with rate limiting and retry logic"""
    
    def __init__(self, requests_per_minute: int = 60, max_retries: int = 3):
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60.0 / requests_per_minute
        self.last_request_time = 0
        
        # Configure session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """Rate-limited GET request"""
        self._wait_if_needed()
        try:
            response = self.session.get(url, timeout=30, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request failed for {url}: {e}")
            raise
    
    def _wait_if_needed(self):
        """Wait if necessary to respect rate limit"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

class BaseDataIngestion(ABC):
    """Base class for all data ingestion jobs"""
    
    def __init__(self, job_name: str, data_source: str, rate_limit: int = 60):
        self.job_name = job_name
        self.data_source = data_source
        self.rate_limit = rate_limit
        self.session = RateLimitedSession(requests_per_minute=rate_limit)
        
        # Import here to avoid circular imports
        from src.models.database import db_manager
        self.db_manager = db_manager
    
    @abstractmethod
    def extract_data(self, **kwargs) -> pd.DataFrame:
        """Extract data from source"""
        pass
    
    @abstractmethod
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and clean data"""
        pass
    
    @abstractmethod
    def load_data(self, df: pd.DataFrame) -> Tuple[int, int, int]:
        """Load data into database. Returns (inserted, updated, failed)"""
        pass
    
    def run_ingestion(self, **kwargs) -> IngestionResult:
        """Run complete ETL pipeline with logging and error handling"""
        result = IngestionResult(
            job_name=self.job_name,
            data_source=self.data_source,
            start_time=datetime.utcnow()
        )
        
        # Create database log entry
        log_id = self.db_manager.create_ingestion_log(self.job_name, self.data_source)
        
        try:
            logger.info(f"Starting ingestion job: {self.job_name}")
            
            # Extract
            logger.info("Extracting data...")
            df = self.extract_data(**kwargs)
            result.records_processed = len(df)
            logger.info(f"Extracted {len(df)} records")
            
            if df.empty:
                logger.warning("No data extracted, skipping transform and load")
                result.end_time = datetime.utcnow()
                return result
            
            # Transform
            logger.info("Transforming data...")
            df_transformed = self.transform_data(df)
            logger.info(f"Transformed data: {len(df_transformed)} records")
            
            # Load
            logger.info("Loading data into database...")
            inserted, updated, failed = self.load_data(df_transformed)
            
            result.records_inserted = inserted
            result.records_updated = updated
            result.records_failed = failed
            result.end_time = datetime.utcnow()
            
            # Update database log
            self.db_manager.update_ingestion_log(
                log_id,
                end_time=result.end_time,
                status='completed',
                records_processed=result.records_processed,
                records_inserted=result.records_inserted,
                records_updated=result.records_updated,
                records_failed=result.records_failed,
                metadata=result.to_dict()
            )
            
            logger.info(f"Ingestion completed: {inserted} inserted, {updated} updated, {failed} failed")
            return result
            
        except Exception as e:
            result.end_time = datetime.utcnow()
            result.error_message = str(e)
            
            # Update database log with error
            self.db_manager.update_ingestion_log(
                log_id,
                end_time=result.end_time,
                status='failed',
                error_message=result.error_message,
                metadata=result.to_dict()
            )
            
            logger.error(f"Ingestion failed: {e}")
            raise

class DataQualityValidator:
    """Data quality validation utilities"""
    
    @staticmethod
    def validate_stock_data(df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean stock price data"""
        logger.info("Validating stock data quality...")
        
        original_count = len(df)
        
        # Remove records with null critical fields
        df = df.dropna(subset=['ticker', 'date', 'close_price'])
        
        # Remove records with zero or negative prices
        df = df[df['close_price'] > 0]
        
        # Remove records with impossible volume (negative)
        df = df[df['volume'] >= 0]
        
        # Remove duplicate ticker/date combinations
        df = df.drop_duplicates(subset=['ticker', 'date'], keep='last')
        
        # Validate price relationships (high >= close >= low, etc.)
        mask = (
            (df['high_price'] >= df['close_price']) &
            (df['close_price'] >= df['low_price']) &
            (df['high_price'] >= df['low_price'])
        )
        df = df[mask]
        
        cleaned_count = len(df)
        removed_count = original_count - cleaned_count
        
        logger.info(f"Data quality validation: {removed_count} records removed, {cleaned_count} records retained")
        
        return df
    
    @staticmethod
    def validate_economic_data(df: pd.DataFrame) -> pd.DataFrame:
        """Validate economic indicator data"""
        logger.info("Validating economic data quality...")
        
        original_count = len(df)
        
        # Remove records with null critical fields
        df = df.dropna(subset=['series_id', 'date', 'value'])
        
        # Remove duplicate series_id/date combinations
        df = df.drop_duplicates(subset=['series_id', 'date'], keep='last')
        
        # Remove obviously invalid values (too extreme)
        for col in ['value']:
            if col in df.columns:
                Q1 = df[col].quantile(0.01)
                Q99 = df[col].quantile(0.99)
                IQR = Q99 - Q1
                lower_bound = Q1 - 10 * IQR  # Very generous bounds
                upper_bound = Q99 + 10 * IQR
                
                df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
        
        cleaned_count = len(df)
        removed_count = original_count - cleaned_count
        
        logger.info(f"Economic data validation: {removed_count} records removed, {cleaned_count} records retained")
        
        return df

class BatchProcessor:
    """Utility for processing data in batches"""
    
    @staticmethod
    def process_in_batches(data: pd.DataFrame, batch_size: int, 
                          process_func, progress_callback=None) -> List[Any]:
        """Process DataFrame in batches"""
        results = []
        total_batches = len(data) // batch_size + (1 if len(data) % batch_size != 0 else 0)
        
        for i in range(0, len(data), batch_size):
            batch = data.iloc[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            logger.debug(f"Processing batch {batch_num}/{total_batches} ({len(batch)} records)")
            
            try:
                result = process_func(batch)
                results.append(result)
                
                if progress_callback:
                    progress_callback(batch_num, total_batches, len(batch))
                    
            except Exception as e:
                logger.error(f"Batch {batch_num} failed: {e}")
                raise
        
        return results

def get_date_range(start_date: str = None, end_date: str = None, 
                  default_years: int = 5) -> Tuple[date, date]:
    """Get date range for data ingestion"""
    if end_date is None:
        end_date = date.today()
    else:
        end_date = pd.to_datetime(end_date).date()
    
    if start_date is None:
        start_date = end_date - timedelta(days=default_years * 365)
    else:
        start_date = pd.to_datetime(start_date).date()
    
    return start_date, end_date

def setup_logging():
    """Setup logging configuration for ingestion jobs"""
    logger.add(
        "logs/ingestion_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="30 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
        level="INFO"
    )
    
    logger.add(
        "logs/ingestion_errors_{time:YYYY-MM-DD}.log", 
        rotation="1 day",
        retention="30 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
        level="ERROR",
        filter=lambda record: record["level"].name == "ERROR"
    )