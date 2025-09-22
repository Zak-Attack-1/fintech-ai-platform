"""
Database connection and ORM models for the fintech platform
"""
import os
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from contextlib import contextmanager
from sqlalchemy import create_engine, Column, Integer, String, Date, NUMERIC, BigInteger, Boolean, Text, TIMESTAMP, JSON, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import UUID, ENUM
from sqlalchemy.sql import func
from pydantic_settings import BaseSettings
import uuid
from loguru import logger

class DatabaseConfig(BaseSettings):
    """Database configuration from environment variables"""
    database_url: str = os.getenv("DATABASE_URL", "postgresql://fintech_user:secure_password_change_me@localhost:9543/fintech_analytics")
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600

# SQLAlchemy setup
Base = declarative_base()

# Custom enum types
market_status_enum = ENUM('open', 'closed', 'pre_market', 'after_hours', name='market_status', create_type=False)
data_source_enum = ENUM('yfinance', 'fred', 'coingecko', 'worldbank', 'manual', name='data_source', create_type=False)

class Company(Base):
    """Companies/Tickers reference table"""
    __tablename__ = 'companies'
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String(20), nullable=False, unique=True)
    company_name = Column(String(255))
    sector = Column(String(100))
    industry = Column(String(100))
    market_cap = Column(BigInteger)
    employees = Column(Integer)
    founded_year = Column(Integer)
    headquarters = Column(String(255))
    website = Column(String(255))
    description = Column(Text)
    is_sp500 = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class StockPrice(Base):
    """Stock prices with time-series partitioning"""
    __tablename__ = 'stock_prices'
    
    id = Column(BigInteger, primary_key=True)
    ticker = Column(String(20), nullable=False)
    date = Column(Date, nullable=False, primary_key=True)
    open_price = Column(NUMERIC(12, 4))
    high_price = Column(NUMERIC(12, 4))
    low_price = Column(NUMERIC(12, 4))
    close_price = Column(NUMERIC(12, 4))
    adj_close_price = Column(NUMERIC(12, 4))
    volume = Column(BigInteger)
    dividends = Column(NUMERIC(8, 4), default=0)
    stock_splits = Column(NUMERIC(8, 4), default=0)
    data_source = Column(data_source_enum, default='yfinance')
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

class EconomicIndicator(Base):
    """Economic indicators from FRED and other sources"""
    __tablename__ = 'economic_indicators'
    
    id = Column(Integer, primary_key=True)
    series_id = Column(String(50), nullable=False)
    series_name = Column(String(200))
    date = Column(Date, nullable=False)
    value = Column(NUMERIC(15, 6))
    units = Column(String(100))
    frequency = Column(String(20))
    seasonal_adjustment = Column(String(100))
    notes = Column(Text)
    data_source = Column(data_source_enum, default='fred')
    extra_data = Column(JSON)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class CryptoPrice(Base):
    """Cryptocurrency price data"""
    __tablename__ = 'crypto_prices'
    
    id = Column(BigInteger, primary_key=True)
    symbol = Column(String(20), nullable=False)
    name = Column(String(100))
    date = Column(Date, nullable=False, primary_key=True)
    price_usd = Column(NUMERIC(15, 8))
    market_cap = Column(BigInteger)
    volume_24h = Column(BigInteger)
    circulating_supply = Column(BigInteger)
    total_supply = Column(BigInteger)
    max_supply = Column(BigInteger)
    price_change_24h = Column(NUMERIC(8, 4))
    price_change_percentage_24h = Column(NUMERIC(8, 4))
    market_cap_rank = Column(Integer)
    data_source = Column(data_source_enum, default='coingecko')
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

class GlobalEconomicData(Base):
    """Global economic indicators from World Bank, IMF, etc."""
    __tablename__ = 'global_economic_data'
    
    id = Column(Integer, primary_key=True)
    country_code = Column(String(3), nullable=False)
    country_name = Column(String(100))
    indicator_code = Column(String(50))
    indicator_name = Column(String(200))
    year = Column(Integer)
    value = Column(NUMERIC(15, 6))
    units = Column(String(100))
    data_source = Column(data_source_enum, default='worldbank')
    extra_data = Column(JSON)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

class IngestionLog(Base):
    """Data ingestion tracking and monitoring"""
    __tablename__ = 'ingestion_logs'
    
    id = Column(Integer, primary_key=True)
    job_name = Column(String(100))
    data_source = Column(data_source_enum)
    start_time = Column(TIMESTAMP(timezone=True))
    end_time = Column(TIMESTAMP(timezone=True))
    status = Column(String(20))  # 'running', 'completed', 'failed', 'cancelled'
    records_processed = Column(Integer, default=0)
    records_inserted = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    error_message = Column(Text)
    extra_data = Column(JSON)

class DatabaseManager:
    """Database connection and session management"""
    
    def __init__(self, config: DatabaseConfig = None):
        self.config = config or DatabaseConfig()
        self.engine = create_engine(
            self.config.database_url,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=self.config.pool_timeout,
            pool_recycle=self.config.pool_recycle,
            echo=False  # Set to True for SQL debugging
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    @contextmanager
    def get_session(self) -> Session:
        """Get database session with automatic cleanup"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def create_ingestion_log(self, job_name: str, data_source: str) -> int:
        """Create new ingestion log entry"""
        with self.get_session() as session:
            log_entry = IngestionLog(
                job_name=job_name,
                data_source=data_source,
                start_time=datetime.utcnow(),
                status='running'
            )
            session.add(log_entry)
            session.commit()
            session.refresh(log_entry)
            return log_entry.id
    
    def update_ingestion_log(self, log_id: int, **kwargs):
        """Update ingestion log entry"""
        with self.get_session() as session:
            log_entry = session.query(IngestionLog).filter(IngestionLog.id == log_id).first()
            if log_entry:
                for key, value in kwargs.items():
                    setattr(log_entry, key, value)
                session.commit()

# Global database manager instance
db_manager = DatabaseManager()