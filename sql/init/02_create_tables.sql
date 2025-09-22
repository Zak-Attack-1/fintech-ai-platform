-- Companies/Tickers reference table
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL UNIQUE,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap BIGINT,
    employees INTEGER,
    founded_year INTEGER,
    headquarters VARCHAR(255),
    website VARCHAR(255),
    description TEXT,
    is_sp500 BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Stock prices with time-series partitioning
CREATE TABLE stock_prices (
    id BIGSERIAL,
    ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(12,4),
    high_price DECIMAL(12,4),
    low_price DECIMAL(12,4),
    close_price DECIMAL(12,4),
    adj_close_price DECIMAL(12,4),
    volume BIGINT,
    dividends DECIMAL(8,4) DEFAULT 0,
    stock_splits DECIMAL(8,4) DEFAULT 0,
    data_source data_source DEFAULT 'yfinance',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (id, date)
) PARTITION BY RANGE (date);

-- Create monthly partitions for stock data (2019-2024)
DO $$
DECLARE
    start_date date := '2019-01-01';
    end_date date := '2025-01-01';
    partition_name text;
    partition_start date;
    partition_end date;
BEGIN
    WHILE start_date < end_date LOOP
        partition_name := 'stock_prices_' || to_char(start_date, 'YYYY_MM');
        partition_start := start_date;
        partition_end := partition_start + interval '1 month';
        
        EXECUTE format('CREATE TABLE %I PARTITION OF stock_prices 
                       FOR VALUES FROM (%L) TO (%L)',
                       partition_name, partition_start, partition_end);
        
        start_date := start_date + interval '1 month';
    END LOOP;
END $$;

-- Economic indicators
CREATE TABLE economic_indicators (
    id SERIAL PRIMARY KEY,
    series_id VARCHAR(50) NOT NULL,
    series_name VARCHAR(200),
    date DATE NOT NULL,
    value DECIMAL(15,6),
    units VARCHAR(100),
    frequency VARCHAR(20), -- Daily, Weekly, Monthly, Quarterly, Annual
    seasonal_adjustment VARCHAR(100),
    notes TEXT,
    data_source data_source DEFAULT 'fred',
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(series_id, date)
);

-- Cryptocurrency data
CREATE TABLE crypto_prices (
    id BIGSERIAL,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    date DATE NOT NULL,
    price_usd DECIMAL(15,8),
    market_cap BIGINT,
    volume_24h BIGINT,
    circulating_supply BIGINT,
    total_supply BIGINT,
    max_supply BIGINT,
    price_change_24h DECIMAL(8,4),
    price_change_percentage_24h DECIMAL(8,4),
    market_cap_rank INTEGER,
    data_source data_source DEFAULT 'coingecko',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (id, date)
) PARTITION BY RANGE (date);

-- Create yearly partitions for crypto data
DO $$
DECLARE
    start_year integer := 2019;
    end_year integer := 2025;
    partition_name text;
    partition_start date;
    partition_end date;
BEGIN
    WHILE start_year < end_year LOOP
        partition_name := 'crypto_prices_' || start_year::text;
        partition_start := (start_year || '-01-01')::date;
        partition_end := ((start_year + 1) || '-01-01')::date;
        
        EXECUTE format('CREATE TABLE %I PARTITION OF crypto_prices 
                       FOR VALUES FROM (%L) TO (%L)',
                       partition_name, partition_start, partition_end);
        
        start_year := start_year + 1;
    END LOOP;
END $$;

-- Global economic data
CREATE TABLE global_economic_data (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(3) NOT NULL, -- ISO 3166-1 alpha-3
    country_name VARCHAR(100),
    indicator_code VARCHAR(50),
    indicator_name VARCHAR(200),
    year INTEGER,
    value DECIMAL(15,6),
    units VARCHAR(100),
    data_source data_source DEFAULT 'worldbank',
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(country_code, indicator_code, year)
);

-- Market analysis cache (for AI-generated insights)
CREATE TABLE market_analysis (
    id SERIAL PRIMARY KEY,
    analysis_type VARCHAR(50), -- 'anomaly', 'sentiment', 'correlation', 'forecast'
    query_hash VARCHAR(64) UNIQUE, -- MD5 hash of input parameters
    input_data JSONB,
    analysis_result JSONB,
    confidence_score DECIMAL(3,2),
    ai_model_used VARCHAR(100),
    processing_time_ms INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE
);

-- Vector embeddings for AI pattern matching
CREATE TABLE pattern_embeddings (
    id SERIAL PRIMARY KEY,
    content_type VARCHAR(50), -- 'stock_pattern', 'market_anomaly', 'economic_event'
    content_id INTEGER,
    embedding_vector FLOAT[],  -- Will upgrade to vector type if extension available
    embedding_model VARCHAR(100),
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Data ingestion logs
CREATE TABLE ingestion_logs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(100),
    data_source data_source,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20), -- 'running', 'completed', 'failed', 'cancelled'
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB
);