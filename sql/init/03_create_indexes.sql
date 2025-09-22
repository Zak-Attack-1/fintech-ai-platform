-- Stock prices indexes for optimal query performance
-- Note: Cannot use CONCURRENTLY on partitioned tables
CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_date 
    ON stock_prices (ticker, date DESC);
CREATE INDEX IF NOT EXISTS idx_stock_prices_date 
    ON stock_prices (date DESC);
CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_close 
    ON stock_prices (ticker, close_price);
CREATE INDEX IF NOT EXISTS idx_stock_prices_volume 
    ON stock_prices (volume DESC) WHERE volume > 1000000;

-- Companies indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_companies_ticker 
    ON companies (ticker);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_companies_sector 
    ON companies (sector);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_companies_sp500 
    ON companies (is_sp500) WHERE is_sp500 = true;

-- Economic indicators indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_economic_series_date 
    ON economic_indicators (series_id, date DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_economic_date 
    ON economic_indicators (date DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_economic_series 
    ON economic_indicators (series_id);

-- Crypto prices indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crypto_symbol_date 
    ON crypto_prices (symbol, date DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crypto_market_cap 
    ON crypto_prices (market_cap DESC) WHERE market_cap IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crypto_volume 
    ON crypto_prices (volume_24h DESC) WHERE volume_24h > 1000000;

-- Global economic data indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_global_country_indicator_year 
    ON global_economic_data (country_code, indicator_code, year DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_global_indicator_year 
    ON global_economic_data (indicator_code, year DESC);

-- Market analysis indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_market_analysis_type_created 
    ON market_analysis (analysis_type, created_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_market_analysis_hash 
    ON market_analysis (query_hash);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_market_analysis_expires 
    ON market_analysis (expires_at) WHERE expires_at IS NOT NULL;

-- Pattern embeddings indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pattern_content_type 
    ON pattern_embeddings (content_type, content_id);

-- Ingestion logs indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ingestion_logs_job_time 
    ON ingestion_logs (job_name, start_time DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ingestion_logs_status 
    ON ingestion_logs (status, start_time DESC);

-- GIN indexes for JSONB columns (for fast JSON queries)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_economic_metadata_gin 
    ON economic_indicators USING gin(metadata);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_global_metadata_gin 
    ON global_economic_data USING gin(metadata);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_analysis_result_gin 
    ON market_analysis USING gin(analysis_result);

-- Text search indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_companies_name_search 
    ON companies USING gin(to_tsvector('english', company_name));
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_companies_description_search 
    ON companies USING gin(to_tsvector('english', description));