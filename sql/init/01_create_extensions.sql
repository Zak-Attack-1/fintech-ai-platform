-- Enable required PostgreSQL extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Install vector extension for AI embeddings (if available)
-- CREATE EXTENSION IF NOT EXISTS vector;

-- Create custom types
CREATE TYPE market_status AS ENUM ('open', 'closed', 'pre_market', 'after_hours');
CREATE TYPE data_source AS ENUM ('yfinance', 'fred', 'coingecko', 'worldbank', 'manual');