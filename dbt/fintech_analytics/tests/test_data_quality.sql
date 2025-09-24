{{
  config(
    severity = 'error'
  )
}}

-- Test for data quality issues
-- This test should return 0 rows if data quality is good

-- Test for future dates
select 
    'future_dates' as test_type,
    ticker,
    date,
    cast(null as numeric) as close_price,
    cast(null as numeric) as high_price,
    cast(null as numeric) as low_price,
    1 as issue_count
from {{ ref('stg_stock_prices') }}
where date > current_date

union all

-- Test for duplicate records
select 
    'duplicate_records' as test_type,
    ticker,
    date,
    cast(null as numeric) as close_price,
    cast(null as numeric) as high_price,
    cast(null as numeric) as low_price,
    count(*)::integer as issue_count
from {{ ref('stg_stock_prices') }}
group by ticker, date
having count(*) > 1

union all

-- Test for negative or zero prices
select 
    'negative_prices' as test_type,
    ticker,
    date,
    close_price,
    cast(null as numeric) as high_price,
    cast(null as numeric) as low_price,
    1 as issue_count
from {{ ref('stg_stock_prices') }}
where close_price <= 0

union all

-- Test for impossible price relationships
select 
    'impossible_prices' as test_type,
    ticker,
    date,
    close_price,
    high_price,
    low_price,
    1 as issue_count
from {{ ref('stg_stock_prices') }}
where high_price < low_price
   or high_price < close_price
   or low_price > close_price