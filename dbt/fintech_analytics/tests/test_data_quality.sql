-- Test for future dates (should not exist)
select count(*) as future_dates
from {{ ref('stg_stock_prices') }}
where date > current_date

-- Test for duplicate records
select ticker, date, count(*) as duplicate_count
from {{ ref('stg_stock_prices') }}
group by ticker, date
having count(*) > 1

-- Test for negative prices
select count(*) as negative_prices
from {{ ref('stg_stock_prices') }}
where close_price <= 0

-- Test for impossible price relationships
select count(*) as impossible_prices
from {{ ref('stg_stock_prices') }}
where high_price < low_price
   or high_price < close_price
   or low_price > close_price