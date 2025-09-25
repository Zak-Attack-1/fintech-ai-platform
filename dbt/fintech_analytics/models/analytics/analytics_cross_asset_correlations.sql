{{ config(
    materialized='table',
    post_hook="create index if not exists idx_correlations_date on {{ this }} (analysis_date)"
) }}

with stock_returns as (
    select 
        date,
        ticker,
        daily_return,
        sector
    from {{ ref('int_stock_daily_analysis') }}
    where daily_return is not null
),

crypto_returns as (
    select 
        date,
        symbol as ticker,
        daily_return,
        crypto_category as sector
    from {{ ref('int_crypto_analysis') }}
    where daily_return is not null
),

economic_changes as (
    select 
        date,
        series_id as ticker,
        percentage_change / 100 as daily_return,  -- Convert to decimal
        indicator_category as sector
    from {{ ref('int_economic_analysis') }}
    where percentage_change is not null
),

all_returns as (
    select *, 'Stock' as asset_class from stock_returns
    union all
    select *, 'Crypto' as asset_class from crypto_returns
    union all 
    select *, 'Economic' as asset_class from economic_changes
),

correlation_matrix as (
    select
        current_date as analysis_date,
        a1.ticker as asset_1,
        a1.asset_class as asset_1_type,
        a1.sector as asset_1_sector,
        a2.ticker as asset_2,
        a2.asset_class as asset_2_type,
        a2.sector as asset_2_sector,
        
        corr(a1.daily_return, a2.daily_return) as correlation_coefficient,
        count(*) as observations,
        
        -- Statistical significance
        case 
            when count(*) >= 30 and abs(corr(a1.daily_return, a2.daily_return)) > 0.3 then 'Significant'
            when count(*) >= 30 and abs(corr(a1.daily_return, a2.daily_return)) > 0.1 then 'Moderate'
            else 'Weak'
        end as correlation_strength,
        
        -- Relationship type
        case
            when corr(a1.daily_return, a2.daily_return) > 0.7 then 'Strong Positive'
            when corr(a1.daily_return, a2.daily_return) > 0.3 then 'Moderate Positive'
            when corr(a1.daily_return, a2.daily_return) > 0.1 then 'Weak Positive'
            when corr(a1.daily_return, a2.daily_return) < -0.7 then 'Strong Negative'
            when corr(a1.daily_return, a2.daily_return) < -0.3 then 'Moderate Negative'
            when corr(a1.daily_return, a2.daily_return) < -0.1 then 'Weak Negative'
            else 'No Correlation'
        end as relationship_type

    from all_returns a1
    join all_returns a2 on a1.date = a2.date
    where a1.ticker != a2.ticker
      and a1.date >= current_date - interval '1 year'  -- Last year of data
    group by a1.ticker, a1.asset_class, a1.sector, a2.ticker, a2.asset_class, a2.sector
    having count(*) >= 20  -- Minimum observations for meaningful correlation
)

select *
from correlation_matrix
where abs(correlation_coefficient) > 0.1  -- Only meaningful correlations
order by abs(correlation_coefficient) desc