{{ config(
    materialized='table',
    indexes=[
      {'columns': ['date'], 'type': 'btree'},
      {'columns': ['date', 'asset_class'], 'type': 'btree'}
    ]
) }}

with stock_summary as (
    select
        date,
        'Stocks' as asset_class,
        count(*) as num_assets,
        avg(daily_return) as avg_return,
        stddev(daily_return) as return_volatility,
        sum(case when daily_return > 0 then 1 else 0 end)::float / count(*) as pct_positive,
        avg(volume * close_price) as avg_dollar_volume,
        count(distinct sector) as num_sectors_traded
        
    from {{ ref('int_stock_daily_analysis') }}
    where daily_return is not null
    group by date
),

crypto_summary as (
    select
        date,
        'Crypto' as asset_class,
        count(*) as num_assets,
        avg(daily_return) as avg_return,
        stddev(daily_return) as return_volatility,
        sum(case when daily_return > 0 then 1 else 0 end)::float / count(*) as pct_positive,
        avg(volume_24h) as avg_dollar_volume,
        count(distinct crypto_category) as num_sectors_traded
        
    from {{ ref('int_crypto_analysis') }}
    where daily_return is not null
    group by date
),

economic_context as (
    select
        date,
        max(case when series_id = 'FEDFUNDS' then value end) as fed_funds_rate,
        max(case when series_id = 'DGS10' then value end) as treasury_10y,
        max(case when series_id = 'CPIAUCSL' then percentage_change end) as inflation_change,
        max(case when series_id = 'UNRATE' then value end) as unemployment_rate,
        string_agg(distinct economic_signal, ', ') as economic_signals
        
    from {{ ref('int_economic_analysis') }}
    group by date
),

combined_summary as (
    select * from stock_summary
    union all
    select * from crypto_summary
),

final_summary as (
    select
        cs.*,
        ec.fed_funds_rate,
        ec.treasury_10y,
        ec.inflation_change,
        ec.unemployment_rate,
        ec.economic_signals,
        
        -- Market regime classification
        case
            when cs.return_volatility > 0.03 and cs.pct_positive < 0.3 then 'Crisis'
            when cs.return_volatility > 0.02 and cs.avg_return < -0.01 then 'Bear Market'
            when cs.return_volatility < 0.015 and cs.avg_return > 0.005 then 'Bull Market'
            when cs.return_volatility < 0.01 then 'Low Volatility'
            else 'Normal'
        end as market_regime,
        
        -- Risk-on vs Risk-off sentiment
        case
            when cs.asset_class = 'Stocks' and cs.pct_positive > 0.7 and cs.return_volatility < 0.02 then 'Risk On'
            when cs.asset_class = 'Stocks' and cs.pct_positive < 0.3 and cs.return_volatility > 0.025 then 'Risk Off'
            when cs.asset_class = 'Crypto' and cs.avg_return > 0.02 then 'Risk On'
            when cs.asset_class = 'Crypto' and cs.avg_return < -0.02 then 'Risk Off'
            else 'Neutral'
        end as risk_sentiment

    from combined_summary cs
    left join economic_context ec on cs.date = ec.date
)

select *
from final_summary
order by date desc, asset_class