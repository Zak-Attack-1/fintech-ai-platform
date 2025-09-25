{{ config(
    materialized='table',
    post_hook="create index if not exists idx_ai_summary_date on {{ this }} (summary_date)"
) }}

-- This model creates AI-friendly summaries for natural language querying

with daily_market_insights as (
    select
        date as summary_date,
        'market_overview' as insight_type,
        
        json_build_object(
            'total_stocks_analyzed', count(*),
            'sectors_active', count(distinct sector),
            'average_return', round(avg(daily_return)::numeric, 4),
            'market_volatility', round(stddev(daily_return)::numeric, 4),
            'positive_stocks_pct', round((sum(case when daily_return > 0 then 1 else 0 end)::float / count(*) * 100)::numeric, 1),
            'high_volume_stocks', count(*) filter (where volume_vs_avg > 0.5),
            'top_gaining_stock', first_value(ticker) over (partition by date order by daily_return desc),
            'top_gaining_return', round(first_value(daily_return) over (partition by date order by daily_return desc)::numeric, 4),
            'top_losing_stock', first_value(ticker) over (partition by date order by daily_return asc),
            'top_losing_return', round(first_value(daily_return) over (partition by date order by daily_return asc)::numeric, 4)
        ) as insight_data,
        
        -- Natural language summary
        case
            when avg(daily_return) > 0.02 then 'Strong bull market day with ' || round((sum(case when daily_return > 0 then 1 else 0 end)::float / count(*) * 100)::numeric, 0) || '% of stocks posting gains'
            when avg(daily_return) > 0.005 then 'Moderate positive market with mixed performance across sectors'
            when avg(daily_return) < -0.02 then 'Bear market day with widespread selling pressure'
            when stddev(daily_return) > 0.03 then 'High volatility trading session with significant price swings'
            else 'Calm trading day with modest price movements'
        end as natural_language_summary

    from {{ ref('int_stock_daily_analysis') }}
    where daily_return is not null
    group by date
),

sector_insights as (
    select
        date as summary_date,
        'sector_performance' as insight_type,
        
        json_agg(
            json_build_object(
                'sector', sector,
                'avg_return', round(avg(daily_return)::numeric, 4),
                'stock_count', count(*),
                'best_performer', first_value(ticker) over (partition by date, sector order by daily_return desc),
                'worst_performer', first_value(ticker) over (partition by date, sector order by daily_return asc)
            ) order by avg(daily_return) desc
        ) as insight_data,
        
        'Sector performance: ' || 
        first_value(sector) over (partition by date order by avg(daily_return) desc) ||
        ' led gains with ' ||
        round(first_value(avg(daily_return)) over (partition by date order by avg(daily_return) desc)::numeric * 100, 1) ||
        '% average return' as natural_language_summary

    from {{ ref('int_stock_daily_analysis') }}
    where daily_return is not null and sector is not null
    group by date, sector
),

crypto_insights as (
    select
        date as summary_date,
        'crypto_market' as insight_type,
        
        json_build_object(
            'total_cryptos', count(*),
            'average_return', round(avg(daily_return)::numeric, 4),
            'extreme_movers', count(*) filter (where abs(daily_return) > 0.1),
            'bitcoin_price', max(case when symbol = 'BTC' then price_usd end),
            'ethereum_price', max(case when symbol = 'ETH' then price_usd end),
            'top_gainer', first_value(symbol) over (partition by date order by daily_return desc),
            'top_gainer_return', round(first_value(daily_return) over (partition by date order by daily_return desc)::numeric, 4),
            'total_market_cap', sum(market_cap)
        ) as insight_data,
        
        case
            when avg(daily_return) > 0.05 then 'Crypto market surge with ' || count(*) filter (where daily_return > 0.1) || ' coins gaining over 10%'
            when avg(daily_return) < -0.05 then 'Crypto market decline with widespread selling'
            else 'Mixed crypto performance with ' || round(avg(daily_return)::numeric * 100, 1) || '% average movement'
        end as natural_language_summary

    from {{ ref('int_crypto_analysis') }}
    where daily_return is not null
    group by date
),

anomaly_insights as (
    select
        date as summary_date,
        'market_anomalies' as insight_type,
        
        json_build_object(
            'total_anomalies', count(*),
            'critical_anomalies', count(*) filter (where anomaly_severity = 'Critical'),
            'high_anomalies', count(*) filter (where anomaly_severity = 'High'),
            'most_unusual_asset', first_value(asset_id) over (partition by date order by anomaly_score desc),
            'highest_anomaly_score', round(first_value(anomaly_score) over (partition by date order by anomaly_score desc)::numeric, 2),
            'common_anomaly_types', mode() within group (order by unnest(anomaly_types))
        ) as insight_data,
        
        case
            when count(*) filter (where anomaly_severity = 'Critical') > 0 then 
                count(*) filter (where anomaly_severity = 'Critical') || ' critical market anomalies detected'
            when count(*) > 5 then 'Multiple unusual market movements detected'
            else 'Normal market conditions with minimal anomalies'
        end as natural_language_summary

    from {{ ref('analytics_market_anomalies') }}
    group by date
),

economic_insights as (
    select
        date as summary_date,
        'economic_context' as insight_type,
        
        json_build_object(
            'fed_funds_rate', max(case when series_id = 'FEDFUNDS' then value end),
            'inflation_rate', max(case when series_id = 'CPIAUCSL' then value end),
            'unemployment_rate', max(case when series_id = 'UNRATE' then value end),
            'gdp_growth', max(case when series_id = 'GDP' then percentage_change end),
            'treasury_10y', max(case when series_id = 'DGS10' then value end),
            'active_signals', count(distinct economic_signal) filter (where economic_signal != 'Normal')
        ) as insight_data,
        
        'Economic context: ' ||
        case
            when max(case when series_id = 'FEDFUNDS' then value end) > 5 then 'High interest rate environment'
            when max(case when series_id = 'CPIAUCSL' then percentage_change end) > 0.5 then 'Elevated inflation pressures'
            when max(case when series_id = 'UNRATE' then value end) > 6 then 'Elevated unemployment levels'
            else 'Stable economic conditions'
        end as natural_language_summary

    from {{ ref('int_economic_analysis') }}
    group by date
),

all_insights as (
    select * from daily_market_insights
    union all
    select * from sector_insights
    union all
    select * from crypto_insights
    union all
    select * from anomaly_insights
    union all
    select * from economic_insights
)

select 
    summary_date,
    insight_type,
    insight_data,
    natural_language_summary,
    current_timestamp as created_at
from all_insights
order by summary_date desc, insight_type