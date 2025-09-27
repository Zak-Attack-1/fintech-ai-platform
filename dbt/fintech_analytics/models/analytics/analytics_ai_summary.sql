-- models/analytics/analytics_ai_summary.sql
{{ config(
    materialized='table',
    post_hook="create index if not exists idx_ai_summary_date on {{ this }} (summary_date)"
) }}

with daily_market_insights as (
    select
        current_date as summary_date,
        'market_overview' as insight_type,
        
        json_build_object(
            'total_assets_analyzed', count(*),
            'stock_count', count(*) filter (where asset_type = 'Stock'),
            'crypto_count', count(*) filter (where asset_type = 'Crypto'),
            'sectors_active', count(distinct sector) filter (where sector is not null),
            'average_return', round(avg(latest_return)::numeric, 4),
            'market_volatility', round(stddev(latest_return)::numeric, 4),
            'positive_assets_pct', round((count(*) filter (where latest_return > 0)::float / count(*) * 100)::numeric, 1),
            'top_gaining_asset', (array_agg(asset_symbol order by latest_return desc))[1],
            'top_gaining_return', round((array_agg(latest_return order by latest_return desc))[1]::numeric, 4),
            'top_losing_asset', (array_agg(asset_symbol order by latest_return asc))[1],
            'top_losing_return', round((array_agg(latest_return order by latest_return asc))[1]::numeric, 4)
        ) as insight_data,
        
        -- Natural language summary
        case
            when avg(latest_return) > 0.02 then 'Strong bull market with ' || round((count(*) filter (where latest_return > 0)::float / count(*) * 100)::numeric, 0) || '% of assets posting gains'
            when avg(latest_return) > 0.005 then 'Moderate positive market with mixed performance across sectors'
            when avg(latest_return) < -0.02 then 'Bear market with widespread selling pressure'
            when stddev(latest_return) > 0.03 then 'High volatility session with significant price swings'
            else 'Calm trading session with modest price movements'
        end as natural_language_summary

    from {{ ref('mart_asset_performance') }}
    where latest_return is not null
),

sector_performance_base as (
    select
        asset_type,
        sector,
        avg(latest_return) as avg_sector_return,
        count(*) as asset_count,
        (array_agg(asset_symbol order by latest_return desc))[1] as best_performer,
        (array_agg(asset_symbol order by latest_return asc))[1] as worst_performer
    from {{ ref('mart_asset_performance') }}
    where latest_return is not null and sector is not null
    group by asset_type, sector
),

sector_insights as (
    select
        current_date as summary_date,
        'sector_performance' as insight_type,
        
        json_agg(
            json_build_object(
                'sector', sector,
                'asset_type', asset_type,
                'avg_return', round(avg_sector_return::numeric, 4),
                'asset_count', asset_count,
                'best_performer', best_performer,
                'worst_performer', worst_performer
            ) order by avg_sector_return desc
        ) as insight_data,
        
        'Sector performance: ' || 
        coalesce((array_agg(sector order by avg_sector_return desc))[1], 'Mixed sectors') ||
        ' showed ' ||
        case 
            when avg(avg_sector_return) > 0 then 'gains'
            else 'declines' 
        end ||
        ' with ' ||
        round(coalesce((array_agg(avg_sector_return order by avg_sector_return desc))[1], 0)::numeric * 100, 1) ||
        '% average return' as natural_language_summary

    from sector_performance_base
),

anomaly_insights as (
    select
        current_date as summary_date,
        'market_anomalies' as insight_type,
        
        json_build_object(
            'total_anomalies', count(*),
            'critical_anomalies', count(*) filter (where anomaly_severity = 'Critical'),
            'high_anomalies', count(*) filter (where anomaly_severity = 'High'),
            'most_unusual_asset', (array_agg(asset_id order by anomaly_score desc))[1],
            'highest_anomaly_score', round((array_agg(anomaly_score order by anomaly_score desc))[1]::numeric, 2)
        ) as insight_data,
        
        case
            when count(*) filter (where anomaly_severity = 'Critical') > 0 then 
                count(*) filter (where anomaly_severity = 'Critical') || ' critical market anomalies detected'
            when count(*) > 5 then 'Multiple unusual market movements detected'
            else 'Normal market conditions with minimal anomalies'
        end as natural_language_summary

    from {{ ref('analytics_market_anomalies') }}
),

all_insights as (
    select * from daily_market_insights
    union all
    select * from sector_insights
    union all
    select * from anomaly_insights
)

select 
    summary_date,
    insight_type,
    insight_data,
    natural_language_summary,
    current_timestamp as created_at
from all_insights
order by summary_date desc, insight_type