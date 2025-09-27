-- models/analytics/analytics_market_anomalies.sql
{{ config(materialized='table') }}

with asset_analysis as (
    select
        last_update_date as date,
        asset_symbol as asset_id,
        asset_name,
        asset_type,
        sector,
        latest_return as daily_return,
        current_price as close_price,
        current_volatility,
        current_rsi,
        
        -- Anomaly scoring based on available data
        case 
            when abs(latest_return) > 0.1 then abs(latest_return) * 10
            when abs(latest_return) > 0.05 then abs(latest_return) * 5  
            when abs(latest_return) > 0.02 then abs(latest_return) * 2
            else abs(latest_return)
        end as return_anomaly_score,
        
        case 
            when current_volatility > 0.05 then current_volatility * 10
            when current_volatility > 0.03 then current_volatility * 5
            else current_volatility * 2
        end as volatility_anomaly_score,
        
        case 
            when current_rsi > 80 or current_rsi < 20 then abs(50 - current_rsi) / 10
            else 0
        end as rsi_anomaly_score
        
    from {{ ref('mart_asset_performance') }}
    where latest_return is not null
),

anomaly_classification as (
    select
        *,
        -- Anomaly types
        case
            when abs(daily_return) > 0.1 then 'Extreme Return'
            when abs(daily_return) > 0.05 then 'Unusual Return'
            else null
        end as return_anomaly,
        
        case
            when current_rsi > 90 then 'Extreme Overbought'
            when current_rsi > 70 then 'Overbought'
            when current_rsi < 10 then 'Extreme Oversold'
            when current_rsi < 30 then 'Oversold'
            else null
        end as momentum_anomaly,
        
        case
            when current_volatility > 0.05 and asset_type = 'Stock' then 'High Volatility'
            when current_volatility > 0.15 and asset_type = 'Crypto' then 'High Volatility'
            else null
        end as volatility_anomaly,
        
        -- Composite anomaly score
        (return_anomaly_score + volatility_anomaly_score + rsi_anomaly_score) / 3 as anomaly_score

    from asset_analysis
),

final_anomalies as (
    select
        date,
        asset_id,
        asset_name,
        asset_type,
        sector,
        daily_return,
        close_price,
        current_volatility,
        current_rsi,
        return_anomaly,
        momentum_anomaly, 
        volatility_anomaly,
        anomaly_score,
        
        -- Anomaly severity
        case
            when anomaly_score > 3 then 'Critical'
            when anomaly_score > 2 then 'High'
            when anomaly_score > 1 then 'Moderate'
            else 'Normal'
        end as anomaly_severity,
        
        -- Anomaly types array
        array_remove(array[
            return_anomaly,
            momentum_anomaly,
            volatility_anomaly
        ], null) as anomaly_types

    from anomaly_classification
)

select 
    date,
    asset_id,
    asset_name,
    asset_type,
    sector,
    daily_return,
    close_price,
    current_volatility as volatility_20d,
    current_rsi as rsi_14d,
    anomaly_score,
    anomaly_severity,
    anomaly_types
from final_anomalies
where anomaly_score > 0.5
order by date desc, anomaly_score desc

---