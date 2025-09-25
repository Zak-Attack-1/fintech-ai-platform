{{ config(materialized='table') }}

with stock_anomalies as (
    select
        date,
        ticker as asset_id,
        company_name as asset_name,
        'Stock' as asset_type,
        sector,
        
        daily_return,
        volume,
        close_price,
        
        -- Z-score calculations for anomaly detection
        (daily_return - avg(daily_return) over (
            partition by ticker 
            order by date 
            rows between 60 preceding and current row
        )) / stddev(daily_return) over (
            partition by ticker 
            order by date 
            rows between 60 preceding and current row
        ) as return_z_score,
        
        (volume - avg(volume) over (
            partition by ticker 
            order by date 
            rows between 30 preceding and current row
        )) / stddev(volume) over (
            partition by ticker 
            order by date 
            rows between 30 preceding and current row
        ) as volume_z_score,
        
        -- Price gaps
        (open_price - lag(close_price) over (partition by ticker order by date)) / 
        lag(close_price) over (partition by ticker order by date) as price_gap,
        
        -- Moving average deviations
        price_vs_20d_ma,
        price_vs_50d_ma,
        
        rsi_14d,
        volatility_20d

    from {{ ref('int_stock_daily_analysis') }}
),

crypto_anomalies as (
    select
        date,
        symbol as asset_id,
        name as asset_name,
        'Crypto' as asset_type,
        crypto_category as sector,
        
        daily_return,
        volume_24h as volume,
        price_usd as close_price,
        
        -- Z-score calculations (crypto is more volatile)
        (daily_return - avg(daily_return) over (
            partition by symbol 
            order by date 
            rows between 30 preceding and current row
        )) / stddev(daily_return) over (
            partition by symbol 
            order by date 
            rows between 30 preceding and current row
        ) as return_z_score,
        
        (volume_24h - avg(volume_24h) over (
            partition by symbol 
            order by date 
            rows between 14 preceding and current row
        )) / stddev(volume_24h) over (
            partition by symbol 
            order by date 
            rows between 14 preceding and current row
        ) as volume_z_score,
        
        null as price_gap,  -- No traditional gaps in crypto
        price_vs_30d_ma as price_vs_20d_ma,
        price_vs_90d_ma as price_vs_50d_ma,
        
        rsi_14d,
        volatility_30d as volatility_20d

    from {{ ref('int_crypto_analysis') }}
),

all_anomalies as (
    select * from stock_anomalies
    union all
    select * from crypto_anomalies
),

anomaly_classification as (
    select
        *,
        -- Anomaly types
        case
            when abs(return_z_score) > 3 then 'Extreme Return'
            when abs(return_z_score) > 2 then 'Unusual Return'
            else null
        end as return_anomaly,
        
        case
            when volume_z_score > 3 then 'Volume Spike'
            when volume_z_score > 2 then 'High Volume'
            when volume_z_score < -2 then 'Low Volume'
            else null
        end as volume_anomaly,
        
        case
            when abs(price_gap) > 0.05 then 'Price Gap'
            else null
        end as gap_anomaly,
        
        case
            when rsi_14d > 90 then 'Extreme Overbought'
            when rsi_14d > 70 then 'Overbought'
            when rsi_14d < 10 then 'Extreme Oversold'
            when rsi_14d < 30 then 'Oversold'
            else null
        end as momentum_anomaly,
        
        case
            when volatility_20d > 0.05 and asset_type = 'Stock' then 'High Volatility'
            when volatility_20d > 0.15 and asset_type = 'Crypto' then 'High Volatility'
            else null
        end as volatility_anomaly,
        
        -- Composite anomaly score
        (coalesce(abs(return_z_score), 0) + 
         coalesce(abs(volume_z_score), 0) + 
         coalesce(abs(price_gap) * 20, 0)) / 3 as anomaly_score

    from all_anomalies
),

final_anomalies as (
    select
        *,
        -- Overall anomaly classification
        case
            when anomaly_score > 3 then 'Critical'
            when anomaly_score > 2 then 'High'
            when anomaly_score > 1.5 then 'Moderate'
            else 'Normal'
        end as anomaly_severity,
        
        -- Combine all anomaly types
        array_remove(array[
            return_anomaly,
            volume_anomaly,
            gap_anomaly,
            momentum_anomaly,
            volatility_anomaly
        ], null) as anomaly_types

    from anomaly_classification
)

select *
from final_anomalies
where anomaly_score > 1.5  -- Only include moderate to critical anomalies
order by date desc, anomaly_score desc