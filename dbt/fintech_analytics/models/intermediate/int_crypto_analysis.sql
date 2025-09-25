{{ config(materialized='table') }}

with crypto_data as (
    select * from {{ ref('stg_crypto_prices') }}
),

basic_calculations as (
    select
        symbol,
        name,
        date,
        price_usd,
        market_cap,
        volume_24h,
        crypto_category,
        market_cap_category,
        daily_return,
        
        -- Previous price for additional calculations
        lag(price_usd) over (partition by symbol order by date) as prev_price

    from crypto_data
    where daily_return is not null  -- Only include records with valid returns
),

moving_averages as (
    select 
        *,
        -- Moving averages
        avg(price_usd) over (
            partition by symbol 
            order by date 
            rows between 6 preceding and current row
        ) as ma_7d,
        
        avg(price_usd) over (
            partition by symbol 
            order by date 
            rows between 29 preceding and current row
        ) as ma_30d,
        
        avg(price_usd) over (
            partition by symbol 
            order by date 
            rows between 89 preceding and current row
        ) as ma_90d,
        
        -- Volume moving averages
        avg(volume_24h) over (
            partition by symbol 
            order by date 
            rows between 6 preceding and current row
        ) as avg_volume_7d,
        
        avg(volume_24h) over (
            partition by symbol 
            order by date 
            rows between 29 preceding and current row
        ) as avg_volume_30d

    from basic_calculations
),

volatility_and_momentum as (
    select 
        *,
        -- Volatility calculations
        stddev(daily_return) over (
            partition by symbol 
            order by date 
            rows between 6 preceding and current row
        ) as volatility_7d,
        
        stddev(daily_return) over (
            partition by symbol 
            order by date 
            rows between 29 preceding and current row
        ) as volatility_30d,
        
        -- Volume analysis
        (volume_24h / nullif(avg_volume_30d, 0) - 1) as volume_vs_30d_avg,
        
        -- Price momentum
        (price_usd / nullif(ma_30d, 0) - 1) as price_vs_30d_ma,
        (price_usd / nullif(ma_90d, 0) - 1) as price_vs_90d_ma,
        
        -- RSI components
        case when daily_return > 0 then daily_return else 0 end as gain,
        case when daily_return < 0 then abs(daily_return) else 0 end as loss

    from moving_averages
),

rsi_and_signals as (
    select 
        *,
        -- RSI calculation
        avg(gain) over (
            partition by symbol 
            order by date 
            rows between 13 preceding and current row
        ) as avg_gain_14d,
        
        avg(loss) over (
            partition by symbol 
            order by date 
            rows between 13 preceding and current row
        ) as avg_loss_14d,
        
        -- Maximum drawdown
        (price_usd / max(price_usd) over (
            partition by symbol 
            order by date 
            rows between 89 preceding and current row
        ) - 1) as max_drawdown_90d

    from volatility_and_momentum
),

final_calculations as (
    select 
        *,
        -- RSI
        case 
            when avg_loss_14d = 0 then 100
            when avg_gain_14d = 0 then 0
            else 100 - (100 / (1 + (avg_gain_14d / nullif(avg_loss_14d, 0))))
        end as rsi_14d,
        
        -- Crypto-specific signals
        case
            when daily_return > 0.2 then 'Pump'
            when daily_return < -0.2 then 'Dump'
            when abs(daily_return) < 0.02 then 'Stable'
            else 'Normal'
        end as daily_movement_signal,
        
        -- Create a simple market cap rank based on current data
        row_number() over (partition by date order by market_cap desc nulls last) as market_cap_rank_daily

    from rsi_and_signals
),

sentiment_and_correlations as (
    select 
        *,
        -- Sentiment signals
        case
            when rsi_14d > 80 then 'Extreme Greed'
            when rsi_14d > 70 then 'Greed'
            when rsi_14d < 20 then 'Extreme Fear'
            when rsi_14d < 30 then 'Fear'
            else 'Neutral'
        end as sentiment_signal,
        
        -- Market cap rank changes (using our calculated rank)
        market_cap_rank_daily - lag(market_cap_rank_daily) over (
            partition by symbol 
            order by date
        ) as rank_change,
        
        -- Add correlation with BTC (simplified approach)
        case 
            when symbol != 'BTC' then
                -- Simple correlation proxy: similar daily movement direction
                case 
                    when daily_return > 0 and 
                         exists (select 1 from final_calculations fc2 
                                where fc2.symbol = 'BTC' 
                                and fc2.date = final_calculations.date 
                                and fc2.daily_return > 0) then 1
                    when daily_return < 0 and 
                         exists (select 1 from final_calculations fc2 
                                where fc2.symbol = 'BTC' 
                                and fc2.date = final_calculations.date 
                                and fc2.daily_return < 0) then 1
                    else 0
                end
            else null
        end as correlation_with_btc_30d

    from final_calculations
)

select *
from sentiment_and_correlations
where date >= '{{ var("analysis_start_date") }}'
order by symbol, date