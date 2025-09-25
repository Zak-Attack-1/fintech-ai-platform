{{ config(
    materialized='table',
    indexes=[
      {'columns': ['ticker', 'date'], 'type': 'btree'},
      {'columns': ['date'], 'type': 'btree'},
      {'columns': ['sector'], 'type': 'hash'}
    ]
) }}

with stock_prices as (
    select * from {{ ref('stg_stock_prices') }}
),

companies as (
    select * from {{ ref('stg_companies') }}
),

basic_calculations as (
    select 
        sp.ticker,
        sp.date,
        sp.open_price,
        sp.high_price,
        sp.low_price,
        sp.close_price,
        sp.adj_close_price,
        sp.volume,
        sp.dividends,
        sp.stock_splits,
        
        -- Company information
        c.company_name,
        c.sector_standardized as sector,
        c.industry,
        c.market_cap,
        c.market_cap_category,
        c.is_sp500,
        
        -- Basic price calculations
        (sp.high_price - sp.low_price) as daily_range,
        (sp.high_price - sp.low_price) / nullif(sp.close_price, 0) as daily_range_pct,
        
        -- Previous day's price for return calculation
        lag(sp.adj_close_price) over (partition by sp.ticker order by sp.date) as prev_adj_close

    from stock_prices sp
    left join companies c on sp.ticker = c.ticker
),

returns_and_moving_averages as (
    select 
        *,
        -- Calculate returns using previous price
        (adj_close_price / nullif(prev_adj_close, 0) - 1) as daily_return,
        
        -- Moving averages (simplified)
        avg(adj_close_price) over (
            partition by ticker 
            order by date 
            rows between 9 preceding and current row
        ) as sma_short,
        
        avg(adj_close_price) over (
            partition by ticker 
            order by date 
            rows between 29 preceding and current row
        ) as sma_long,
        
        avg(volume) over (
            partition by ticker 
            order by date 
            rows between 9 preceding and current row
        ) as avg_volume_10d,
        
        -- 20-day moving average for other calculations
        avg(adj_close_price) over (
            partition by ticker 
            order by date 
            rows between 19 preceding and current row
        ) as sma_20d,
        
        -- 50-day moving average
        avg(adj_close_price) over (
            partition by ticker 
            order by date 
            rows between 49 preceding and current row
        ) as sma_50d

    from basic_calculations
),

volatility_and_indicators as (
    select 
        *,
        -- Volatility calculation (standard deviation of returns)
        stddev(daily_return) over (
            partition by ticker 
            order by date 
            rows between 19 preceding and current row
        ) as volatility_20d,
        
        -- Price vs moving averages
        (adj_close_price / nullif(sma_20d, 0) - 1) as price_vs_20d_ma,
        (adj_close_price / nullif(sma_50d, 0) - 1) as price_vs_50d_ma,
        
        -- Volume vs average
        (volume / nullif(avg_volume_10d, 0) - 1) as volume_vs_avg,
        
        -- RSI components (simplified approach)
        case when daily_return > 0 then daily_return else 0 end as gain,
        case when daily_return < 0 then abs(daily_return) else 0 end as loss

    from returns_and_moving_averages
),

rsi_calculation as (
    select 
        *,
        -- RSI calculation using average gains and losses
        avg(gain) over (
            partition by ticker 
            order by date 
            rows between 13 preceding and current row
        ) as avg_gain_14d,
        
        avg(loss) over (
            partition by ticker 
            order by date 
            rows between 13 preceding and current row
        ) as avg_loss_14d

    from volatility_and_indicators
),

final_calculations as (
    select 
        *,
        -- RSI calculation
        case 
            when avg_loss_14d = 0 then 100
            when avg_gain_14d = 0 then 0
            else 100 - (100 / (1 + (avg_gain_14d / nullif(avg_loss_14d, 0))))
        end as rsi_14d,
        
        -- Bollinger Bands
        sma_20d + (2 * volatility_20d) as bollinger_upper,
        sma_20d - (2 * volatility_20d) as bollinger_lower,
        
        -- Trading signals
        case 
            when sma_short > sma_long then 'Golden Cross'
            when sma_short < sma_long then 'Death Cross'
            else 'Neutral'
        end as ma_signal,
        
        -- Sharpe ratio (simplified - annualized)
        case 
            when volatility_20d > 0 then 
                (avg(daily_return) over (
                    partition by ticker 
                    order by date 
                    rows between 251 preceding and current row
                ) * 252 - {{ var('risk_free_rate') }}) / 
                (stddev(daily_return) over (
                    partition by ticker 
                    order by date 
                    rows between 251 preceding and current row
                ) * sqrt(252))
            else null
        end as sharpe_ratio_1yr

    from rsi_calculation
),

signals_and_final as (
    select 
        *,
        -- RSI signal
        case
            when rsi_14d > 70 then 'Overbought'
            when rsi_14d < 30 then 'Oversold'
            else 'Neutral'
        end as rsi_signal,
        
        -- Maximum drawdown (simplified)
        (adj_close_price / max(adj_close_price) over (
            partition by ticker 
            order by date 
            rows between 251 preceding and current row
        ) - 1) as drawdown_1yr

    from final_calculations
)

select *
from signals_and_final
where date >= '{{ var("analysis_start_date") }}'
  and date <= '{{ var("analysis_end_date") }}'