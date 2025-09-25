{{ config(materialized='table') }}

with stock_performance as (
    select
        ticker as asset_symbol,
        company_name as asset_name,
        'Stock' as asset_type,
        sector,
        market_cap_category,
        
        -- Performance metrics (last available date)
        first_value(adj_close_price) over (partition by ticker order by date desc) as current_price,
        first_value(daily_return) over (partition by ticker order by date desc) as latest_return,
        first_value(volatility_20d) over (partition by ticker order by date desc) as current_volatility,
        first_value(rsi_14d) over (partition by ticker order by date desc) as current_rsi,
        first_value(sharpe_ratio_1yr) over (partition by ticker order by date desc) as sharpe_ratio,
        
        -- Return calculations
        (first_value(adj_close_price) over (partition by ticker order by date desc) /
         first_value(adj_close_price) over (partition by ticker order by date asc) - 1) as total_return,
         
        -- Simplified annualized return calculation
        case 
            when count(*) over (partition by ticker) >= 252 then
                power(
                    first_value(adj_close_price) over (partition by ticker order by date desc) /
                    first_value(adj_close_price) over (partition by ticker order by date asc),
                    252.0 / count(*) over (partition by ticker)
                ) - 1
            else null
        end as annualized_return,
        
        -- Annualized volatility
        stddev(daily_return) over (partition by ticker) * sqrt(252) as annualized_volatility,
        
        -- Risk metrics
        min(drawdown_1yr) over (partition by ticker) as max_drawdown,
        
        -- Simple beta calculation (correlation with market average)
        corr(daily_return, avg(daily_return) over (partition by date)) over (partition by ticker) as beta_proxy,
        
        -- Signal summary using string_agg instead of mode()
        string_agg(distinct ma_signal, ', ') over (partition by ticker) as dominant_ma_signal,
        string_agg(distinct rsi_signal, ', ') over (partition by ticker) as dominant_rsi_signal,
        
        count(*) over (partition by ticker) as days_of_data,
        max(date) over (partition by ticker) as last_update_date,
        
        -- Add row number to get one record per ticker
        row_number() over (partition by ticker order by date desc) as rn

    from {{ ref('int_stock_daily_analysis') }}
    where daily_return is not null
),

crypto_performance as (
    select
        symbol as asset_symbol,
        name as asset_name,
        'Crypto' as asset_type,
        crypto_category as sector,
        market_cap_category,
        
        -- Performance metrics
        first_value(price_usd) over (partition by symbol order by date desc) as current_price,
        first_value(daily_return) over (partition by symbol order by date desc) as latest_return,
        first_value(volatility_30d) over (partition by symbol order by date desc) as current_volatility,
        first_value(rsi_14d) over (partition by symbol order by date desc) as current_rsi,
        null::numeric as sharpe_ratio,  -- Not calculated for crypto in this model
        
        -- Return calculations
        (first_value(price_usd) over (partition by symbol order by date desc) /
         first_value(price_usd) over (partition by symbol order by date asc) - 1) as total_return,
         
        -- Simplified annualized return for crypto (365 days)
        case 
            when count(*) over (partition by symbol) >= 365 then
                power(
                    first_value(price_usd) over (partition by symbol order by date desc) /
                    first_value(price_usd) over (partition by symbol order by date asc),
                    365.0 / count(*) over (partition by symbol)
                ) - 1
            else null
        end as annualized_return,
        
        stddev(daily_return) over (partition by symbol) * sqrt(365) as annualized_volatility,
        
        -- Risk metrics
        min(max_drawdown_90d) over (partition by symbol) as max_drawdown,
        first_value(correlation_with_btc_30d) over (partition by symbol order by date desc) as beta_proxy,
        
        -- Signal summary
        string_agg(distinct daily_movement_signal, ', ') over (partition by symbol) as dominant_ma_signal,
        string_agg(distinct sentiment_signal, ', ') over (partition by symbol) as dominant_rsi_signal,
        
        count(*) over (partition by symbol) as days_of_data,
        max(date) over (partition by symbol) as last_update_date,
        
        -- Add row number to get one record per symbol
        row_number() over (partition by symbol order by date desc) as rn

    from {{ ref('int_crypto_analysis') }}
    where daily_return is not null
),

combined_performance as (
    select 
        asset_symbol,
        asset_name,
        asset_type,
        sector,
        market_cap_category,
        current_price,
        latest_return,
        current_volatility,
        current_rsi,
        sharpe_ratio,
        total_return,
        annualized_return,
        annualized_volatility,
        max_drawdown,
        beta_proxy,
        dominant_ma_signal,
        dominant_rsi_signal,
        days_of_data,
        last_update_date
    from stock_performance
    where rn = 1  -- One record per ticker
    
    union all
    
    select 
        asset_symbol,
        asset_name,
        asset_type,
        sector,
        market_cap_category,
        current_price,
        latest_return,
        current_volatility,
        current_rsi,
        sharpe_ratio,
        total_return,
        annualized_return,
        annualized_volatility,
        max_drawdown,
        beta_proxy,
        dominant_ma_signal,
        dominant_rsi_signal,
        days_of_data,
        last_update_date
    from crypto_performance
    where rn = 1  -- One record per symbol
),

ranked_performance as (
    select
        *,
        -- Performance rankings
        row_number() over (partition by asset_type order by total_return desc) as return_rank,
        row_number() over (partition by asset_type order by annualized_return desc nulls last) as annualized_return_rank,
        row_number() over (partition by asset_type order by current_volatility asc nulls last) as low_volatility_rank,
        row_number() over (partition by asset_type order by sharpe_ratio desc nulls last) as sharpe_rank,
        
        -- Risk-adjusted performance categories
        case
            when annualized_return > 0.15 and annualized_volatility < 0.25 then 'High Return Low Risk'
            when annualized_return > 0.15 and annualized_volatility > 0.25 then 'High Return High Risk'
            when coalesce(annualized_return, 0) < 0.05 and annualized_volatility < 0.15 then 'Low Return Low Risk'
            when coalesce(annualized_return, 0) < 0.05 and annualized_volatility > 0.25 then 'Low Return High Risk'
            else 'Moderate'
        end as risk_return_profile

    from combined_performance
)

select *
from ranked_performance
order by asset_type, return_rank