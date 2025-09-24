{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw_financial_data', 'crypto_prices') }}
),

cleaned_crypto as (
    select
        symbol,
        name,
        date,
        price_usd,
        market_cap,
        volume_24h,
        circulating_supply,
        total_supply,
        max_supply,
        price_change_24h,
        price_change_percentage_24h,
        market_cap_rank,
        data_source,
        created_at,
        
        -- Categorize cryptocurrencies
        case 
            when symbol in ('BTC') then 'Bitcoin'
            when symbol in ('ETH') then 'Ethereum'
            when symbol in ('USDT', 'USDC', 'DAI') then 'Stablecoin'
            when symbol in ('SOL', 'ADA', 'DOT', 'AVAX', 'NEAR') then 'Layer 1'
            when symbol in ('LINK', 'UNI') then 'DeFi'
            when symbol in ('DOGE', 'SHIB') then 'Meme'
            else 'Other'
        end as crypto_category,
        
        -- Market cap categories for crypto
        case
            when market_cap >= 100000000000 then 'Large Cap'     -- $100B+
            when market_cap >= 10000000000 then 'Mid Cap'        -- $10B-$100B
            when market_cap >= 1000000000 then 'Small Cap'       -- $1B-$10B
            when market_cap >= 100000000 then 'Micro Cap'        -- $100M-$1B
            else 'Nano Cap'                                       -- <$100M
        end as market_cap_category

    from source_data
    where 
        symbol is not null
        and date is not null
        and price_usd is not null
        and price_usd > 0
        -- Basic validation
        and (market_cap is null or market_cap >= 0)
        and (volume_24h is null or volume_24h >= 0)
),

final as (
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['symbol', 'date']) }} as crypto_price_id,
        
        -- Calculate returns
        (price_usd / nullif(lag(price_usd) over (
            partition by symbol 
            order by date
        ), 0) - 1) as daily_return,
        
        -- Row number for deduplication
        row_number() over (
            partition by symbol, date 
            order by created_at desc
        ) as row_num

    from cleaned_crypto
)

select *
from final
where row_num = 1  -- Keep most recent record per symbol/date