{{ config(
    materialized='view',
    indexes=[
      {'columns': ['ticker', 'date'], 'type': 'btree'},
      {'columns': ['date'], 'type': 'btree'}
    ]
) }}

with source_data as (
    select * from {{ source('raw_financial_data', 'stock_prices') }}
),

cleaned_data as (
    select
        ticker,
        date,
        open_price,
        high_price,
        low_price,
        close_price,
        adj_close_price,
        volume,
        dividends,
        stock_splits,
        data_source,
        created_at,
        
        -- Data quality flags
        case 
            when close_price <= 0 then true
            when volume < 0 then true
            when high_price < low_price then true
            when high_price < close_price then true
            when low_price > close_price then true
            else false
        end as has_data_quality_issues,
        
        -- Price validations
        case
            when high_price = low_price and close_price = open_price then true
            else false
        end as is_suspicious_flat_price

    from source_data
    where 
        close_price is not null
        and date is not null
        and ticker is not null
        -- Remove obvious data errors
        and close_price > 0
        and volume >= 0
        and high_price >= low_price
        and high_price >= close_price
        and low_price <= close_price
),

final as (
    select 
        *,
        -- Generate a unique row identifier
        {{ dbt_utils.generate_surrogate_key(['ticker', 'date']) }} as stock_price_id,
        
        -- Add row number for deduplication
        row_number() over (
            partition by ticker, date 
            order by created_at desc
        ) as row_num

    from cleaned_data
)

select * 
from final
where row_num = 1  -- Keep only the most recent record per ticker/date