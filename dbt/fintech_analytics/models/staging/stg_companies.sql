{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw_financial_data', 'companies') }}
),

cleaned_companies as (
    select
        ticker,
        company_name,
        sector,
        industry,
        market_cap,
        employees,
        founded_year,
        headquarters,
        website,
        description,
        is_sp500,
        is_active,
        created_at,
        updated_at,
        
        -- Clean and standardize sector names
        case 
            when lower(sector) like '%technology%' then 'Technology'
            when lower(sector) like '%financial%' then 'Financial Services'
            when lower(sector) like '%health%' then 'Healthcare'
            when lower(sector) like '%consumer%' then 'Consumer Discretionary'
            when lower(sector) like '%energy%' then 'Energy'
            when lower(sector) like '%industrial%' then 'Industrials'
            when lower(sector) like '%material%' then 'Materials'
            when lower(sector) like '%utilities%' then 'Utilities'
            when lower(sector) like '%real estate%' then 'Real Estate'
            when lower(sector) like '%communication%' then 'Communication Services'
            else coalesce(sector, 'Unknown')
        end as sector_standardized,
        
        -- Market cap categories
        case
            when market_cap >= 200000000000 then 'Mega Cap'      -- $200B+
            when market_cap >= 10000000000 then 'Large Cap'      -- $10B-$200B
            when market_cap >= 2000000000 then 'Mid Cap'         -- $2B-$10B
            when market_cap >= 300000000 then 'Small Cap'        -- $300M-$2B
            when market_cap >= 50000000 then 'Micro Cap'         -- $50M-$300M
            else 'Nano Cap'                                       -- <$50M
        end as market_cap_category

    from source_data
    where 
        ticker is not null
        and is_active = true
)

select 
    *,
    {{ dbt_utils.generate_surrogate_key(['ticker']) }} as company_id
from cleaned_companies