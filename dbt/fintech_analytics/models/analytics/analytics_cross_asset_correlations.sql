-- models/analytics/analytics_cross_asset_correlations.sql
{{ config(
    materialized='table',
    post_hook="create index if not exists idx_correlations_date on {{ this }} (analysis_date)"
) }}

with asset_returns as (
    select 
        asset_symbol as ticker,
        asset_type as asset_class,
        sector,
        latest_return as daily_return,
        last_update_date as date
    from {{ ref('mart_asset_performance') }}
    where latest_return is not null
),

correlation_pairs as (
    select
        current_date as analysis_date,
        a1.ticker as asset_1,
        a1.asset_class as asset_1_type,
        a1.sector as asset_1_sector,
        a2.ticker as asset_2,
        a2.asset_class as asset_2_type,
        a2.sector as asset_2_sector,
        
        -- For this simplified version, we'll use a proxy correlation
        -- based on same-day performance similarity
        case 
            when sign(a1.daily_return) = sign(a2.daily_return) then 0.5
            else -0.5
        end as correlation_coefficient,
        
        1 as observations,
        
        -- Simplified correlation strength
        case 
            when a1.sector = a2.sector and a1.asset_class = a2.asset_class then 'Significant'
            when a1.asset_class = a2.asset_class then 'Moderate'
            else 'Weak'
        end as correlation_strength,
        
        -- Simplified relationship type
        case
            when sign(a1.daily_return) = sign(a2.daily_return) then 'Positive'
            else 'Negative'
        end as relationship_type
 
    from asset_returns a1
    cross join asset_returns a2
    where a1.ticker != a2.ticker
      and a1.date = a2.date
)

select * 
from correlation_pairs 
where abs(correlation_coefficient) > 0.1
order by abs(correlation_coefficient) desc

---