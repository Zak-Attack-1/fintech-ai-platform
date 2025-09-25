{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw_financial_data', 'economic_indicators') }}
),

cleaned_economic as (
    select
        series_id,
        series_name,
        date,
        value,
        units,
        frequency,
        seasonal_adjustment,
        notes,
        data_source,
        metadata,
        created_at,
        updated_at,
        
        -- Categorize economic indicators
        case 
            when series_id in ('GDP', 'GDPC1') then 'GDP & Growth'
            when series_id in ('CPIAUCSL', 'CPILFESL') then 'Inflation'
            when series_id in ('UNRATE', 'CIVPART', 'PAYEMS') then 'Employment'
            when series_id in ('FEDFUNDS', 'DGS10', 'DGS2', 'DGS30', 'TB3MS') then 'Interest Rates'
            when series_id in ('CSUSHPINSA', 'HOUST', 'HSN1F') then 'Housing'
            when series_id in ('INDPRO', 'UMCSENT', 'RSXFS', 'TOTALSL') then 'Business Activity'
            when series_id in ('M1SL', 'M2SL') then 'Money Supply'
            when series_id in ('DEXUSEU', 'DEXJPUS') then 'Exchange Rates'
            else 'Other'
        end as indicator_category,
        
        -- Standardize frequency
        case
            when lower(frequency) like '%daily%' then 'Daily'
            when lower(frequency) like '%weekly%' then 'Weekly'
            when lower(frequency) like '%monthly%' then 'Monthly'
            when lower(frequency) like '%quarterly%' then 'Quarterly'
            when lower(frequency) like '%annual%' then 'Annual'
            else coalesce(frequency, 'Unknown')
        end as frequency_standardized

    from source_data
    where 
        series_id is not null
        and date is not null
        and value is not null
        -- Remove obvious outliers (very basic)
        and abs(value) < 1e12  -- Exclude extremely large values
),

final as (
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['series_id', 'date']) }} as economic_indicator_id,
        
        -- Calculate period-over-period changes
        lag(value) over (
            partition by series_id 
            order by date
        ) as previous_value,
        
        value - lag(value) over (
            partition by series_id 
            order by date
        ) as absolute_change,
        
        (value / nullif(lag(value) over (
            partition by series_id 
            order by date
        ), 0) - 1) * 100 as percentage_change,
        
        -- Row number for deduplication
        row_number() over (
            partition by series_id, date 
            order by created_at desc
        ) as row_num

    from cleaned_economic
)

select *
from final
where row_num = 1  -- Keep most recent record per series/date