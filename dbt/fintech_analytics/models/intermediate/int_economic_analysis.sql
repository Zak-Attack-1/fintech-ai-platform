{{ config(materialized='table') }}

with economic_data as (
    select * from {{ ref('stg_economic_indicators') }}
),

economic_calculations as (
    select
        series_id,
        series_name,
        date,
        value,
        units,
        frequency_standardized as frequency,
        indicator_category,
        
        -- Previous values for comparison
        previous_value,
        absolute_change,
        percentage_change,
        
        -- Moving averages for smoothing
        {{ moving_average('value', 3, 'series_id', 'date') }} as ma_3_periods,
        {{ moving_average('value', 6, 'series_id', 'date') }} as ma_6_periods,
        {{ moving_average('value', 12, 'series_id', 'date') }} as ma_12_periods,
        
        -- Trend analysis
        case
            when {{ moving_average('percentage_change', 3, 'series_id', 'date') }} > 1 then 'Rising'
            when {{ moving_average('percentage_change', 3, 'series_id', 'date') }} < -1 then 'Falling'
            else 'Stable'
        end as trend_direction,
        
        -- Volatility of changes
        {{ calculate_volatility('percentage_change', 12, 'series_id', 'date') }} as volatility_12_periods,
        
        -- Z-score (how many standard deviations from mean)
        (value - {{ moving_average('value', 60, 'series_id', 'date') }}) / 
        nullif({{ calculate_volatility('value', 60, 'series_id', 'date') }}, 0) as z_score_5yr,
        
        -- Economic cycle indicators
        case 
            when series_id = 'UNRATE' and percentage_change > 0.5 then 'Deteriorating'
            when series_id = 'UNRATE' and percentage_change < -0.5 then 'Improving'
            when series_id = 'GDP' and percentage_change > 2 then 'Strong Growth'
            when series_id = 'GDP' and percentage_change < 0 then 'Recession Risk'
            when series_id = 'CPIAUCSL' and percentage_change > 0.5 then 'High Inflation'
            when series_id = 'CPIAUCSL' and percentage_change < 0 then 'Deflation Risk'
            else 'Normal'
        end as economic_signal,
        
        -- Recession indicators (Sahm Rule for unemployment)
        case 
            when series_id = 'UNRATE' and 
                 value - min(value) over (
                     partition by series_id 
                     order by date 
                     rows between 12 preceding and current row
                 ) >= 0.5 
            then true
            else false
        end as sahm_recession_indicator

    from economic_data
),

economic_relationships as (
    select 
        *,
        -- Fed funds vs inflation spread
        case 
            when series_id = 'FEDFUNDS' then
                value - coalesce((
                    select value 
                    from economic_calculations ec2 
                    where ec2.series_id = 'CPIAUCSL' 
                    and ec2.date = economic_calculations.date
                ), 0)
        end as real_fed_funds_rate,
        
        -- Yield curve (10Y - 2Y spread)
        case 
            when series_id = 'DGS10' then
                value - coalesce((
                    select value 
                    from economic_calculations ec2 
                    where ec2.series_id = 'DGS2' 
                    and ec2.date = economic_calculations.date
                ), 0)
        end as yield_curve_spread

    from economic_calculations
)

select *
from economic_relationships
where date >= '{{ var("analysis_start_date") }}'