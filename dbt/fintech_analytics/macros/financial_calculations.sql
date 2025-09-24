-- Calculate simple moving average
{% macro moving_average(column_name, window_size, partition_by=None, order_by='date') %}
  avg({{ column_name }}) over (
    {% if partition_by %}
    partition by {{ partition_by }}
    {% endif %}
    order by {{ order_by }}
    rows between {{ window_size - 1 }} preceding and current row
  )
{% endmacro %}

-- Calculate price returns
{% macro calculate_returns(price_column, partition_by=None, order_by='date') %}
  ({{ price_column }} / lag({{ price_column }}) over (
    {% if partition_by %}
    partition by {{ partition_by }}
    {% endif %}
    order by {{ order_by }}
  ) - 1)
{% endmacro %}

-- Calculate volatility (rolling standard deviation of returns)
{% macro calculate_volatility(returns_column, window_size, partition_by=None, order_by='date') %}
  stddev({{ returns_column }}) over (
    {% if partition_by %}
    partition by {{ partition_by }}
    {% endif %}
    order by {{ order_by }}
    rows between {{ window_size - 1 }} preceding and current row
  )
{% endmacro %}

-- Calculate RSI (Relative Strength Index)
{% macro calculate_rsi(price_column, window_size=14, partition_by=None, order_by='date') %}
  {% set gains_column = price_column ~ ' - lag(' ~ price_column ~ ') over (partition by ' ~ (partition_by or 'null') ~ ' order by ' ~ order_by ~ ')' %}
  {% set losses_column = 'case when (' ~ gains_column ~ ') < 0 then abs(' ~ gains_column ~ ') else 0 end' %}
  {% set avg_gains = 'avg(case when (' ~ gains_column ~ ') > 0 then (' ~ gains_column ~ ') else 0 end)' %}
  {% set avg_losses = 'avg(' ~ losses_column ~ ')' %}
  
  100 - (100 / (1 + (
    {{ avg_gains }} over (
      {% if partition_by %}
      partition by {{ partition_by }}
      {% endif %}
      order by {{ order_by }}
      rows between {{ window_size - 1 }} preceding and current row
    ) / 
    {{ avg_losses }} over (
      {% if partition_by %}
      partition by {{ partition_by }}
      {% endif %}
      order by {{ order_by }}
      rows between {{ window_size - 1 }} preceding and current row
    )
  )))
{% endmacro %}

-- Calculate Sharpe ratio
{% macro calculate_sharpe_ratio(returns_column, risk_free_rate=0.045, window_size=252, partition_by=None, order_by='date') %}
  (
    avg({{ returns_column }}) over (
      {% if partition_by %}
      partition by {{ partition_by }}
      {% endif %}
      order by {{ order_by }}
      rows between {{ window_size - 1 }} preceding and current row
    ) * 252 - {{ risk_free_rate }}
  ) / (
    stddev({{ returns_column }}) over (
      {% if partition_by %}
      partition by {{ partition_by }}
      {% endif %}
      order by {{ order_by }}
      rows between {{ window_size - 1 }} preceding and current row
    ) * sqrt(252)
  )
{% endmacro %}

-- Calculate correlation between two series
{% macro calculate_correlation(col1, col2, window_size, partition_by=None, order_by='date') %}
  (
    avg({{ col1 }} * {{ col2 }}) over (
      {% if partition_by %}
      partition by {{ partition_by }}
      {% endif %}
      order by {{ order_by }}
      rows between {{ window_size - 1 }} preceding and current row
    ) - 
    avg({{ col1 }}) over (
      {% if partition_by %}
      partition by {{ partition_by }}
      {% endif %}
      order by {{ order_by }}
      rows between {{ window_size - 1 }} preceding and current row
    ) * 
    avg({{ col2 }}) over (
      {% if partition_by %}
      partition by {{ partition_by }}
      {% endif %}
      order by {{ order_by }}
      rows between {{ window_size - 1 }} preceding and current row
    )
  ) / (
    stddev({{ col1 }}) over (
      {% if partition_by %}
      partition by {{ partition_by }}
      {% endif %}
      order by {{ order_by }}
      rows between {{ window_size - 1 }} preceding and current row
    ) * 
    stddev({{ col2 }}) over (
      {% if partition_by %}
      partition by {{ partition_by }}
      {% endif %}
      order by {{ order_by }}
      rows between {{ window_size - 1 }} preceding and current row
    )
  )
{% endmacro %}

-- Generate date spine for financial analysis
{% macro generate_date_spine(start_date, end_date) %}
  select 
    date_trunc('day', dd)::date as date
  from generate_series(
    '{{ start_date }}'::date,
    '{{ end_date }}'::date,
    '1 day'::interval
  ) dd
  where extract(dow from dd) not in (0, 6)  -- Exclude weekends
{% endmacro %}

-- Calculate market cap weighted returns
{% macro market_cap_weighted_return(return_column, market_cap_column, partition_by=None, order_by='date') %}
  sum({{ return_column }} * {{ market_cap_column }}) / sum({{ market_cap_column }})
{% endmacro %}

-- Risk-adjusted return metrics
{% macro calculate_beta(stock_returns, market_returns, window_size=252, partition_by=None, order_by='date') %}
  covar_samp({{ stock_returns }}, {{ market_returns }}) over (
    {% if partition_by %}
    partition by {{ partition_by }}
    {% endif %}
    order by {{ order_by }}
    rows between {{ window_size - 1 }} preceding and current row
  ) / var_samp({{ market_returns }}) over (
    {% if partition_by %}
    partition by {{ partition_by }}
    {% endif %}
    order by {{ order_by }}
    rows between {{ window_size - 1 }} preceding and current row
  )
{% endmacro %}