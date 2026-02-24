{{
    config(
        materialized='incremental',
        schema='crypto_analytics',
        partition_by={
            'field': 'window_start',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by=['symbol', 'is_alert'],
        incremental_strategy='insert_overwrite',
        on_schema_change='sync_all_columns'
    )
}}

/*
  volatility_alert_model
  ----------------------
  Per-minute volatility metrics and alert flags derived from agg_1m_prices.

  Volatility signals:
    1. price_range_pct    - (high - low) / avg * 100  → intra-bar spread
    2. return_pct         - close-to-close return vs previous minute
    3. ma_10m / ma_30m    - simple moving averages over 10 and 30 windows
    4. stddev_10m         - rolling 10-window price standard deviation
    5. z_score            - how many stddevs the current close is from the 10m mean
    6. buy_sell_ratio     - buy_volume / sell_volume  (> 1 = buying pressure)

  Alert rule  (is_alert = TRUE) when ANY of:
    - price_range_pct > 0.5   (intra-minute spread wider than 0.5 %)
    - ABS(return_pct)  > 1.0  (1-minute return larger than 1 %)
    - ABS(z_score)     > 2.0  (price more than 2 stddevs from 10-minute mean)
*/

with source as (

    select * from {{ ref('agg_1m_prices') }}

    {% if is_incremental() %}
        where window_start >= TIMESTAMP_SUB(
            TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE),
            INTERVAL 35 MINUTE          -- need 30 prior rows for moving windows
        )
    {% endif %}

),

with_windows as (

    select
        window_start,
        window_end,
        symbol,
        open_price,
        high_price,
        low_price,
        close_price,
        avg_price,
        volume,
        quote_volume,
        trade_count,
        buy_volume,
        sell_volume,

        -- ── Intra-bar spread ──────────────────────────────────────────────────
        ROUND(
            SAFE_DIVIDE(high_price - low_price, avg_price) * 100,
            4
        )                                                        AS price_range_pct,

        -- ── Close-to-close return ─────────────────────────────────────────────
        ROUND(
            SAFE_DIVIDE(
                close_price - LAG(close_price) OVER w,
                LAG(close_price) OVER w
            ) * 100,
            4
        )                                                        AS return_pct,

        -- ── Rolling 10-minute simple moving average ───────────────────────────
        ROUND(
            AVG(close_price) OVER (w ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
            6
        )                                                        AS ma_10m,

        -- ── Rolling 30-minute simple moving average ───────────────────────────
        ROUND(
            AVG(close_price) OVER (w ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
            6
        )                                                        AS ma_30m,

        -- ── Rolling 10-minute standard deviation ──────────────────────────────
        ROUND(
            STDDEV_POP(close_price) OVER (w ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
            6
        )                                                        AS stddev_10m,

        -- ── Buy/sell pressure ratio ───────────────────────────────────────────
        ROUND(
            SAFE_DIVIDE(buy_volume, sell_volume),
            4
        )                                                        AS buy_sell_ratio

    from source

    window w as (PARTITION BY symbol ORDER BY window_start)

),

with_z_score as (

    select
        *,
        ROUND(
            SAFE_DIVIDE(close_price - ma_10m, NULLIF(stddev_10m, 0)),
            4
        ) AS z_score

    from with_windows

),

final as (

    select
        *,
        -- ── Alert flag ────────────────────────────────────────────────────────
        (
            COALESCE(price_range_pct, 0) > 0.5
            OR ABS(COALESCE(return_pct, 0))   > 1.0
            OR ABS(COALESCE(z_score, 0))       > 2.0
        )                                                        AS is_alert,

        -- Human-readable alert reason (first matching rule wins)
        CASE
            WHEN ABS(COALESCE(z_score, 0))       > 2.0  THEN 'Z_SCORE_SPIKE'
            WHEN ABS(COALESCE(return_pct, 0))    > 1.0  THEN 'RETURN_SPIKE'
            WHEN COALESCE(price_range_pct, 0)    > 0.5  THEN 'WIDE_SPREAD'
            ELSE NULL
        END                                                      AS alert_reason,

        CURRENT_TIMESTAMP()                                      AS modeled_at

    from with_z_score

)

select * from final
