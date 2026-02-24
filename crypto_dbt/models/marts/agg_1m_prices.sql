{{
    config(
        materialized='incremental',
        schema='crypto_analytics',
        partition_by={
            'field': 'window_start',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by=['symbol'],
        incremental_strategy='insert_overwrite',
        on_schema_change='sync_all_columns'
    )
}}

/*
  agg_1m_prices
  -------------
  1-minute OHLC candlestick aggregation per symbol.

  Columns:
    window_start  - start of the 1-minute bucket (TIMESTAMP_TRUNC to MINUTE)
    window_end    - window_start + 1 minute
    symbol        - trading pair
    open_price    - first trade price in the window
    high_price    - highest trade price
    low_price     - lowest trade price
    close_price   - last trade price in the window
    avg_price     - volume-weighted average price (VWAP)
    volume        - total traded quantity (base asset)
    quote_volume  - total traded value (price * quantity)
    trade_count   - number of trades
    buy_volume    - quantity from taker buy orders (is_buyer_maker = FALSE)
    sell_volume   - quantity from taker sell orders (is_buyer_maker = TRUE)
*/

with base as (

    select * from {{ ref('stg_trades') }}

    {% if is_incremental() %}
        -- Only reprocess the last 2 completed minutes on incremental runs
        where event_ts >= TIMESTAMP_SUB(
            TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE),
            INTERVAL 2 MINUTE
        )
    {% endif %}

),

ohlc as (

    select
        event_minute                                            AS window_start,
        TIMESTAMP_ADD(event_minute, INTERVAL 1 MINUTE)         AS window_end,
        symbol,

        -- Open: first trade price in the window
        ARRAY_AGG(price ORDER BY event_ts ASC  LIMIT 1)[OFFSET(0)] AS open_price,

        -- Close: last trade price in the window
        ARRAY_AGG(price ORDER BY event_ts DESC LIMIT 1)[OFFSET(0)] AS close_price,

        MAX(price)                                              AS high_price,
        MIN(price)                                             AS low_price,

        -- VWAP = sum(price * qty) / sum(qty)
        SAFE_DIVIDE(
            SUM(price * quantity),
            SUM(quantity)
        )                                                       AS avg_price,

        SUM(quantity)                                           AS volume,
        SUM(price * quantity)                                   AS quote_volume,
        COUNT(*)                                                AS trade_count,

        -- Taker buy vs sell split
        SUM(CASE WHEN NOT is_buyer_maker THEN quantity ELSE 0 END) AS buy_volume,
        SUM(CASE WHEN     is_buyer_maker THEN quantity ELSE 0 END) AS sell_volume

    from base
    group by 1, 2, 3

)

select * from ohlc
