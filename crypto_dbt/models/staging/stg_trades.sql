{{
    config(
        materialized='view',
        schema='crypto_analytics'
    )
}}

/*
  stg_trades
  ----------
  Staging layer over crypto_raw.trades.
  - Casts price / quantity to NUMERIC for precision
  - Exposes clean column names and a derived trade_date partition column
  - Filters out any rows with missing critical fields
*/

with source as (

    select * from {{ source('crypto_raw', 'trades') }}

),

renamed as (

    select
        -- timestamps
        CAST(event_ts  AS TIMESTAMP)                AS event_ts,
        CAST(ingest_ts AS TIMESTAMP)                AS ingest_ts,

        -- dimensions
        UPPER(TRIM(symbol))                         AS symbol,
        CAST(trade_id AS INT64)                     AS trade_id,
        CAST(is_buyer_maker AS BOOL)                AS is_buyer_maker,

        -- measures
        CAST(price    AS NUMERIC)                   AS price,
        CAST(quantity AS NUMERIC)                   AS quantity,

        -- derived
        DATE(event_ts)                              AS trade_date,
        TIMESTAMP_TRUNC(event_ts, MINUTE)           AS event_minute,
        TIMESTAMP_TRUNC(event_ts, HOUR)             AS event_hour

    from source

    where
        event_ts  is not null
        and price    > 0
        and quantity > 0
        and symbol   is not null

)

select * from renamed
