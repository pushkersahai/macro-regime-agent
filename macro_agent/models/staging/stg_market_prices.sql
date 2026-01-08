-- Staging model: Clean raw market prices
-- Reads from: RAW.MARKET_PRICES
-- Outputs to: BRONZE.STG_MARKET_PRICES (as a view)

WITH source AS (
    SELECT * FROM {{ source('raw', 'market_prices') }}
),

cleaned AS (
    SELECT
        UPPER(TRIM(ticker)) AS ticker,
        price_date,
        close_price,
        loaded_at,
        source
    FROM source
    WHERE price_date IS NOT NULL
      AND close_price IS NOT NULL
      AND close_price > 0
)

SELECT * FROM cleaned