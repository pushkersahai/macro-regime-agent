-- Staging model: Clean raw FRED indicators
-- Reads from: RAW.FRED_INDICATORS
-- Outputs to: BRONZE.STG_FRED_INDICATORS (as a view)

WITH source AS (
    SELECT * FROM {{ source('raw', 'fred_indicators') }}
),

cleaned AS (
    SELECT
        UPPER(TRIM(indicator_id)) AS indicator_id,
        observation_date,
        value,
        loaded_at,
        source
    FROM source
    WHERE observation_date IS NOT NULL
      AND value IS NOT NULL
)

SELECT * FROM cleaned