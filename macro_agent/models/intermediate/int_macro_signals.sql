-- Intermediate model: Calculate macro signals from indicators
-- Reads from: BRONZE.STG_FRED_INDICATORS
-- Outputs to: SILVER.INT_MACRO_SIGNALS

WITH monthly_data AS (
    -- Get end-of-month observations only
    SELECT 
        indicator_id,
        DATE_TRUNC('MONTH', observation_date) AS month,
        AVG(value) AS monthly_avg_value
    FROM {{ ref('stg_fred_indicators') }}
    GROUP BY indicator_id, DATE_TRUNC('MONTH', observation_date)
),

with_changes AS (
    SELECT
        indicator_id,
        month,
        monthly_avg_value AS current_value,
        LAG(monthly_avg_value, 1) OVER (
            PARTITION BY indicator_id 
            ORDER BY month
        ) AS prev_month_value,
        LAG(monthly_avg_value, 3) OVER (
            PARTITION BY indicator_id 
            ORDER BY month
        ) AS prev_3month_value,
        LAG(monthly_avg_value, 12) OVER (
            PARTITION BY indicator_id 
            ORDER BY month
        ) AS prev_12month_value
    FROM monthly_data
),

calculated_signals AS (
    SELECT
        indicator_id,
        month,
        current_value,
        
        -- Month-over-month change
        CASE 
            WHEN prev_month_value IS NOT NULL 
            THEN ((current_value - prev_month_value) / prev_month_value) * 100
            ELSE NULL
        END AS mom_change_pct,
        
        -- 3-month change
        CASE 
            WHEN prev_3month_value IS NOT NULL 
            THEN ((current_value - prev_3month_value) / prev_3month_value) * 100
            ELSE NULL
        END AS qoq_change_pct,
        
        -- 12-month change (year-over-year)
        CASE 
            WHEN prev_12month_value IS NOT NULL 
            THEN ((current_value - prev_12month_value) / prev_12month_value) * 100
            ELSE NULL
        END AS yoy_change_pct,
        
        -- 3-month rolling average
        AVG(current_value) OVER (
            PARTITION BY indicator_id 
            ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3month_avg
        
    FROM with_changes
)

SELECT * FROM calculated_signals
WHERE month >= DATEADD('MONTH', -36, CURRENT_DATE())  -- Keep last 3 years
ORDER BY indicator_id, month