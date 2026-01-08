-- Gold model: Aggregate all indicators into decision-ready format
-- Reads from: SILVER.INT_MACRO_SIGNALS and RAW.SEED_INDICATORS
-- Outputs to: GOLD.FCT_REGIME_INPUTS

WITH latest_signals AS (
    SELECT
        s.month,
        s.indicator_id,
        s.current_value,
        s.mom_change_pct,
        s.yoy_change_pct,
        s.rolling_3month_avg,
        c.category,
        c.interpretation
    FROM {{ ref('int_macro_signals') }} s
    LEFT JOIN {{ source('raw', 'seed_indicators') }} c
        ON s.indicator_id = c.indicator_id
    WHERE c.is_active = TRUE
),

scored_signals AS (
    SELECT
        month,
        indicator_id,
        category,
        current_value,
        mom_change_pct,
        yoy_change_pct,
        
        -- Score based on interpretation and change direction
        CASE 
            -- For HIGHER_IS_RISKIER indicators
            WHEN interpretation = 'HIGHER_IS_RISKIER' THEN
                CASE
                    WHEN yoy_change_pct > 10 THEN -2  -- Strong negative signal
                    WHEN yoy_change_pct > 0 THEN -1   -- Negative signal
                    WHEN yoy_change_pct > -10 THEN 0  -- Neutral
                    ELSE 1                             -- Positive signal
                END
            -- For LOWER_IS_RISKIER indicators  
            WHEN interpretation = 'LOWER_IS_RISKIER' THEN
                CASE
                    WHEN yoy_change_pct < -10 THEN -2 -- Strong negative signal
                    WHEN yoy_change_pct < 0 THEN -1   -- Negative signal
                    WHEN yoy_change_pct < 10 THEN 0   -- Neutral
                    ELSE 1                             -- Positive signal
                END
            ELSE 0
        END AS risk_score
        
    FROM latest_signals
),

monthly_aggregate AS (
    SELECT
        month,
        
        -- Count of indicators
        COUNT(DISTINCT indicator_id) AS indicator_count,
        
        -- Average risk score across all indicators
        AVG(risk_score) AS avg_risk_score,
        
        -- Category-level scores
        AVG(CASE WHEN category = 'GROWTH' THEN risk_score END) AS growth_score,
        AVG(CASE WHEN category = 'INFLATION' THEN risk_score END) AS inflation_score,
        AVG(CASE WHEN category = 'RATES' THEN risk_score END) AS rates_score,
        AVG(CASE WHEN category = 'CREDIT' THEN risk_score END) AS credit_score,
        
        -- Raw values for reference
        MAX(CASE WHEN indicator_id = 'UNRATE' THEN current_value END) AS unemployment_rate,
        MAX(CASE WHEN indicator_id = 'CPIAUCSL' THEN yoy_change_pct END) AS cpi_yoy_change,
        MAX(CASE WHEN indicator_id = 'BAMLH0A0HYM2' THEN current_value END) AS credit_spread
        
    FROM scored_signals
    GROUP BY month
)

SELECT 
    month,
    indicator_count,
    ROUND(avg_risk_score, 2) AS avg_risk_score,
    ROUND(growth_score, 2) AS growth_score,
    ROUND(inflation_score, 2) AS inflation_score,
    ROUND(rates_score, 2) AS rates_score,
    ROUND(credit_score, 2) AS credit_score,
    ROUND(unemployment_rate, 2) AS unemployment_rate,
    ROUND(cpi_yoy_change, 2) AS cpi_yoy_change,
    ROUND(credit_spread, 2) AS credit_spread,
    CURRENT_TIMESTAMP() AS calculated_at
FROM monthly_aggregate
WHERE month IS NOT NULL
ORDER BY month DESC