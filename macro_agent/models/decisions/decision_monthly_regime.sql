-- Decision model: Monthly regime classification
-- Reads from: GOLD.FCT_REGIME_INPUTS
-- Outputs to: DECISIONS.DECISION_MONTHLY_REGIME

WITH latest_inputs AS (
    SELECT *
    FROM {{ ref('fct_regime_inputs') }}
    WHERE month >= DATEADD('YEAR', -5, CURRENT_DATE())  -- Last 12 months
),

regime_decision AS (
    SELECT
        month,
        avg_risk_score,
        growth_score,
        inflation_score,
        credit_score,
        unemployment_rate,
        credit_spread,
        
        -- Decision logic based on average risk score
        CASE
            WHEN avg_risk_score >= 0.5 THEN 'RISK_ON'
            WHEN avg_risk_score <= -1.0 THEN 'RISK_OFF'
            ELSE 'NEUTRAL'
        END AS regime,
        
        -- Confidence level
        CASE
            WHEN ABS(avg_risk_score) >= 1.5 THEN 'HIGH'
            WHEN ABS(avg_risk_score) >= 0.75 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS confidence,
        
        -- Generate human-readable rationale
        CONCAT(
            'Regime: ', 
            CASE
                WHEN avg_risk_score >= 0.5 THEN 'RISK_ON'
                WHEN avg_risk_score <= -1.0 THEN 'RISK_OFF'
                ELSE 'NEUTRAL'
            END,
            ' | Avg Score: ', ROUND(avg_risk_score, 2),
            ' | Growth: ', COALESCE(ROUND(growth_score, 2), 0),
            ' | Inflation: ', COALESCE(ROUND(inflation_score, 2), 0),
            ' | Credit: ', COALESCE(ROUND(credit_score, 2), 0),
            ' | Unemployment: ', ROUND(unemployment_rate, 2), '%',
            ' | Credit Spread: ', ROUND(credit_spread, 2), 'bps'
        ) AS rationale,
        
        CURRENT_TIMESTAMP() AS decision_timestamp
        
    FROM latest_inputs
)

SELECT 
    month,
    regime,
    confidence,
    avg_risk_score,
    growth_score,
    inflation_score,
    credit_score,
    unemployment_rate,
    credit_spread,
    rationale,
    decision_timestamp
FROM regime_decision
ORDER BY month DESC