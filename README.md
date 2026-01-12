# Monthly Macro Regime Decision Agent

A production-ready analytics pipeline that ingests macro-economic data and outputs monthly investment regime decisions (RISK_ON, NEUTRAL, RISK_OFF) using dbt + Snowflake.

## Project Overview

**Business Problem:** Portfolio managers need systematic, data-driven signals to adjust asset allocation based on macro economic conditions.

**Solution:** Automated decision agent that:
- Ingests real-time macro indicators (unemployment, inflation, credit spreads, yield curve)
- Applies deterministic scoring logic
- Outputs monthly regime classification with confidence levels
- Provides transparent, auditable decision rationale

## Architecture

**Tech Stack:**
- **Data Warehouse:** Snowflake
- **Transformation:** dbt (Data Build Tool)
- **Ingestion:** Python + FRED API
- **Orchestration:** dbt (no Airflow - intentionally simple)

**Data Flow (Medallion Architecture):**
```
FRED API → RAW → BRONZE → SILVER → GOLD → DECISIONS
```

- **RAW:** Untransformed API data
- **BRONZE:** Cleaned, standardized data
- **SILVER:** Business logic (month-over-month changes, rolling averages)
- **GOLD:** Aggregated risk scores by category
- **DECISIONS:** Final regime output

## Data Sources

- **FRED (Federal Reserve Economic Data):**
  - UNRATE: Unemployment Rate
  - CPIAUCSL: Consumer Price Index (Inflation)
  - BAMLH0A0HYM2: High Yield Credit Spread
  - T10Y2Y: 10Y-2Y Treasury Yield Curve

## Getting Started

### Prerequisites
- Python 3.10+
- Snowflake account (free trial works)
- FRED API key (free from https://fred.stlouisfed.org/)

### Setup

1. **Clone repository**
```bash
git clone https://github.com/YOUR_USERNAME/macro-regime-agent.git
cd macro-regime-agent
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. **Configure credentials**

Create `.env` file in project root:
```
FRED_API_KEY=your_fred_api_key
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MACRO_AGENT
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

4. **Setup Snowflake**

Run SQL in Snowflake worksheet:
```sql
CREATE DATABASE IF NOT EXISTS MACRO_AGENT;
CREATE SCHEMA IF NOT EXISTS MACRO_AGENT.RAW;
CREATE SCHEMA IF NOT EXISTS MACRO_AGENT.BRONZE;
CREATE SCHEMA IF NOT EXISTS MACRO_AGENT.SILVER;
CREATE SCHEMA IF NOT EXISTS MACRO_AGENT.GOLD;
CREATE SCHEMA IF NOT EXISTS MACRO_AGENT.DECISIONS;
```

5. **Initialize dbt**
```bash
cd macro_agent
dbt debug  # Verify connection
dbt seed   # Load configuration tables
```

6. **Ingest data**
```bash
cd ..
python scripts/ingest_fred_data.py
```

7. **Run transformations**
```bash
cd macro_agent
dbt run
dbt test
```

## Usage

### View Latest Decisions
```sql
SELECT 
    month,
    regime,
    confidence,
    rationale
FROM MACRO_AGENT.DECISIONS.DECISION_MONTHLY_REGIME
ORDER BY month DESC
LIMIT 3;
```

### Update Data Monthly
```bash
python scripts/ingest_fred_data.py  # Fetch new data
cd macro_agent && dbt run            # Rebuild models
```

## Testing
```bash
cd macro_agent
dbt test  # Run all data quality tests
```

Tests include:
- Not null constraints
- Unique key validation
- Accepted values (regime must be RISK_ON/NEUTRAL/RISK_OFF)

##  Project Structure
```
macro-regime-agent/
├── macro_agent/              # dbt project
│   ├── models/
│   │   ├── staging/         # Bronze: Cleaned data
│   │   ├── intermediate/    # Silver: Business logic
│   │   ├── marts/           # Gold: Aggregates
│   │   └── decisions/       # Final outputs
│   ├── seeds/               # Configuration CSVs
│   ├── tests/               # Custom tests
│   └── dbt_project.yml
├── scripts/
│   └── ingest_fred_data.py  # FRED API ingestion
├── data/
│   └── config/              # Seed files
├── .env                     # Credentials (not in git)
├── requirements.txt
└── README.md
```

##  Key Concepts Demonstrated

- Medallion architecture (Bronze/Silver/Gold)
- Incremental data processing
- Deterministic decision logic (transparent, explainable AI)
- Data quality testing
- Production-ready SQL transformations
- Clear separation of concerns

**Business Value:**
- Automated monthly portfolio rebalancing signals
- Transparent decision rationale (no black-box)
- Audit trail (every decision has data lineage)
- Cost-efficient (runs on Snowflake free tier)

## Decision Logic

**Scoring System:**
- Each indicator scored -2 (very risky) to +1 (favorable)
- Average score calculated across all indicators
- Regime rules:
  - **RISK_ON:** avg_score ≥ 0.5 (favorable conditions)
  - **RISK_OFF:** avg_score ≤ -1.0 (defensive positioning)
  - **NEUTRAL:** Between -1.0 and 0.5 (mixed signals)

## Future Enhancements

- [ ] Add market price data (SPY, TLT, GLD returns)
- [ ] LLM-generated monthly memo (Claude/GPT-4)
- [ ] Email/Slack alerts on regime changes
- [ ] Backtesting framework
- [ ] dbt Cloud deployment

##  Author

**Pushker Sahai**
www.linkedin.com/in/pushkersahai


## License

MIT License - feel free to use for your own portfolio projects!