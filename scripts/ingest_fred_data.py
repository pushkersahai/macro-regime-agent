"""
Fetch macro indicators from FRED API and load into Snowflake RAW schema
"""
import os
from datetime import datetime, timedelta
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables
load_dotenv()

# Initialize FRED API
fred = Fred(api_key=os.getenv('FRED_API_KEY'))

# Snowflake connection parameters
SNOWFLAKE_PARAMS = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'role': os.getenv('SNOWFLAKE_ROLE')
}

def fetch_fred_indicators():
    """Fetch indicators from FRED based on config table"""
    
    # Connect to Snowflake
    conn = snowflake.connector.connect(**SNOWFLAKE_PARAMS)
    cursor = conn.cursor()
    
    # Get list of active indicators
    cursor.execute("""
        SELECT indicator_id, fred_series_id 
        FROM SEED_INDICATORS 
        WHERE is_active = TRUE
    """)
    indicators = cursor.fetchall()
    
    print(f"Found {len(indicators)} active indicators to fetch")
    
    # Fetch last 5 years of data for each indicator
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5*365)
    
    all_data = []
    
    for indicator_id, fred_series_id in indicators:
        print(f"Fetching {indicator_id} ({fred_series_id})...")
        
        try:
            # Fetch from FRED
            series = fred.get_series(fred_series_id, start_date, end_date)
            
            # Convert to dataframe
            df = series.reset_index()
            df.columns = ['observation_date', 'value']
            df['indicator_id'] = indicator_id
            df['source'] = 'FRED_API'
            
            all_data.append(df)
            print(f"  ✓ Fetched {len(df)} observations")
            
        except Exception as e:
            print(f"  ✗ Error fetching {indicator_id}: {e}")
    
# Combine all data
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Remove rows with NaN values
        combined_df = combined_df.dropna(subset=['value'])
        print(f"After removing NaN: {len(combined_df)} rows remain")
        
        # Insert into Snowflake
        print(f"\nInserting {len(combined_df)} total rows into FRED_INDICATORS...")
        
        # Clear existing data (for simplicity - in production you'd do incremental)
        cursor.execute("TRUNCATE TABLE FRED_INDICATORS")
        
        # Insert new data
        insert_query = """
            INSERT INTO FRED_INDICATORS 
            (indicator_id, observation_date, value, source)
            VALUES (%s, %s, %s, %s)
        """
        
        for _, row in combined_df.iterrows():
            cursor.execute(insert_query, (
                row['indicator_id'],
                row['observation_date'].date(),  # Convert to Python date
                float(row['value']),
                row['source']
            ))
        
        conn.commit()
        print("✓ Data loaded successfully!")
        
    cursor.close()
    conn.close()

if __name__ == "__main__":
    fetch_fred_indicators()