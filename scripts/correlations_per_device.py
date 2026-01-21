import pandas as pd
import psycopg2
import os

# --- Database Configuration (Adapt these values) ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "ocei")
DB_USER = os.getenv("DB_USER", "ocei")
DB_PASS = os.getenv("DB_PASS", "ocei")

# SQL query to get daily production and daily energy per device
SQL_QUERY = """
WITH daily_energy AS (
    SELECT
        date_trunc('day', ts) AS day_ts,
        d.alias AS device_alias,
        SUM(energy_kwh) AS daily_kwh
    FROM
        fact_energy_hourly AS feh
    JOIN device AS d ON d.id = feh.device_id
    WHERE d.client = 'LK Dairy Farm'
    GROUP BY
        1, 2
),
daily_production AS (
    SELECT
        production_date,
        day_production_per_cow_kg,
        fed_per_cow_total_kg,
        feed_efficiency,
        rumination_minutes,
        average_lactation_days
    FROM
        dairy_production
    WHERE
        production_date >= CURRENT_DATE - INTERVAL '180 days'
)

SELECT
    dp.production_date AS date,
    de.device_alias,
    dp.day_production_per_cow_kg,
    dp.fed_per_cow_total_kg,
    dp.feed_efficiency,
    dp.rumination_minutes,
    dp.average_lactation_days,
    de.daily_kwh
FROM
    daily_production AS dp
LEFT JOIN
    daily_energy AS de ON dp.production_date = de.day_ts
ORDER BY
    de.device_alias,
    dp.production_date
"""

def analyze_correlations_per_device():
    """Fetches data from Postgres, computes correlations per device, and prints results."""
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Database connection successful.")
        
        # 1. Fetch data into a Pandas DataFrame
        df = pd.read_sql(SQL_QUERY, conn, index_col='date')
        
        # 2. Get list of unique devices
        devices = df['device_alias'].dropna().unique()
        
        print("\n--- Correlation Analysis Per Device ---")
        print(f"Found {len(devices)} device(s): {', '.join(devices)}\n")
        
        # 3. Calculate correlations for each device
        for device in devices:
            device_df = df[df['device_alias'] == device].drop(columns=['device_alias'])
            
            # Skip if not enough data
            if len(device_df) < 3:
                print(f"\n[{device}] Insufficient data (< 3 records). Skipping.")
                continue
            
            # Calculate correlation matrix
            correlation_matrix = device_df.corr()
            
            # Focus on correlations with milk production
            milk_corr = correlation_matrix['day_production_per_cow_kg'].sort_values(ascending=False)
            
            print(f"\n--- Device: {device} ---")
            print(f"Records: {len(device_df)}")
            print(f"\nCorrelation with 'day_production_per_cow_kg':")
            print(milk_corr)
            
            # Identify strong correlations
            strong_correlations = milk_corr[abs(milk_corr) > 0.5]
            
            if not strong_correlations.empty:
                print(f"\nStrong correlates (|r| > 0.5):")
                print(strong_correlations)
            else:
                print("\nNo strong correlations found (|r| > 0.5).")
            
    except Exception as e:
        print(f"An error occurred during analysis: {e}")
        
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    analyze_correlations_per_device()