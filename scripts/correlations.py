import pandas as pd
import psycopg2
import os

# --- Database Configuration (Adapt these values) ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "ocei")
DB_USER = os.getenv("DB_USER", "ocei")
DB_PASS = os.getenv("DB_PASS", "ocei")

# The SQL query we defined above (must be adapted for your schema)
SQL_QUERY = """
WITH daily_energy AS (
    SELECT
        date_trunc('day', ts) AS day_ts,
        d.alias,
        SUM(energy_kwh) AS daily_kwh
    FROM
        fact_energy_hourly AS feh
    JOIN device AS d ON d.id = feh.device_id
    WHERE d.client = 'LK Dairy Farm'
    GROUP BY
        1, 2
),
-- Pivot the energy data so each meter is its own column
pivoted_energy AS (
    SELECT
        day_ts,
        -- SUM(CASE WHEN alias = 'fan' THEN daily_kwh ELSE 0 END) AS fan_kwh_daily,
        -- SUM(CASE WHEN alias = 'fridge' THEN daily_kwh ELSE 0 END) AS fridge_kwh_daily,
        SUM(daily_kwh) AS total_kwh_daily -- Total energy for the day
    FROM
        daily_energy
    GROUP BY
        day_ts
)

-- Join the pivoted energy data with the daily production data
SELECT
    dp.production_date  AS date,
    dp.day_production_per_cow_kg ,
    dp.fed_per_cow_total_kg ,
    dp.feed_efficiency,
    dp.rumination_minutes ,
    dp.average_lactation_days ,
    -- pe.fan_kwh_daily,
    -- pe.fridge_kwh_daily,
    pe.total_kwh_daily
FROM
    dairy_production as dp
JOIN
    pivoted_energy as pe ON dp.production_date = pe.day_ts
ORDER BY
    dp.production_date
"""

def analyze_correlation():
    """Fetches data from Postgres, computes the correlation matrix, and prints results."""
    
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
        
        # 2. Calculate the Correlation Matrix
        # The .corr() method uses the Pearson correlation coefficient by default.
        correlation_matrix = df.corr()
        
        # 3. Focus on correlations involving milk production
        milk_corr = correlation_matrix['day_production_per_cow_kg'].sort_values(ascending=False)
        
        print("\n--- Correlation Results (vs. Milk Production) ---")

        print("\nCorrelation with 'day_production_per_cow_kg' (Range -1.0 to 1.0):")
        print(milk_corr)
        
        # 4. Identify Strong Correlations
        strong_correlations = milk_corr[abs(milk_corr) > 0.5]
        
        if not strong_correlations.empty:
            print("\n--- Strongest Correlates (Absolute Value > 0.5) ---")
            print("These variables are most relevant for Grafana visualization.")
            print(strong_correlations)
        else:
            print("\nNo strong correlations found (absolute value > 0.5).")
            
    except Exception as e:
        print(f"An error occurred during analysis: {e}")
        
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    analyze_correlation()