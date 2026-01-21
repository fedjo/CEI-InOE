import pandas as pd
import psycopg2
import os
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "ocei")
DB_USER = os.getenv("DB_USER", "ocei")
DB_PASS = os.getenv("DB_PASS", "ocei")

SQL_QUERY = """
SELECT
    dp.production_date AS date,
    dp.day_production_per_cow_kg,
    dp.fed_per_cow_total_kg,
    dp.feed_efficiency,
    dp.rumination_minutes,
    dp.average_lactation_days,

    -- Add PV energy when available
    SUM(CASE WHEN d.alias IS NOT NULL THEN feh.energy_kwh ELSE 0 END) AS total_device_kwh
FROM
    dairy_production dp
LEFT JOIN
    fact_energy_hourly feh ON date_trunc('day', feh.ts) = dp.production_date
LEFT JOIN
    device d ON d.id = feh.device_id
LEFT JOIN
    environmental_metrics env ON date_trunc('day', env.timestamp) = dp.production_date
WHERE
	dp.production_date  >= '2025-11-07'
GROUP BY
    dp.production_date, dp.day_production_per_cow_kg, dp.fed_per_cow_total_kg,
    dp.feed_efficiency, dp.rumination_minutes, dp.average_lactation_days,
    env.temperature, env.humidity
ORDER BY
    dp.production_date
"""

def run_regression():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Database connection successful.")

        df = pd.read_sql(SQL_QUERY, conn, parse_dates=['date'])
        # df = df.dropna()
        print(f"Total rows from query: {len(df)}")
        print(f"\nDataFrame info:")
        print(df.info())
        print(f"\nMissing values per column:")
        print(df.isnull().sum())
        print(f"\nFirst few rows:")
        print(df.head(10))
        
        # Drop rows only where milk production is null (essential column)
        df = df[df['day_production_per_cow_kg'].notna()]
        print(f"\nRows after removing null milk production: {len(df)}")
        
        # Fill missing values in other columns (or drop if too many)
        print(f"\nRows before dropna: {len(df)}")
        df = df.dropna(subset=['fed_per_cow_total_kg', 'feed_efficiency', 'rumination_minutes', 'average_lactation_days', 'total_device_kwh'])
        print(f"Rows after dropna on key features: {len(df)}")
        
        if len(df) == 0:
            print("ERROR: No valid data after cleaning. Check your SQL query or data.")
            return


        # Correlation matrix
        print("\n--- Correlation Matrix ---")
        print(df.corr())

        # Add seasonality feature
        df['day_of_year'] = df['date'].dt.dayofyear

        # Features for regression
        features = [
            'total_device_kwh',
            # 'temperature',
            # 'humidity',
            'fed_per_cow_total_kg',
            'feed_efficiency',
            'rumination_minutes',
            'average_lactation_days',
            'day_of_year'
        ]
        X = df[features]
        y = df['day_production_per_cow_kg']


        # Regression
        model = LinearRegression()
        model.fit(X, y)
        print("\n--- Regression Coefficients ---")
        for f, coef in zip(features, model.coef_):
            print(f"{f}: {coef:.4f}")

        # Forecast for next 7 days (example: using last 7 days as input)
        y_pred = model.predict(X.tail(7))
        print("\n--- Forecast for Next 7 Days (using last 7 days features) ---")
        print(y_pred)

        # Plot seasonality and trend
        plt.figure(figsize=(10,5))
        plt.plot(df['date'], y, label='Milk Production')
        plt.title('Milk Production Over Time')
        plt.xlabel('Date')
        plt.ylabel('Milk Production (kg/cow)')
        plt.legend()
        plt.show()

        plt.figure(figsize=(10,5))
        plt.scatter(df['day_of_year'], y)
        plt.title('Seasonality: Milk Production vs Day of Year')
        plt.xlabel('Day of Year')
        plt.ylabel('Milk Production (kg/cow)')
        plt.show()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    run_regression()