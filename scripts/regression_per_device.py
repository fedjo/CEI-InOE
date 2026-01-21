import pandas as pd
import psycopg2
import os
import numpy as np
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from datetime import timedelta
import statsmodels.api as sm

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
    d.alias AS device_alias,
    SUM(feh.energy_kwh) AS device_kwh
FROM
    dairy_production dp
LEFT JOIN
    fact_energy_hourly feh ON date_trunc('day', feh.ts) = dp.production_date
LEFT JOIN
    device d ON d.id = feh.device_id
WHERE
    dp.production_date >= '2025-11-07'
GROUP BY
    dp.production_date, dp.day_production_per_cow_kg, dp.fed_per_cow_total_kg,
    dp.feed_efficiency, dp.rumination_minutes, dp.average_lactation_days,
    d.alias
ORDER BY
    d.alias, dp.production_date
"""

# ============================================================================
# PREDICTION APPROACHES - Choose one by uncommenting in main()
# ============================================================================
def approach_1_simple_average(device_df, model, features, n_days=7):
    """
    Approach 1: Use average of last 7 days for all features.
    Assumes tomorrow will be similar to recent average.
    """
    print(f"\n--- Approach 1: Simple Average Prediction (Next {n_days} Days) ---")
    
    # Calculate averages from last 7 days
    last_7_days = device_df.tail(7)
    avg_features = {}
    for feature in features:
        avg_features[feature] = last_7_days[feature].mean()
    
    print(f"\nAverage values from last 7 days:")
    for feature, val in avg_features.items():
        print(f"  {feature}: {val:.4f}")
    
    # Create prediction dataframe (repeat same values for next 7 days)
    future_dates = [device_df['date'].max() + timedelta(days=i) for i in range(1, n_days + 1)]
    future_X = pd.DataFrame([avg_features] * n_days)
    future_predictions = model.predict(future_X)
    
    print(f"\n--- Predictions for Next {n_days} Days ---")
    for date, pred in zip(future_dates, future_predictions):
        print(f"  {date.date()}: Predicted Production = {pred:.2f} kg/cow")
    
    return future_dates, future_predictions


def approach_3_scenario_planning(device_df, model, features, n_days=7, scenarios=None):
    """
    Approach 3: Scenario Planning - Multiple what-if scenarios.
    Default scenarios: low (-20%), medium (baseline), high (+20%) device energy.
    """
    print(f"\n--- Approach 3: Scenario Planning (Next {n_days} Days) ---")
    
    if scenarios is None:
        scenarios = {
            'Low Energy': -0.20,
            'Medium Energy (Baseline)': 0.00,
            'High Energy': +0.20
        }
    
    # Use last day as baseline
    last_day = device_df.iloc[-1]
    baseline_features = {feature: last_day[feature] for feature in features}
    
    print(f"\nBaseline (Last Day) values:")
    for feature, val in baseline_features.items():
        print(f"  {feature}: {val:.4f}")
    
    future_dates = [device_df['date'].max() + timedelta(days=i) for i in range(1, n_days + 1)]
    results = {}
    
    for scenario_name, adjustment in scenarios.items():
        print(f"\n[{scenario_name}] - Device Energy Adjustment: {adjustment*100:+.0f}%")
        
        scenario_predictions = []
        for day_idx in range(n_days):
            # Adjust device_kwh for this scenario
            adjusted_features = baseline_features.copy()
            adjusted_features['device_kwh'] = adjusted_features['device_kwh'] * (1 + adjustment)
            
            # Increment day_of_year for future days
            adjusted_features['day_of_year'] = baseline_features['day_of_year'] + day_idx + 1
            
            X_scenario = pd.DataFrame([adjusted_features])
            pred = model.predict(X_scenario)[0]
            scenario_predictions.append(pred)
            print(f"  Day {day_idx + 1}: Predicted Production = {pred:.2f} kg/cow (Device: {adjusted_features['device_kwh']:.2f} kWh)")
        
        results[scenario_name] = scenario_predictions
    
    return future_dates, results


def approach_4_trend_projection(device_df, model, features, n_days=7, lookback_days=7):
    """
    Approach 4: Simple Trend - Project the trend of each feature forward.
    Calculates slope over last lookback_days and extrapolates.
    """
    print(f"\n--- Approach 4: Trend Projection (Next {n_days} Days) ---")
    
    # Use last lookback_days to calculate trend
    lookback_data = device_df.tail(lookback_days)
    trends = {}
    
    print(f"\nTrends calculated from last {lookback_days} days:")
    for feature in features:
        x_vals = np.arange(len(lookback_data))
        y_vals = lookback_data[feature].values
        slope = np.polyfit(x_vals, y_vals, 1)[0]  # Linear fit
        trends[feature] = slope
        print(f"  {feature}: {slope:.6f} per day")
    
    # Project forward
    last_day = device_df.iloc[-1]
    future_dates = [device_df['date'].max() + timedelta(days=i) for i in range(1, n_days + 1)]
    future_predictions = []
    
    print(f"\n--- Predictions for Next {n_days} Days (with trend) ---")
    for day_idx in range(1, n_days + 1):
        projected_features = {}
        for feature in features:
            projected_features[feature] = last_day[feature] + trends[feature] * day_idx
        
        X_projected = pd.DataFrame([projected_features])
        pred = model.predict(X_projected)[0]
        future_predictions.append(pred)
        print(f"  Day {day_idx}: Predicted Production = {pred:.2f} kg/cow")
    
    return future_dates, future_predictions


# ============================================================================
# MAIN ANALYSIS
# ============================================================================
def run_regression_per_device(prediction_approach='approach_1'):
    """
    prediction_approach: 'approach_1', 'approach_3', or 'approach_4'
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Database connection successful.")

        df = pd.read_sql(SQL_QUERY, conn, parse_dates=['date'])
        print(f"Total rows from query: {len(df)}")
        print(f"\nMissing values per column:")
        print(df.isnull().sum())

        # Drop rows where milk production is null
        df = df[df['day_production_per_cow_kg'].notna()]
        print(f"\nRows after removing null milk production: {len(df)}")
        
        if len(df) == 0:
            print("ERROR: No valid data after cleaning.")
            return
        
        # Get unique devices
        devices = df['device_alias'].dropna().unique()
        print(f"\nFound {len(devices)} device(s): {list(devices)}\n")
        
        # Perform regression per device
        for device in devices:
            device_df = df[df['device_alias'] == device].copy()
            device_df = device_df[device_df['device_kwh'].notna()]
            
            if len(device_df) < 3:
                print(f"\n[{device}] Insufficient data (< 3 records). Skipping.\n")
                continue

            print(f"\n{'='*60}")
            print(f"Device: {device}")
            print(f"{'='*60}")
            print(f"Records: {len(device_df)}")

            # Drop other nulls
            device_df = device_df.dropna(subset=['fed_per_cow_total_kg', 'feed_efficiency', 'rumination_minutes', 'average_lactation_days'])
            
            if len(device_df) < 3:
                print(f"Insufficient data after cleaning. Skipping.\n")
                continue
            
            # Add seasonality feature
            device_df['day_of_year'] = device_df['date'].dt.dayofyear
            device_df = device_df.sort_values('date').reset_index(drop=True)


            # Features for regression
            features = [
                'device_kwh',
                'fed_per_cow_total_kg',
                'feed_efficiency',
                'rumination_minutes',
                'average_lactation_days',
                'day_of_year'
            ]
            X = device_df[features]
            y = device_df['day_production_per_cow_kg']

            # Correlation matrix
            corr_df = device_df[features + ['day_production_per_cow_kg']].corr()
            print(f"\n--- Correlation with Milk Production ---")
            print(corr_df['day_production_per_cow_kg'].sort_values(ascending=False))
            
            # Regression
            model = LinearRegression()
            model.fit(X, y)
            r_squared = model.score(X, y)
            
            print(f"\n--- Regression Results ---")
            print(f"R-squared: {r_squared:.4f}")
            print(f"Intercept: {model.intercept_:.4f}")
            print(f"\nCoefficients:")
            for f, coef in zip(features, model.coef_):
                print(f"  {f}: {coef:.6f}")
            
            # Forecast for last 7 days (validation)
            if len(X) >= 7:
                y_pred = model.predict(X.tail(7))
                print(f"\n--- Validation: Last 7 Days ---")
                for date, actual, pred in zip(device_df['date'].tail(7), y.tail(7), y_pred):
                    print(f"  {date.date()}: Actual={actual:.2f}, Predicted={pred:.2f}")
            
            # Choose prediction approach
            if prediction_approach == 'approach_1':
                future_dates, future_predictions = approach_1_simple_average(device_df, model, features)
            elif prediction_approach == 'approach_3':
                future_dates, future_predictions = approach_3_scenario_planning(device_df, model, features)
            elif prediction_approach == 'approach_4':
                future_dates, future_predictions = approach_4_trend_projection(device_df, model, features)
            else:
                print(f"Unknown approach: {prediction_approach}")
                continue
            
            # Plot
            plt.figure(figsize=(14, 5))
            
            plt.subplot(1, 2, 1)
            plt.plot(device_df['date'], y, marker='o', label='Actual Production', linewidth=2)
            plt.plot(device_df['date'], model.predict(X), marker='x', label='Predicted Production', linewidth=2)
            
            # Plot future predictions
            if prediction_approach == 'approach_1' or prediction_approach == 'approach_4':
                plt.plot(future_dates, future_predictions, marker='s', linestyle='--', label='Future Forecast', color='green', linewidth=2)
            elif prediction_approach == 'approach_3':
                for scenario_name, predictions in future_predictions.items():
                    plt.plot(future_dates, predictions, marker='s', linestyle='--', label=f'Future - {scenario_name}', linewidth=2)
            
            plt.title(f'{device}: Milk Production Over Time ({prediction_approach})')
            plt.xlabel('Date')
            plt.ylabel('Milk Production (kg/cow)')
            plt.legend()
            plt.grid()
            
            plt.subplot(1, 2, 2)
            plt.scatter(device_df['device_kwh'], y)
            plt.title(f'{device}: Energy vs Milk Production')
            plt.xlabel('Device Energy (kWh)')
            plt.ylabel('Milk Production (kg/cow)')
            plt.grid()
            
            plt.tight_layout()
            plt.show()

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    # ========================================================================
    # CHOOSE YOUR PREDICTION APPROACH HERE
    # ========================================================================
    # run_regression_per_device(prediction_approach='approach_1')  # Simple Average
    run_regression_per_device(prediction_approach='approach_3')  # Scenario Planning
    # run_regression_per_device(prediction_approach='approach_4')  # Trend Projection