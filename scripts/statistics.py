"""
Dairy Farm Analytics - Comprehensive Statistics Extraction
Extracts baseline statistics for milk production prediction and energy optimization.
"""

import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
import os

# --- Database Configuration ---
DB_HOST = os.getenv("DB_HOST", "37.60.249.48")
DB_NAME = os.getenv("DB_NAME", "ocei3")
DB_USER = os.getenv("DB_USER", "ocei")
DB_PASS = os.getenv("DB_PASS", "ocei")
print(f"Database Config: host={DB_HOST}, db={DB_NAME}, user={DB_USER}")


def get_connection():
    """Establish database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


# =============================================================================
# 1. MILK PRODUCTION STATISTICS
# =============================================================================

SQL_MILK_DAILY = """
SELECT
    production_date,
    day_production_per_cow_kg,
    number_of_animals,
    average_lactation_days,
    fed_per_cow_total_kg,
    fed_per_cow_water_kg,
    feed_efficiency,
    rumination_minutes,
    (day_production_per_cow_kg * number_of_animals) AS total_daily_production_kg
FROM dairy_production
ORDER BY production_date
"""


def analyze_milk_production(conn):
    """Extract milk production statistics."""
    print("\n" + "="*80)
    print("1. MILK PRODUCTION STATISTICS")
    print("="*80)

    df = pd.read_sql(SQL_MILK_DAILY, conn, parse_dates=['production_date'])
    df = df.set_index('production_date')

    if df.empty:
        print("No milk production data available.")
        return None

    # Basic statistics
    print("\n--- Basic Statistics ---")
    print(df[['day_production_per_cow_kg', 'feed_efficiency', 'rumination_minutes']].describe())

    # Rolling 7-day averages
    df['production_7d_avg'] = df['day_production_per_cow_kg'].rolling(7).mean()
    df['production_7d_std'] = df['day_production_per_cow_kg'].rolling(7).std()
    df['feed_eff_7d_avg'] = df['feed_efficiency'].rolling(7).mean()

    print("\n--- Rolling 7-Day Statistics (Latest) ---")
    latest = df.tail(1)
    print(f"  Production 7-day avg: {latest['production_7d_avg'].values[0]:.2f} kg/cow")
    print(f"  Production 7-day std: {latest['production_7d_std'].values[0]:.3f} kg/cow")
    print(f"  Feed efficiency 7-day avg: {latest['feed_eff_7d_avg'].values[0]:.4f}")

    # Weekly trends (slope)
    df['week'] = df.index.isocalendar().week
    weekly = df.groupby('week').agg({
        'day_production_per_cow_kg': ['mean', 'std'],
        'feed_efficiency': 'mean',
        'rumination_minutes': 'mean',
        'total_daily_production_kg': 'sum'
    }).round(3)

    print("\n--- Weekly Aggregates ---")
    print(weekly.tail(8))

    # Lactation day bands analysis
    df['lactation_band'] = pd.cut(
        df['average_lactation_days'],
        bins=[0, 50, 100, 150, 200, 250, 300, 400],
        labels=['0-50', '51-100', '101-150', '151-200', '201-250', '251-300', '300+']
    )

    lactation_impact = df.groupby('lactation_band').agg({
        'day_production_per_cow_kg': ['mean', 'std', 'count']
    }).round(2)

    print("\n--- Production by Lactation Day Band ---")
    print(lactation_impact)

    # Day of week patterns
    df['day_of_week'] = df.index.dayofweek
    dow_pattern = df.groupby('day_of_week').agg({
        'day_production_per_cow_kg': ['mean', 'std']
    }).round(3)
    dow_pattern.index = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    print("\n--- Day of Week Patterns ---")
    print(dow_pattern)

    # Rumination correlation with next-day production
    df['next_day_production'] = df['day_production_per_cow_kg'].shift(-1)
    rumination_corr = df['rumination_minutes'].corr(df['next_day_production'])
    print(f"\n--- Leading Indicators ---")
    print(f"  Rumination → Next-day production correlation: {rumination_corr:.3f}")

    return df


# =============================================================================
# 2. ENERGY CONSUMPTION STATISTICS
# =============================================================================

SQL_ENERGY_HOURLY = """
SELECT
    d.alias AS device,
    d.device_id,
    feh.ts,
    EXTRACT(hour FROM feh.ts) AS hour,
    EXTRACT(dow FROM feh.ts) AS day_of_week,
    DATE(feh.ts) AS date,
    feh.energy_kwh
FROM fact_energy_hourly feh
JOIN generic_device d ON d.id = feh.device_id
ORDER BY d.alias, feh.ts
"""

SQL_ENERGY_DAILY = """
SELECT
    d.alias AS device,
    d.device_id,
    fed.ts AS date,
    fed.energy_kwh
FROM fact_energy_daily fed
JOIN generic_device d ON d.id = fed.device_id
ORDER BY d.alias, fed.ts
"""


def analyze_energy_consumption(conn):
    """Extract energy consumption statistics."""
    print("\n" + "="*80)
    print("2. ENERGY CONSUMPTION STATISTICS")
    print("="*80)

    # Hourly data for peak analysis
    df_hourly = pd.read_sql(SQL_ENERGY_HOURLY, conn, parse_dates=['ts', 'date'])

    # Daily data for trends
    df_daily = pd.read_sql(SQL_ENERGY_DAILY, conn, parse_dates=['date'])

    if df_hourly.empty:
        print("No energy data available.")
        return None, None

    devices = df_hourly['device'].dropna().unique()
    print(f"\n--- Found {len(devices)} device(s) ---")

    # Per-device analysis
    device_stats = []
    for device in devices:
        dev_hourly = df_hourly[df_hourly['device'] == device]
        dev_daily = df_daily[df_daily['device'] == device]

        # Daily totals
        daily_totals = dev_hourly.groupby('date')['energy_kwh'].sum()

        stats = {
            'device': device,
            'total_kwh': dev_hourly['energy_kwh'].sum(),
            'daily_avg_kwh': daily_totals.mean(),
            'daily_max_kwh': daily_totals.max(),
            'daily_std_kwh': daily_totals.std(),
            'hourly_peak_kwh': dev_hourly['energy_kwh'].max(),
            'peak_hour': dev_hourly.groupby('hour')['energy_kwh'].mean().idxmax(),
            'days_recorded': len(daily_totals)
        }
        device_stats.append(stats)

        # Hourly profile
        hourly_profile = dev_hourly.groupby('hour')['energy_kwh'].agg(['mean', 'max']).round(3)

        print(f"\n--- Device: {device} ---")
        print(f"  Total kWh: {stats['total_kwh']:.2f}")
        print(f"  Daily avg: {stats['daily_avg_kwh']:.2f} kWh")
        print(f"  Daily max: {stats['daily_max_kwh']:.2f} kWh")
        print(f"  Peak hour: {int(stats['peak_hour'])}:00 ({dev_hourly.groupby('hour')['energy_kwh'].mean().max():.3f} kWh avg)")

        # Weekend vs weekday
        dev_hourly['is_weekend'] = dev_hourly['day_of_week'].isin([0, 6])  # Sunday=0, Saturday=6
        weekend_avg = dev_hourly[dev_hourly['is_weekend']].groupby('date')['energy_kwh'].sum().mean()
        weekday_avg = dev_hourly[~dev_hourly['is_weekend']].groupby('date')['energy_kwh'].sum().mean()

        print(f"  Weekday avg: {weekday_avg:.2f} kWh | Weekend avg: {weekend_avg:.2f} kWh")

    # Summary table
    device_summary = pd.DataFrame(device_stats)
    print("\n--- Device Summary ---")
    print(device_summary.to_string(index=False))

    # Rolling 7-day consumption per device
    print("\n--- 7-Day Rolling Consumption (Latest per Device) ---")
    for device in devices:
        dev_daily = df_daily[df_daily['device'] == device].set_index('date').sort_index()
        dev_daily['rolling_7d'] = dev_daily['energy_kwh'].rolling(7).sum()
        if not dev_daily.empty:
            latest = dev_daily.tail(1)
            print(f"  {device}: {latest['rolling_7d'].values[0]:.2f} kWh (7-day)")

    return df_hourly, df_daily, device_summary


# =============================================================================
# 3. ENVIRONMENTAL STATISTICS
# =============================================================================

SQL_ENVIRONMENTAL = """
SELECT
    DATE(timestamp) AS date,
    AVG(temperature) AS avg_temp,
    MIN(temperature) AS min_temp,
    MAX(temperature) AS max_temp,
    AVG(humidity) AS avg_humidity,
    AVG(wind_speed) AS avg_wind_speed,
    MAX(wind_speed) AS max_wind_speed,
    AVG(noise_level_db) AS avg_noise,
    AVG(pm10) AS avg_pm10,
    COUNT(*) AS readings
FROM environmental_metrics
GROUP BY DATE(timestamp)
ORDER BY date
"""


def calculate_thi(temperature, humidity):
    """Calculate Temperature-Humidity Index for dairy cattle."""
    # THI = 0.8 * T + RH * (T - 14.4) / 100 + 46.4
    return 0.8 * temperature + humidity * (temperature - 14.4) / 100 + 46.4


def analyze_environmental(conn):
    """Extract environmental statistics."""
    print("\n" + "="*80)
    print("3. ENVIRONMENTAL STATISTICS")
    print("="*80)

    df = pd.read_sql(SQL_ENVIRONMENTAL, conn, parse_dates=['date'])

    if df.empty:
        print("No environmental data available.")
        return None

    df = df.set_index('date')

    # Calculate THI
    df['THI'] = calculate_thi(df['avg_temp'], df['avg_humidity'])
    df['temp_range'] = df['max_temp'] - df['min_temp']

    # Basic statistics
    print("\n--- Basic Environmental Statistics ---")
    print(df[['avg_temp', 'avg_humidity', 'THI', 'temp_range', 'avg_wind_speed']].describe().round(2))

    # THI categories
    df['THI_category'] = pd.cut(
        df['THI'],
        bins=[0, 68, 72, 80, 90, 100],
        labels=['No stress', 'Mild stress', 'Moderate stress', 'Severe stress', 'Emergency']
    )

    thi_distribution = df['THI_category'].value_counts().sort_index()
    print("\n--- THI Stress Distribution ---")
    print(thi_distribution)

    # Days above THI threshold
    days_stressed = (df['THI'] > 72).sum()
    total_days = len(df)
    print(f"\n  Days with THI > 72 (heat stress): {days_stressed}/{total_days} ({100*days_stressed/total_days:.1f}%)")

    # Rolling averages for forecasting baseline
    df['temp_7d_avg'] = df['avg_temp'].rolling(7).mean()
    df['THI_7d_avg'] = df['THI'].rolling(7).mean()

    print("\n--- Latest 7-Day Environmental Averages ---")
    latest = df.tail(1)
    print(f"  Temperature: {latest['temp_7d_avg'].values[0]:.1f}°C")
    print(f"  THI: {latest['THI_7d_avg'].values[0]:.1f}")

    return df


# =============================================================================
# 4. CROSS-DOMAIN ANALYSIS
# =============================================================================

SQL_COMBINED = """
WITH daily_energy_by_device AS (
    SELECT
        DATE(feh.ts) AS date,
        d.alias AS device,
        SUM(feh.energy_kwh) AS daily_kwh
    FROM fact_energy_hourly feh
    JOIN generic_device d ON d.id = feh.device_id
    GROUP BY DATE(feh.ts), d.alias
),
daily_energy_total AS (
    SELECT
        date,
        SUM(daily_kwh) AS total_kwh
    FROM daily_energy_by_device
    GROUP BY date
),
env_daily AS (
    SELECT
        DATE(timestamp) AS date,
        AVG(temperature) AS avg_temp,
        AVG(humidity) AS avg_humidity,
        AVG(wind_speed) AS avg_wind_speed
    FROM environmental_metrics
    GROUP BY DATE(timestamp)
)

SELECT
    dp.production_date AS date,
    dp.day_production_per_cow_kg,
    dp.number_of_animals,
    (dp.day_production_per_cow_kg * dp.number_of_animals) AS total_production_kg,
    dp.feed_efficiency,
    dp.fed_per_cow_total_kg,
    dp.rumination_minutes,
    dp.average_lactation_days,
    det.total_kwh AS total_energy_kwh,
    env.avg_temp,
    env.avg_humidity,
    env.avg_wind_speed
FROM dairy_production dp
LEFT JOIN daily_energy_total det ON dp.production_date = det.date
LEFT JOIN env_daily env ON dp.production_date = env.date
ORDER BY dp.production_date
"""


def analyze_cross_domain(conn):
    """Cross-domain analysis: production, energy, and environment."""
    print("\n" + "="*80)
    print("4. CROSS-DOMAIN ANALYSIS")
    print("="*80)

    df = pd.read_sql(SQL_COMBINED, conn, parse_dates=['date'])

    if df.empty:
        print("No combined data available.")
        return None

    df = df.set_index('date')

    # Calculate derived metrics
    df['THI'] = calculate_thi(df['avg_temp'], df['avg_humidity'])
    df['kwh_per_kg_milk'] = df['total_energy_kwh'] / df['total_production_kg']
    df['kwh_per_cow'] = df['total_energy_kwh'] / df['number_of_animals']

    # Drop rows with missing critical data
    df_clean = df.dropna(subset=['day_production_per_cow_kg', 'total_energy_kwh'])

    print(f"\n--- Data Coverage ---")
    print(f"  Total production days: {len(df)}")
    print(f"  Days with complete data (production + energy): {len(df_clean)}")

    # Efficiency metrics
    print("\n--- Energy Efficiency Metrics ---")
    if not df_clean.empty:
        print(f"  Average kWh per kg milk: {df_clean['kwh_per_kg_milk'].mean():.4f}")
        print(f"  Min kWh per kg milk: {df_clean['kwh_per_kg_milk'].min():.4f}")
        print(f"  Max kWh per kg milk: {df_clean['kwh_per_kg_milk'].max():.4f}")
        print(f"  Std dev: {df_clean['kwh_per_kg_milk'].std():.4f}")
    
    # Correlation matrix
    correlation_cols = [
        'day_production_per_cow_kg',
        'total_energy_kwh',
        'kwh_per_kg_milk',
        'feed_efficiency',
        'rumination_minutes',
        'average_lactation_days',
        'avg_temp',
        'avg_humidity',
        'THI'
    ]

    available_cols = [c for c in correlation_cols if c in df_clean.columns]
    corr_matrix = df_clean[available_cols].corr().round(3)

    print("\n--- Correlation Matrix ---")
    print(corr_matrix)

    # Key correlations
    print("\n--- Key Correlations with Milk Production ---")
    if 'day_production_per_cow_kg' in corr_matrix.columns:
        milk_corr = corr_matrix['day_production_per_cow_kg'].drop('day_production_per_cow_kg').sort_values(ascending=False)
        for col, val in milk_corr.items():
            significance = "**" if abs(val) > 0.5 else "*" if abs(val) > 0.3 else ""
            print(f"  {col}: {val:+.3f} {significance}")
    
    print("\n--- Key Correlations with Energy Consumption ---")
    if 'total_energy_kwh' in corr_matrix.columns:
        energy_corr = corr_matrix['total_energy_kwh'].drop('total_energy_kwh').sort_values(ascending=False)
        for col, val in energy_corr.items():
            significance = "**" if abs(val) > 0.5 else "*" if abs(val) > 0.3 else ""
            print(f"  {col}: {val:+.3f} {significance}")

    # THI impact analysis
    print("\n--- Production by THI Band ---")
    if 'THI' in df_clean.columns:
        df_clean['THI_band'] = pd.cut(
            df_clean['THI'],
            bins=[0, 68, 72, 80, 100],
            labels=['<68 (no stress)', '68-72 (mild)', '72-80 (moderate)', '>80 (severe)']
        )
        thi_impact = df_clean.groupby('THI_band').agg({
            'day_production_per_cow_kg': ['mean', 'std', 'count'],
            'total_energy_kwh': ['mean', 'std']
        }).round(2)
        print(thi_impact)

    # Lagged correlations
    print("\n--- Lagged Correlations (Weather → Next-Day Production) ---")
    df_clean['next_day_production'] = df_clean['day_production_per_cow_kg'].shift(-1)

    for col in ['avg_temp', 'THI', 'avg_humidity', 'total_energy_kwh']:
        if col in df_clean.columns:
            lag_corr = df_clean[col].corr(df_clean['next_day_production'])
            print(f"  {col} → next-day production: {lag_corr:+.3f}")

    return df, df_clean


# =============================================================================
# 5. DEVICE-SPECIFIC CORRELATIONS WITH ENVIRONMENT
# =============================================================================

SQL_DEVICE_ENV = """
WITH daily_energy_by_device AS (
    SELECT
        DATE(feh.ts) AS date,
        d.alias AS device,
        SUM(feh.energy_kwh) AS daily_kwh
    FROM fact_energy_hourly feh
    JOIN generic_device d ON d.id = feh.device_id
    GROUP BY DATE(feh.ts), d.alias
),
env_daily AS (
    SELECT
        DATE(timestamp) AS date,
        AVG(temperature) AS avg_temp,
        AVG(humidity) AS avg_humidity,
        AVG(wind_speed) AS avg_wind_speed
    FROM environmental_metrics
    GROUP BY DATE(timestamp)
)

SELECT
    de.date,
    de.device,
    de.daily_kwh,
    env.avg_temp,
    env.avg_humidity,
    env.avg_wind_speed,
    (0.8 * env.avg_temp + env.avg_humidity * (env.avg_temp - 14.4) / 100 + 46.4) AS THI
FROM daily_energy_by_device de
LEFT JOIN env_daily env ON de.date = env.date
ORDER BY de.device, de.date
"""


def analyze_device_environment_correlation(conn):
    """Analyze correlation between device energy and environmental conditions."""
    print("\n" + "="*80)
    print("5. DEVICE-ENVIRONMENT CORRELATIONS")
    print("="*80)

    df = pd.read_sql(SQL_DEVICE_ENV, conn, parse_dates=['date'])

    if df.empty:
        print("No device-environment data available.")
        return None

    devices = df['device'].dropna().unique()

    print("\n--- Device Energy vs Environmental Factors ---")
    print("(Identify which devices are temperature-sensitive - likely fans, cooling)")

    device_corr_results = []
    for device in devices:
        dev_df = df[df['device'] == device].dropna()

        if len(dev_df) < 5:
            continue

        temp_corr = dev_df['daily_kwh'].corr(dev_df['avg_temp'])
        thi_corr = dev_df['daily_kwh'].corr(dev_df['thi'])
        wind_corr = dev_df['daily_kwh'].corr(dev_df['avg_wind_speed'])

        device_corr_results.append({
            'device': device,
            'temp_corr': temp_corr,
            'THI_corr': thi_corr,
            'wind_corr': wind_corr,
            'data_points': len(dev_df)
        })

        # Flag temperature-sensitive devices
        temp_flag = "🔥 TEMP-SENSITIVE" if temp_corr > 0.4 else ""
        print(f"\n  {device} {temp_flag}")
        print(f"    Temperature corr: {temp_corr:+.3f}")
        print(f"    THI corr: {thi_corr:+.3f}")
        print(f"    Wind speed corr: {wind_corr:+.3f}")

    return pd.DataFrame(device_corr_results)


# =============================================================================
# 6. FEATURE ENGINEERING FOR PREDICTION
# =============================================================================

def create_prediction_features(conn):
    """Create lag features and rolling statistics for ML prediction."""
    print("\n" + "="*80)
    print("6. PREDICTION FEATURE SUMMARY")
    print("="*80)

    df = pd.read_sql(SQL_COMBINED, conn, parse_dates=['date'])
    
    if df.empty:
        print("No data available for feature engineering.")
        return None
    
    df = df.set_index('date').sort_index()
    
    # Calculate derived metrics
    df['THI'] = calculate_thi(df['avg_temp'], df['avg_humidity'])
    df['total_production_kg'] = df['day_production_per_cow_kg'] * df['number_of_animals']

    # Temporal features
    df['day_of_week'] = df.index.dayofweek
    df['day_of_year'] = df.index.dayofyear
    df['week_of_year'] = df.index.isocalendar().week
    df['month'] = df.index.month
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    # Lag features (previous days)
    for lag in [1, 2, 3, 7]:
        df[f'production_lag_{lag}d'] = df['day_production_per_cow_kg'].shift(lag)
        df[f'energy_lag_{lag}d'] = df['total_energy_kwh'].shift(lag)
        df[f'THI_lag_{lag}d'] = df['THI'].shift(lag)

    # Rolling statistics
    for window in [3, 7, 14]:
        df[f'production_roll_{window}d_mean'] = df['day_production_per_cow_kg'].rolling(window).mean()
        df[f'production_roll_{window}d_std'] = df['day_production_per_cow_kg'].rolling(window).std()
        df[f'energy_roll_{window}d_mean'] = df['total_energy_kwh'].rolling(window).mean()
        df[f'THI_roll_{window}d_mean'] = df['THI'].rolling(window).mean()
    
    # Trend features
    df['production_diff_1d'] = df['day_production_per_cow_kg'].diff(1)
    df['production_diff_7d'] = df['day_production_per_cow_kg'].diff(7)
    df['energy_diff_1d'] = df['total_energy_kwh'].diff(1)
    
    # Target variables (what we want to predict)
    df['production_next_day'] = df['day_production_per_cow_kg'].shift(-1)
    df['production_next_week_avg'] = df['day_production_per_cow_kg'].shift(-1).rolling(7).mean().shift(-6)
    df['energy_next_day'] = df['total_energy_kwh'].shift(-1)
    df['energy_next_week_sum'] = df['total_energy_kwh'].shift(-1).rolling(7).sum().shift(-6)
    
    print("\n--- Available Prediction Features ---")
    feature_groups = {
        'Temporal': ['day_of_week', 'day_of_year', 'week_of_year', 'month', 'is_weekend'],
        'Production Lags': [c for c in df.columns if 'production_lag' in c],
        'Production Rolling': [c for c in df.columns if 'production_roll' in c],
        'Energy Lags': [c for c in df.columns if 'energy_lag' in c],
        'Energy Rolling': [c for c in df.columns if 'energy_roll' in c],
        'Environmental': ['avg_temp', 'avg_humidity', 'THI', 'avg_wind_speed'],
        'Environmental Lags': [c for c in df.columns if 'THI_lag' in c or 'THI_roll' in c],
        'Dairy Metrics': ['feed_efficiency', 'rumination_minutes', 'average_lactation_days'],
        'Trends': ['production_diff_1d', 'production_diff_7d', 'energy_diff_1d']
    }
    
    for group, features in feature_groups.items():
        available = [f for f in features if f in df.columns]
        print(f"\n  {group}:")
        for f in available:
            print(f"    - {f}")
    
    print("\n--- Target Variables ---")
    print("  - production_next_day (next-day milk production)")
    print("  - production_next_week_avg (7-day forward average)")
    print("  - energy_next_day (next-day energy consumption)")
    print("  - energy_next_week_sum (7-day forward total energy)")
    
    # Data completeness
    df_clean = df.dropna()
    print(f"\n--- Data Completeness ---")
    print(f"  Total rows: {len(df)}")
    print(f"  Complete rows (all features): {len(df_clean)}")
    
    return df


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Run all analyses."""
    print("="*80)
    print("DAIRY FARM ANALYTICS - COMPREHENSIVE STATISTICS")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    try:
        conn = get_connection()
        print("✓ Database connection successful")
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return
    
    try:
        # Run all analyses
        milk_df = analyze_milk_production(conn)
        energy_hourly, energy_daily, device_summary = analyze_energy_consumption(conn)
        env_df = analyze_environmental(conn)
        combined_df, combined_clean = analyze_cross_domain(conn)
        device_env_corr = analyze_device_environment_correlation(conn)
        prediction_df = create_prediction_features(conn)
        
        # Summary recommendations
        print("\n" + "="*80)
        print("RECOMMENDATIONS FOR NEXT STEPS")
        print("="*80)
        
        print("""
1. OPTIMIZE FAN/COOLING DEVICES:
   - Identify devices with high temperature correlation (THI_corr > 0.4)
   - Schedule pre-cooling before peak THI hours
   - Use wind speed as natural ventilation indicator

2. MILK PRODUCTION PREDICTION MODEL:
   - Use rolling 7-day averages as baseline
   - Include THI lag features (weather affects production with delay)
   - Account for lactation day progression
   - Add day-of-week effects

3. ENERGY PREDICTION MODEL:
   - Separate base load from temperature-driven consumption
   - Model each device category (fans, milking, cooling) separately
   - Include weather forecast as future feature

4. OPTIMIZATION OPPORTUNITIES:
   - Target days with THI > 72 for proactive cooling
   - Shift energy-intensive operations to off-peak hours
   - Monitor kWh/kg milk ratio for efficiency tracking
        """)
        
    finally:
        conn.close()
        print("\n✓ Database connection closed")


if __name__ == "__main__":
    main()