import os, time, uuid, hashlib, logging
import pandas as pd
import psycopg2
import yaml
from psycopg2.extras import execute_batch
from datetime import datetime

WATCH_DIR = "/data/incoming"
PROCESSED_DIR = "/data/processed"
REJECTED_DIR = "/data/rejected"
MAPPINGS_DIR = "/app/mappings"

POLL_INTERVAL = 10
STABLE_SECONDS = 3

logging.basicConfig(level=logging.INFO)

def get_conn():
    return psycopg2.connect(os.environ["DB_DSN"])

def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def is_stable(path):
    s1 = os.path.getsize(path)
    time.sleep(STABLE_SECONDS)
    return s1 == os.path.getsize(path)

def load_mapping(dataset_type):
    path = f"{MAPPINGS_DIR}/{dataset_type}.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)

def coerce(value, target_type):
    if value is None or pd.isna(value) or value == "":
        return None
    if target_type == "float":
        if isinstance(value, str):
            value = value.replace(",", ".")
        return float(value)
    if target_type == "int":
        if isinstance(value, str):
            value = value.strip()
        return int(float(value))
    if target_type == "datetime":
        return pd.to_datetime(value)
    if target_type == "date":
        return pd.to_datetime(value).date()
    if target_type == "string":
        return str(value).strip()
    return value

def generate_device_id(location, farm_name="Lelyna"):
    """Generate device_id from location and farm name"""
    base = f"{farm_name}-{location}-{uuid.uuid4()}".replace(" ", "-").lower()
    return base

def detect_dataset_type(df, filename):
    """Detect if this is dairy production or energy data"""
    dairy_cols = {"Date", "Day production/cow (kg)", "Nr. animals", "Feed efficiency"}

    df_cols = set(df.columns)
    
    if dairy_cols.issubset(df_cols):
        return "dairy_production_daily"
    elif "Date and Time" in df_cols:
        if "Hourly" in df.columns:
            return "energy_hourly"
        elif "Daily" in df.columns:
            return "energy_daily"
        else:
            raise Exception(f"Unknown energy data format. Columns: {df_cols}")
    else:
        raise Exception(f"Unknown data format. Columns: {df_cols}")

def parse_csv_filename(fname):
    """Parse device_id from CSV filename format: device_id-dataset_type-startdate-enddate.csv"""
    parts = fname.replace(".csv","").split("-")
    if len(parts) < 4:
        raise Exception(f"Invalid CSV filename format: {fname}")
    
    dev_id = parts[0]
    start = parts[-2]
    end = parts[-1]
    start_date = datetime.strptime(start, "%d%m%Y").date()
    end_date = datetime.strptime(end, "%d%m%Y").date()
    
    return dev_id, start_date, end_date

def process_file(path):
    fname = os.path.basename(path)
    logging.info(f"Processing file: {fname}")

    sha = file_sha256(path)
    file_id = str(uuid.uuid4())

    try:
        # Determine file type and read
        if fname.endswith('.xlsx'):
            df = pd.read_excel(path)
            # Extract device_id from Location column for Excel
            location = df.iloc[0].get("Location", "unknown") if len(df) > 0 else "unknown"
            device_id = generate_device_id(location)
            
            # Extract date range from data
            date_col = "Date"
            if date_col in df.columns:
                dates = pd.to_datetime(df[date_col], errors='coerce').dropna()
                start_date = dates.min().date()
                end_date = dates.max().date()
            else:
                start_date = None
                end_date = None

        elif fname.endswith('.csv'):
            df = pd.read_csv(path)
            # Parse device_id from CSV filename
            device_id, start_date, end_date = parse_csv_filename(fname)
            
        else:
            raise Exception(f"Unsupported file format: {fname}")
        
        dataset_type = detect_dataset_type(df, fname)
        logging.info(f"Detected dataset type: {dataset_type}")
        
        mapping = load_mapping(dataset_type)
        logging.info(f"Generated/parsed device_id: {device_id}")

    except Exception as e:
        logging.error(f"Failed to read file {fname}: {e}")
        os.rename(path, f"{REJECTED_DIR}/{fname}")
        return

    print(device_id, start_date, end_date, dataset_type)
    with get_conn() as conn:
        with conn.cursor() as cur:

            # File deduplication
            cur.execute("SELECT 1 FROM ingest_file WHERE sha256=%s", (sha,))
            if cur.fetchone():
                logging.info("File already ingested")
                os.rename(path, f"{PROCESSED_DIR}/{fname}")
                return

            # Register file with device_id in ingest_file table
            cur.execute("""
                INSERT INTO ingest_file
                (file_id, file_name, device_id, granularity, start_date, end_date, sha256)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (file_id, fname, device_id, mapping["granularity"], start_date, end_date, sha))

            # Get device.id (numeric primary key)
            cur.execute("SELECT id FROM device WHERE device_id=%s", (device_id,))
            result = cur.fetchone()
            if result:
                device_pk_id = result[0]
                logging.info(f"Found device with id: {device_pk_id}")
            else:
                logging.warning(f"Device {device_id} not found in device table")
                device_pk_id = None

            rows = []

            for idx, r in df.iterrows():
                canonical = {}

                for csv_col, canonical_col in mapping["columns"].items():
                    raw_val = r.get(csv_col)
                    target_type = mapping["coercions"].get(canonical_col)
                    try:
                        canonical[canonical_col] = coerce(raw_val, target_type)
                    except Exception as e:
                        logging.warning(f"Failed to coerce {csv_col}={raw_val} to {target_type}: {e}")
                        canonical[canonical_col] = None

                # Skip rows based on dataset type
                if dataset_type == "dairy_production_daily":
                    if not canonical.get("production_date"):
                        continue
                    rows.append((
                        canonical.get("production_date"),
                        canonical.get("day_production_per_cow_kg"),
                        canonical.get("number_of_animals"),
                        canonical.get("average_lactation_days"),
                        canonical.get("fed_per_cow_total_kg"),
                        canonical.get("fed_per_cow_water_kg"),
                        canonical.get("feed_efficiency"),
                        canonical.get("rumination_minutes"),
                        file_id
                    ))
                elif dataset_type == "energy_hourly" or dataset_type == "energy_daily":
                    if not canonical.get("ts"):
                        continue
                    rows.append((
                        device_pk_id,
                        canonical.get("ts"),
                        canonical.get("energy_kwh"),
                        file_id
                    ))

            # Insert into appropriate table
            if dataset_type == "dairy_production_daily" and rows:
                execute_batch(cur, """
                    INSERT INTO dairy_production 
                    (production_date, day_production_per_cow_kg, 
                     number_of_animals, average_lactation_days, fed_per_cow_total_kg,
                     fed_per_cow_water_kg, feed_efficiency, rumination_minutes, source_file)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (production_date) DO UPDATE SET
                        day_production_per_cow_kg = EXCLUDED.day_production_per_cow_kg,
                        number_of_animals = EXCLUDED.number_of_animals,
                        average_lactation_days = EXCLUDED.average_lactation_days,
                        fed_per_cow_total_kg = EXCLUDED.fed_per_cow_total_kg,
                        fed_per_cow_water_kg = EXCLUDED.fed_per_cow_water_kg,
                        feed_efficiency = EXCLUDED.feed_efficiency,
                        rumination_minutes = EXCLUDED.rumination_minutes
                """, rows, page_size=500)
                
            elif dataset_type == "energy_hourly" and rows:
                execute_batch(cur, """
                    INSERT INTO fact_energy_hourly (device_id, ts, energy_kwh, source_file)
                    VALUES (%s,%s,%s,%s)
                """, rows, page_size=500)
                
            elif dataset_type == "energy_daily" and rows:
                execute_batch(cur, """
                    INSERT INTO fact_energy_daily (device_id, ts, energy_kwh, source_file)
                    VALUES (%s,%s,%s,%s)
                """, rows, page_size=500)

            conn.commit()
            logging.info(f"Inserted {len(rows)} rows from {fname}")

    os.rename(path, f"{PROCESSED_DIR}/{fname}")
    logging.info(f"Finished processing {fname}")

def main():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(REJECTED_DIR, exist_ok=True)

    while True:
        for f in os.listdir(WATCH_DIR):
            path = f"{WATCH_DIR}/{f}"
            if (f.endswith(".xlsx") or f.endswith(".csv")) and is_stable(path):
                try:
                    process_file(path)
                except Exception as e:
                    logging.exception(f"Error processing {f}: {e}")
                    os.rename(path, f"{REJECTED_DIR}/{f}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
