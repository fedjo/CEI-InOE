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

def load_mapping(granularity):
    path = f"{MAPPINGS_DIR}/energy_{granularity}.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)

def coerce(value, target_type):
    if value is None or value == "":
        return None
    if target_type == "float":
        # Handle comma as decimal separator
        if isinstance(value, str):
            value = value.replace(",", ".")
        return float(value)
    if target_type == "int":
        return int(value)
    if target_type == "datetime":
        return pd.to_datetime(value)
    if target_type == "date":
        return pd.to_datetime(value).date()
    return value

def process_file(path):
    fname = os.path.basename(path)
    logging.info(f"Processing {fname}")

    sha = file_sha256(path)
    file_id = str(uuid.uuid4())

    try:
        dev_id, granularity, start, end = fname.replace(".csv","").split("-")
        # Parse dates from DDMMYYYY format
        start_date = datetime.strptime(start, "%d%m%Y").date()
        end_date = datetime.strptime(end, "%d%m%Y").date()
    except Exception as e:
        logging.error(f"Invalid filename format: {e}")
        os.rename(path, f"{REJECTED_DIR}/{fname}")
        return

    mapping = load_mapping(granularity)
    df = pd.read_csv(path)

    with get_conn() as conn:
        with conn.cursor() as cur:

            # file dedupe
            cur.execute("SELECT 1 FROM ingest_file WHERE sha256=%s", (sha,))
            if cur.fetchone():
                logging.info("File already ingested")
                os.rename(path, f"{PROCESSED_DIR}/{fname}")
                return

            # register file
            cur.execute("""
                INSERT INTO ingest_file
                (file_id, file_name, device_id, granularity, start_date, end_date, sha256)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (file_id, fname, dev_id, granularity, start_date, end_date, sha))

            # resolve device
            cur.execute("SELECT id FROM device WHERE device_id=%s", (dev_id,))
            row = cur.fetchone()
            if not row:
                raise Exception(f"Unknown device token {dev_id}")
            dev_id = row[0]

            rows = []
            for _, r in df.iterrows():
                canonical = {}

                for csv_col, canonical_col in mapping["columns"].items():
                    raw_val = r.get(csv_col)
                    target_type = mapping["coercions"].get(canonical_col)
                    canonical[canonical_col] = coerce(raw_val, target_type)

                rows.append((
                    dev_id,
                    canonical["ts"],
                    canonical["energy_kwh"],
                    file_id
                ))

            # Insert into appropriate table based on granularity
            table_name = f"fact_energy_{granularity}"
            execute_batch(cur, f"""
                INSERT INTO {table_name} (device_id, ts, energy_kwh, source_file)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (device_id, ts) DO NOTHING
            """, rows, page_size=500)
            
            conn.commit()

    os.rename(path, f"{PROCESSED_DIR}/{fname}")
    logging.info(f"Finished {fname}")

def main():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(REJECTED_DIR, exist_ok=True)

    while True:
        for f in os.listdir(WATCH_DIR):
            path = f"{WATCH_DIR}/{f}"
            if f.endswith(".csv") and is_stable(path):
                try:
                    process_file(path)
                except Exception as e:
                    logging.exception(e)
                    os.rename(path, f"{REJECTED_DIR}/{f}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
