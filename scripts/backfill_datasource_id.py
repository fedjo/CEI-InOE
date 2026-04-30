#!/usr/bin/env python3
"""
Backfill datasource_id in fact / production / environmental tables
==================================================================
After the Phase-1 schema migration (c4d5e6f7a8b9) added the datasource_id
column to the seven final tables, this script populates that column for rows
that were ingested before the column existed.

Before each backfill pass, duplicate rows are removed so that the new
datasource-scoped unique constraint is not violated.  The "winner" within
each duplicate group is the most-recently-ingested row (MAX ingested_at,
then MAX PK as tiebreaker).  Losers are deleted.

Two dedup + backfill passes are run per table:

  Dedup-1a  Delete NULL rows whose (candidate_datasource_id, ts) already
            exists in a row that already has datasource_id set.
  Dedup-1b  Among NULL rows that would resolve to the same
            (datasource_id, ts) via the ingest_batch join, keep only
            the most-recently-ingested one.
  Pass 1    UPDATE … SET datasource_id = b.datasource_id FROM ingest_batch b

  Dedup-2a  Same as 1a but for the source_device_id-cast path.
  Dedup-2b  Same as 1b but for the source_device_id-cast path.
  Pass 2    UPDATE … SET datasource_id = source_device_id::integer

After both passes the script prints a per-table audit of unresolved rows.

Usage:
    python scripts/backfill_datasource_id.py
    python scripts/backfill_datasource_id.py --dry-run
    DB_DSN=postgresql://user:pass@host/db python scripts/backfill_datasource_id.py
"""

import argparse
import logging
import os
import sys

import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths & environment
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
)
logger = logging.getLogger('backfill_datasource_id')

# ---------------------------------------------------------------------------
# Table config: pk column and business-time column per table
# (used for deduplication tiebreaking)
# ---------------------------------------------------------------------------
TABLE_CONFIG = {
    'fact_energy_hourly':    {'pk': 'energy_id', 'ts_col': 'ts'},
    'fact_energy_daily':     {'pk': 'energy_id', 'ts_col': 'ts'},
    'environmental_metrics': {'pk': 'id',        'ts_col': 'timestamp'},
    'dairy_production':      {'pk': 'id',        'ts_col': 'production_date'},
    'fact_solar_hourly':     {'pk': 'id',        'ts_col': 'ts'},
    'fact_solar_daily':      {'pk': 'id',        'ts_col': 'ts'},
    'fact_solar_monthly':    {'pk': 'id',        'ts_col': 'ts'},
}

# ---------------------------------------------------------------------------
# SQL templates
# ---------------------------------------------------------------------------

# Dedup 1a — delete NULL rows whose (candidate datasource_id, ts) is already
# occupied by a row that already has datasource_id set.
DEDUP1A_SQL = """
    DELETE FROM {table}
    WHERE  {pk} IN (
        SELECT t.{pk}
        FROM   {table} t
        JOIN   ingest_batch b ON t.source_batch_id = b.batch_id
        WHERE  b.datasource_id IS NOT NULL
          AND  t.datasource_id IS NULL
          AND  EXISTS (
                   SELECT 1
                   FROM   {table} t2
                   WHERE  t2.datasource_id = b.datasource_id
                     AND  t2.{ts_col} = t.{ts_col}
               )
    )
"""

# Dedup 1b — among NULL rows that resolve to the same (candidate datasource_id,
# ts) via the ingest_batch join, keep only the most-recently-ingested one.
DEDUP1B_SQL = """
    DELETE FROM {table}
    WHERE  {pk} IN (
        SELECT {pk}
        FROM (
            SELECT t.{pk},
                   ROW_NUMBER() OVER (
                       PARTITION BY b.datasource_id, t.{ts_col}
                       ORDER BY t.ingested_at DESC NULLS LAST, t.{pk} DESC
                   ) AS rn
            FROM   {table} t
            JOIN   ingest_batch b ON t.source_batch_id = b.batch_id
            WHERE  b.datasource_id IS NOT NULL
              AND  t.datasource_id IS NULL
        ) ranked
        WHERE rn > 1
    )
"""

PASS1_SQL = """
    UPDATE {table} t
    SET    datasource_id = b.datasource_id
    FROM   ingest_batch b
    WHERE  t.source_batch_id = b.batch_id
      AND  b.datasource_id IS NOT NULL
      AND  t.datasource_id  IS NULL
"""

# Dedup 2a — delete NULL rows whose (candidate datasource_id from
# source_device_id cast, ts) is already occupied by a row with datasource_id set.
DEDUP2A_SQL = """
    DELETE FROM {table}
    WHERE  {pk} IN (
        SELECT t.{pk}
        FROM   {table} t
        WHERE  t.datasource_id IS NULL
          AND  t.source_device_id IS NOT NULL
          AND  t.source_device_id ~ '^[0-9]+$'
          AND  EXISTS (
                   SELECT 1
                   FROM   datasource d
                   WHERE  d.id = t.source_device_id::integer
               )
          AND  EXISTS (
                   SELECT 1
                   FROM   {table} t2
                   WHERE  t2.datasource_id = t.source_device_id::integer
                     AND  t2.{ts_col} = t.{ts_col}
               )
    )
"""

# Dedup 2b — among NULL rows that resolve to the same (source_device_id::int,
# ts), keep only the most-recently-ingested one.
DEDUP2B_SQL = """
    DELETE FROM {table}
    WHERE  {pk} IN (
        SELECT {pk}
        FROM (
            SELECT t.{pk},
                   ROW_NUMBER() OVER (
                       PARTITION BY t.source_device_id::integer, t.{ts_col}
                       ORDER BY t.ingested_at DESC NULLS LAST, t.{pk} DESC
                   ) AS rn
            FROM   {table} t
            WHERE  t.datasource_id IS NULL
              AND  t.source_device_id IS NOT NULL
              AND  t.source_device_id ~ '^[0-9]+$'
              AND  EXISTS (
                       SELECT 1
                       FROM   datasource d
                       WHERE  d.id = t.source_device_id::integer
                   )
        ) ranked
        WHERE rn > 1
    )
"""

PASS2_SQL = """
    UPDATE {table} t
    SET    datasource_id = t.source_device_id::integer
    WHERE  t.datasource_id IS NULL
      AND  t.source_device_id IS NOT NULL
      AND  t.source_device_id ~ '^[0-9]+$'
      AND  EXISTS (
               SELECT 1
               FROM   datasource d
               WHERE  d.id = t.source_device_id::integer
           )
"""

AUDIT_SQL = """
    SELECT COUNT(*) AS unresolved
    FROM   {table}
    WHERE  datasource_id IS NULL
"""

TOTAL_SQL = """
    SELECT COUNT(*) AS total
    FROM   {table}
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_connection() -> psycopg2.extensions.connection:
    dsn = os.environ.get('DB_DSN', '')
    if not dsn:
        raise ValueError(
            "DB_DSN environment variable is not set.  "
            "Example: postgresql://ocei:ocei@localhost:5432/ocei3"
        )
    return psycopg2.connect(dsn)


def run_backfill(conn, dry_run: bool = False) -> None:
    results = []

    with conn.cursor() as cur:
        for table, cfg in TABLE_CONFIG.items():
            pk = cfg['pk']
            ts_col = cfg['ts_col']
            fmt = dict(table=table, pk=pk, ts_col=ts_col)

            logger.info("Processing table: %s", table)

            # Total row count (for context)
            cur.execute(TOTAL_SQL.format(**fmt))
            total = cur.fetchone()[0]

            # --- Dedup before Pass 1 ---
            cur.execute(DEDUP1A_SQL.format(**fmt))
            d1a = cur.rowcount
            if d1a:
                logger.info("  Dedup-1a (conflict with existing): deleted %d duplicate(s)", d1a)

            cur.execute(DEDUP1B_SQL.format(**fmt))
            d1b = cur.rowcount
            if d1b:
                logger.info("  Dedup-1b (within-batch duplicates): deleted %d duplicate(s)", d1b)

            # Pass 1
            cur.execute(PASS1_SQL.format(**fmt))
            pass1_updated = cur.rowcount
            logger.info("  Pass 1 (ingest_batch join): %d row(s) updated", pass1_updated)

            # --- Dedup before Pass 2 ---
            cur.execute(DEDUP2A_SQL.format(**fmt))
            d2a = cur.rowcount
            if d2a:
                logger.info("  Dedup-2a (conflict with existing): deleted %d duplicate(s)", d2a)

            cur.execute(DEDUP2B_SQL.format(**fmt))
            d2b = cur.rowcount
            if d2b:
                logger.info("  Dedup-2b (within-cast duplicates): deleted %d duplicate(s)", d2b)

            # Pass 2
            cur.execute(PASS2_SQL.format(**fmt))
            pass2_updated = cur.rowcount
            logger.info("  Pass 2 (source_device_id cast): %d row(s) updated", pass2_updated)

            # Audit
            cur.execute(AUDIT_SQL.format(**fmt))
            unresolved = cur.fetchone()[0]

            deleted = d1a + d1b + d2a + d2b
            results.append({
                'table': table,
                'total': total,
                'deleted': deleted,
                'pass1': pass1_updated,
                'pass2': pass2_updated,
                'unresolved': unresolved,
            })

            if unresolved > 0:
                logger.warning(
                    "  %d / %d row(s) still have datasource_id = NULL",
                    unresolved,
                    total,
                )
            else:
                logger.info("  All rows resolved.")

    print("\n" + "=" * 80)
    print(f"  {'TABLE':<30}  {'TOTAL':>7}  {'DELETED':>7}  {'PASS1':>6}  {'PASS2':>6}  {'NULL':>6}")
    print("=" * 80)
    for r in results:
        print(
            f"  {r['table']:<30}  {r['total']:>7}  "
            f"{r['deleted']:>7}  {r['pass1']:>6}  {r['pass2']:>6}  {r['unresolved']:>6}"
        )
    print("=" * 80)

    if dry_run:
        logger.info("Dry-run mode — rolling back all changes.")
        conn.rollback()
    else:
        conn.commit()
        logger.info("Changes committed.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill datasource_id in fact/production/environmental tables "
            "for rows ingested before the FK column was introduced."
        )
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run all SQL but roll back instead of committing.',
    )
    args = parser.parse_args()

    if args.dry_run:
        logger.info("DRY-RUN mode enabled — no data will be persisted.")

    try:
        conn = get_connection()
    except Exception as exc:
        logger.error("Could not connect to the database: %s", exc)
        sys.exit(1)

    try:
        run_backfill(conn, dry_run=args.dry_run)
    except Exception as exc:
        logger.exception("Backfill failed, rolling back: %s", exc)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
