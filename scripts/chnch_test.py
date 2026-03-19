from fusionsolar import Client
import pandas as pd
import json

user = "k.loizouchristis"
passwd = "antreasa1"
date = pd.Timestamp('20260302', tz='Europe/Athens')


def preview(label, data):
    """Print a section header and the raw JSON response."""
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(json.dumps(data, indent=2, default=str))


with Client(user_name=user, system_code=passwd) as client:

    # ── Station list ────────────────────────────────────────────
    stations = client.get_station_list()
    preview("STATION LIST", stations)
    station_code = stations['data'][0]['stationCode']

    # ── Station KPIs at every granularity ───────────────────────
    preview("STATION KPI — REAL-TIME",
            client.get_station_kpi_real(station_code))

    preview("STATION KPI — HOURLY",
            client.get_station_kpi_hour(station_code, date))

    preview("STATION KPI — DAILY",
            client.get_station_kpi_day(station_code, date))

    preview("STATION KPI — MONTHLY",
            client.get_station_kpi_month(station_code, date))

    preview("STATION KPI — YEARLY",
            client.get_station_kpi_year(station_code, date))

    # ── Device list ─────────────────────────────────────────────
    devices = client.get_dev_list(station_code)
    preview("DEVICE LIST", devices)

    # ── Device KPIs (first device of each type) ─────────────────
    seen_types = set()
    for dev in (devices.get('data') or []):
        dev_type_id = dev['devTypeId']
        if dev_type_id in seen_types:
            continue
        seen_types.add(dev_type_id)
        dev_id = str(dev['id'])
        dev_name = dev.get('devName', dev_id)

        preview(f"DEV REAL — {dev_name} (type {dev_type_id})",
                client.get_dev_kpi_real(dev_id, dev_type_id))

        preview(f"DEV 5-MIN — {dev_name} (type {dev_type_id})",
                client.get_dev_kpi_fivemin(dev_id, dev_type_id, date))

        preview(f"DEV HOURLY — {dev_name} (type {dev_type_id})",
                client.get_dev_kpi_hour(dev_id, dev_type_id, date))

        preview(f"DEV DAILY — {dev_name} (type {dev_type_id})",
                client.get_dev_kpi_day(dev_id, dev_type_id, date))

        preview(f"DEV MONTHLY — {dev_name} (type {dev_type_id})",
                client.get_dev_kpi_month(dev_id, dev_type_id, date))

        preview(f"DEV YEARLY — {dev_name} (type {dev_type_id})",
                client.get_dev_kpi_year(dev_id, dev_type_id, date))
