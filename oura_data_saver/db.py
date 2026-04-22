from __future__ import annotations

import json
import urllib.parse

import pg8000.native


def connect(database_url: str) -> pg8000.native.Connection:
    p = urllib.parse.urlparse(database_url)
    return pg8000.native.Connection(
        user=p.username or "postgres",
        password=p.password or "",
        host=p.hostname or "localhost",
        port=p.port or 5432,
        database=p.path.lstrip("/") or "postgres",
    )


# ---------------------------------------------------------------------------
# Schema: every table stores only (data JSONB), all other columns are virtual
# generated columns computed from the JSONB on the fly (PG 18+).
# ---------------------------------------------------------------------------

_DOCUMENT_TABLES: dict[str, list[str]] = {
    "personal_info": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "age INTEGER GENERATED ALWAYS AS ((data->>'age')::integer) VIRTUAL",
        "weight REAL GENERATED ALWAYS AS ((data->>'weight')::real) VIRTUAL",
        "height REAL GENERATED ALWAYS AS ((data->>'height')::real) VIRTUAL",
        "biological_sex TEXT GENERATED ALWAYS AS (data->>'biological_sex') VIRTUAL",
        "email TEXT GENERATED ALWAYS AS (data->>'email') VIRTUAL",
    ],
    "tag": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "text TEXT GENERATED ALWAYS AS (data->>'text') VIRTUAL",
        "timestamp TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'timestamp')::timestamptz) VIRTUAL",
    ],
    "enhanced_tag": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "tag_type_code TEXT GENERATED ALWAYS AS (data->>'tag_type_code') VIRTUAL",
        "start_time TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'start_time')::timestamptz) VIRTUAL",
        "end_time TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'end_time')::timestamptz) VIRTUAL",
        "start_day DATE GENERATED ALWAYS AS ((data->>'start_day')::date) VIRTUAL",
        "end_day DATE GENERATED ALWAYS AS ((data->>'end_day')::date) VIRTUAL",
        "comment TEXT GENERATED ALWAYS AS (data->>'comment') VIRTUAL",
        "custom_name TEXT GENERATED ALWAYS AS (data->>'custom_name') VIRTUAL",
    ],
    "workout": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "activity TEXT GENERATED ALWAYS AS (data->>'activity') VIRTUAL",
        "calories REAL GENERATED ALWAYS AS ((data->>'calories')::real) VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "distance REAL GENERATED ALWAYS AS ((data->>'distance')::real) VIRTUAL",
        "end_datetime TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'end_datetime')::timestamptz) VIRTUAL",
        "intensity TEXT GENERATED ALWAYS AS (data->>'intensity') VIRTUAL",
        "label TEXT GENERATED ALWAYS AS (data->>'label') VIRTUAL",
        "source TEXT GENERATED ALWAYS AS (data->>'source') VIRTUAL",
        "start_datetime TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'start_datetime')::timestamptz) VIRTUAL",
    ],
    "session": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "end_datetime TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'end_datetime')::timestamptz) VIRTUAL",
        "start_datetime TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'start_datetime')::timestamptz) VIRTUAL",
        "mood TEXT GENERATED ALWAYS AS (data->>'mood') VIRTUAL",
        "motion_count INTEGER GENERATED ALWAYS AS ((data->>'motion_count')::integer) VIRTUAL",
        "type TEXT GENERATED ALWAYS AS (data->>'type') VIRTUAL",
    ],
    "daily_activity": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "score INTEGER GENERATED ALWAYS AS ((data->>'score')::integer) VIRTUAL",
        "active_calories INTEGER GENERATED ALWAYS AS ((data->>'active_calories')::integer) VIRTUAL",
        "total_calories INTEGER GENERATED ALWAYS AS ((data->>'total_calories')::integer) VIRTUAL",
        "steps INTEGER GENERATED ALWAYS AS ((data->>'steps')::integer) VIRTUAL",
        "equivalent_walking_distance INTEGER GENERATED ALWAYS AS ((data->>'equivalent_walking_distance')::integer) VIRTUAL",
        "high_activity_time INTEGER GENERATED ALWAYS AS ((data->>'high_activity_time')::integer) VIRTUAL",
        "medium_activity_time INTEGER GENERATED ALWAYS AS ((data->>'medium_activity_time')::integer) VIRTUAL",
        "low_activity_time INTEGER GENERATED ALWAYS AS ((data->>'low_activity_time')::integer) VIRTUAL",
        "sedentary_time INTEGER GENERATED ALWAYS AS ((data->>'sedentary_time')::integer) VIRTUAL",
        "resting_time INTEGER GENERATED ALWAYS AS ((data->>'resting_time')::integer) VIRTUAL",
        "non_wear_time INTEGER GENERATED ALWAYS AS ((data->>'non_wear_time')::integer) VIRTUAL",
        "average_met_minutes REAL GENERATED ALWAYS AS ((data->>'average_met_minutes')::real) VIRTUAL",
        "timestamp TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'timestamp')::timestamptz) VIRTUAL",
    ],
    "daily_sleep": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "score INTEGER GENERATED ALWAYS AS ((data->>'score')::integer) VIRTUAL",
        "timestamp TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'timestamp')::timestamptz) VIRTUAL",
    ],
    "daily_spo2": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "breathing_disturbance_index REAL GENERATED ALWAYS AS ((data->>'breathing_disturbance_index')::real) VIRTUAL",
    ],
    "daily_readiness": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "score INTEGER GENERATED ALWAYS AS ((data->>'score')::integer) VIRTUAL",
        "temperature_deviation REAL GENERATED ALWAYS AS ((data->>'temperature_deviation')::real) VIRTUAL",
        "temperature_trend_deviation REAL GENERATED ALWAYS AS ((data->>'temperature_trend_deviation')::real) VIRTUAL",
        "timestamp TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'timestamp')::timestamptz) VIRTUAL",
    ],
    "sleep": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "bedtime_start TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'bedtime_start')::timestamptz) VIRTUAL",
        "bedtime_end TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'bedtime_end')::timestamptz) VIRTUAL",
        "average_breath REAL GENERATED ALWAYS AS ((data->>'average_breath')::real) VIRTUAL",
        "average_heart_rate REAL GENERATED ALWAYS AS ((data->>'average_heart_rate')::real) VIRTUAL",
        "average_hrv INTEGER GENERATED ALWAYS AS ((data->>'average_hrv')::integer) VIRTUAL",
        "awake_time INTEGER GENERATED ALWAYS AS ((data->>'awake_time')::integer) VIRTUAL",
        "deep_sleep_duration INTEGER GENERATED ALWAYS AS ((data->>'deep_sleep_duration')::integer) VIRTUAL",
        "light_sleep_duration INTEGER GENERATED ALWAYS AS ((data->>'light_sleep_duration')::integer) VIRTUAL",
        "rem_sleep_duration INTEGER GENERATED ALWAYS AS ((data->>'rem_sleep_duration')::integer) VIRTUAL",
        "total_sleep_duration INTEGER GENERATED ALWAYS AS ((data->>'total_sleep_duration')::integer) VIRTUAL",
        "time_in_bed INTEGER GENERATED ALWAYS AS ((data->>'time_in_bed')::integer) VIRTUAL",
        "efficiency INTEGER GENERATED ALWAYS AS ((data->>'efficiency')::integer) VIRTUAL",
        "latency INTEGER GENERATED ALWAYS AS ((data->>'latency')::integer) VIRTUAL",
        "lowest_heart_rate INTEGER GENERATED ALWAYS AS ((data->>'lowest_heart_rate')::integer) VIRTUAL",
        "restless_periods INTEGER GENERATED ALWAYS AS ((data->>'restless_periods')::integer) VIRTUAL",
        "period INTEGER GENERATED ALWAYS AS ((data->>'period')::integer) VIRTUAL",
        "type TEXT GENERATED ALWAYS AS (data->>'type') VIRTUAL",
    ],
    "sleep_time": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "recommendation TEXT GENERATED ALWAYS AS (data->>'recommendation') VIRTUAL",
        "status TEXT GENERATED ALWAYS AS (data->>'status') VIRTUAL",
    ],
    "rest_mode_period": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "start_day DATE GENERATED ALWAYS AS ((data->>'start_day')::date) VIRTUAL",
        "end_day DATE GENERATED ALWAYS AS ((data->>'end_day')::date) VIRTUAL",
        "start_time TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'start_time')::timestamptz) VIRTUAL",
        "end_time TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'end_time')::timestamptz) VIRTUAL",
    ],
    "ring_configuration": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "color TEXT GENERATED ALWAYS AS (data->>'color') VIRTUAL",
        "design TEXT GENERATED ALWAYS AS (data->>'design') VIRTUAL",
        "firmware_version TEXT GENERATED ALWAYS AS (data->>'firmware_version') VIRTUAL",
        "hardware_type TEXT GENERATED ALWAYS AS (data->>'hardware_type') VIRTUAL",
        "set_up_at TEXT GENERATED ALWAYS AS (data->>'set_up_at') VIRTUAL",
        "size INTEGER GENERATED ALWAYS AS ((data->>'size')::integer) VIRTUAL",
    ],
    "daily_stress": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "stress_high INTEGER GENERATED ALWAYS AS ((data->>'stress_high')::integer) VIRTUAL",
        "recovery_high INTEGER GENERATED ALWAYS AS ((data->>'recovery_high')::integer) VIRTUAL",
        "day_summary TEXT GENERATED ALWAYS AS (data->>'day_summary') VIRTUAL",
    ],
    "daily_resilience": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "level TEXT GENERATED ALWAYS AS (data->>'level') VIRTUAL",
    ],
    "daily_cardiovascular_age": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "vascular_age INTEGER GENERATED ALWAYS AS ((data->>'vascular_age')::integer) VIRTUAL",
    ],
    "vo2_max": [
        "id TEXT GENERATED ALWAYS AS (data->>'id') VIRTUAL",
        "day DATE GENERATED ALWAYS AS ((data->>'day')::date) VIRTUAL",
        "vo2_max_value INTEGER GENERATED ALWAYS AS ((data->>'vo2_max')::integer) VIRTUAL",
        "timestamp TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'timestamp')::timestamptz) VIRTUAL",
    ],
}

_TIMESERIES_TABLES: dict[str, list[str]] = {
    "heartrate": [
        "ts TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'timestamp')::timestamptz) VIRTUAL",
        "bpm SMALLINT GENERATED ALWAYS AS ((data->>'bpm')::smallint) VIRTUAL",
        "source TEXT GENERATED ALWAYS AS (data->>'source') VIRTUAL",
    ],
    "ring_battery_level": [
        "ts TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'timestamp')::timestamptz) VIRTUAL",
        "level SMALLINT GENERATED ALWAYS AS ((data->>'level')::smallint) VIRTUAL",
        "charging BOOLEAN GENERATED ALWAYS AS ((data->>'charging')::boolean) VIRTUAL",
        "in_charger BOOLEAN GENERATED ALWAYS AS ((data->>'in_charger')::boolean) VIRTUAL",
    ],
    "interbeat_interval": [
        "ts TIMESTAMPTZ GENERATED ALWAYS AS ((data->>'timestamp')::timestamptz) VIRTUAL",
        "ibi REAL GENERATED ALWAYS AS ((data->>'ibi')::real) VIRTUAL",
        "validity SMALLINT GENERATED ALWAYS AS ((data->>'validity')::smallint) VIRTUAL",
    ],
}


def init_schema(conn: pg8000.native.Connection) -> None:
    conn.run("""
        CREATE TABLE IF NOT EXISTS sync_state (
            endpoint TEXT PRIMARY KEY,
            last_sync_date TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)

    # Document tables: data JSONB as sole stored column, id as unique constraint
    for table, vcols in _DOCUMENT_TABLES.items():
        cols = ["data JSONB NOT NULL", "synced_at TIMESTAMPTZ NOT NULL DEFAULT now()"] + vcols
        col_sql = ",\n                ".join(cols)
        conn.run(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {col_sql},
                CONSTRAINT {table}_pkey PRIMARY KEY (id)
            )
        """)

    # Time-series tables
    for table, vcols in _TIMESERIES_TABLES.items():
        cols = ["data JSONB NOT NULL", "synced_at TIMESTAMPTZ NOT NULL DEFAULT now()"] + vcols
        col_sql = ",\n                ".join(cols)
        conn.run(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {col_sql},
                CONSTRAINT {table}_pkey PRIMARY KEY (ts)
            )
        """)


def get_last_sync(conn: pg8000.native.Connection, endpoint: str) -> str | None:
    rows = conn.run(
        "SELECT last_sync_date FROM sync_state WHERE endpoint = :endpoint",
        endpoint=endpoint,
    )
    return rows[0][0] if rows else None


def set_last_sync(conn: pg8000.native.Connection, endpoint: str, date_str: str) -> None:
    conn.run(
        """
        INSERT INTO sync_state (endpoint, last_sync_date, updated_at)
        VALUES (:endpoint, :date, now())
        ON CONFLICT (endpoint)
        DO UPDATE SET last_sync_date = :date, updated_at = now()
        """,
        endpoint=endpoint,
        date=date_str,
    )


def upsert_document(conn: pg8000.native.Connection, table: str, doc_id: str, data: dict) -> None:
    conn.run(
        f"""
        INSERT INTO {table} (data)
        VALUES (:data::jsonb)
        ON CONFLICT (id)
        DO UPDATE SET data = :data::jsonb, synced_at = now()
        """,
        data=json.dumps(data),
    )


def upsert_timeseries(conn: pg8000.native.Connection, table: str, row: dict) -> None:
    conn.run(
        f"""
        INSERT INTO {table} (data)
        VALUES (:data::jsonb)
        ON CONFLICT (ts)
        DO UPDATE SET data = :data::jsonb, synced_at = now()
        """,
        data=json.dumps(row),
    )
