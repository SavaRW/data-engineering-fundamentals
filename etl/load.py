
import polars as pl
from psycopg import Connection


TRIP_COLUMNS = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
]


def load_trips(conn: Connection, trips: pl.DataFrame) -> int:
    """Append accepted trips to PostgreSQL using COPY."""

    if trips.is_empty():
        return 0

    selected = trips.select(TRIP_COLUMNS)

    copy_sql = """
        COPY raw.yellow_taxi_trips (
            vendor_id,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            pickup_location_id,
            dropoff_location_id,
            payment_type,
            fare_amount,
            tip_amount,
            total_amount
        )
        FROM STDIN
    """

    with conn.cursor() as cursor:
        with cursor.copy(copy_sql) as copy:
            for row in selected.iter_rows():
                copy.write_row(row)

    return selected.height


def load_zones(conn: Connection, zones: pl.DataFrame) -> int:
    """Insert or update taxi zones safely when the pipeline is rerun."""

    required = ["location_id", "borough", "zone", "service_zone"]
    selected = zones.select(required)

    sql = """
        INSERT INTO raw.taxi_zones (
            location_id, borough, zone, service_zone
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (location_id)
        DO UPDATE SET
            borough = EXCLUDED.borough,
            zone = EXCLUDED.zone,
            service_zone = EXCLUDED.service_zone
    """

    rows = list(selected.iter_rows())

    with conn.cursor() as cursor:
        cursor.executemany(sql, rows)

    return len(rows)
