
import argparse
import os
import sys
import time
from datetime import date
from pathlib import Path

import polars as pl
import psycopg

from extract import extract_trips, extract_zones
from load import load_trips, load_zones
from transform import clean_trips


DEFAULT_MONTH = "2024-01"
DEFAULT_DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def parse_month(value: str) -> str:
    try:
        date.fromisoformat(f"{value}-01")
    except ValueError:
        raise argparse.ArgumentTypeError(
            "month must be in YYYY-MM format"
        ) from None

    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the NYC Yellow Taxi data ingestion pipeline."
    )

    parser.add_argument(
        "--month",
        type=parse_month,
        default=DEFAULT_MONTH,
    )

    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(
            os.environ.get("TAXI_DATA_DIR") or DEFAULT_DATA_DIR
        ),
        help="Absolute data directory",
    )

    parser.add_argument(
        "--source",
        type=Path,
        help="Use a local Parquet file without downloading",
    )

    parser.add_argument(
        "--zones-only",
        action="store_true",
        help="Download and load the taxi zone lookup CSV only",
    )

    args = parser.parse_args(argv)
    args.data_dir = args.data_dir.resolve()

    if not args.data_dir.is_absolute():
        parser.error("the data directory must be absolute")

    args.download = args.source is None

    if args.download:
        args.source = (
            args.data_dir
            / "raw"
            / "yellow_taxi"
            / f"yellow_tripdata_{args.month}.parquet"
        )
    else:
        args.source = args.source.resolve()

    return args


def run(args: argparse.Namespace) -> None:
    start_time = time.time()

    if args.zones_only:
        zone_source = (
            args.data_dir / "raw" / "reference" / "taxi_zone_lookup.csv"
        )

        print("Extracting taxi zones...")
        zones = extract_zones(zone_source)

        zones = zones.select(
            [
                pl.col("LocationID").cast(pl.Int32).alias("location_id"),
                pl.col("Borough").cast(pl.String).alias("borough"),
                pl.col("Zone").cast(pl.String).alias("zone"),
                pl.col("service_zone").cast(pl.String).alias("service_zone"),
            ]
        )

        with psycopg.connect(connect_timeout=10) as conn:
            loaded = load_zones(conn, zones)

        print(f"Taxi zones extracted: {zones.height}")
        print(f"Taxi zones loaded: {loaded}")

    else:
        print(f"Extracting taxi trips for {args.month}...")
        source = extract_trips(
            args.source,
            month=args.month,
            download=args.download,
        )

        print(f"Source rows: {source.height}")

        print("Transforming taxi trips...")
        trips = clean_trips(source)

        print(f"Accepted rows: {trips.height}")

        print("Loading taxi trips into PostgreSQL...")
        with psycopg.connect(connect_timeout=10) as conn:
            loaded = load_trips(conn, trips)

        print(f"Loaded rows: {loaded}")

    elapsed = time.time() - start_time
    print(f"Pipeline completed in {elapsed:.2f} seconds.")


def main() -> int:
    try:
        run(parse_args())
        return 0
    except Exception as error:
        print(f"Pipeline failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
