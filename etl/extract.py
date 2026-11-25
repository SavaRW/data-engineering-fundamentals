
from pathlib import Path
import urllib.request

import polars as pl

TRIPS_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{month}.parquet"
)
ZONES_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/"
    "taxi_zone_lookup.csv"
)


def download_file(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")

    try:
        request = urllib.request.Request(
            url, headers={"User-Agent": "NIBM-Taxi-ETL/1.0"}
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            with temporary.open("wb") as output:
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)

        temporary.replace(destination)
    finally:
        if temporary.exists():
            temporary.unlink()


def extract_trips(
    source: Path, *, month: str, download: bool
) -> pl.DataFrame:
    if not source.exists():
        if not download:
            raise FileNotFoundError(source)
        download_file(TRIPS_URL.format(month=month), source)

    return pl.read_parquet(source)


def extract_zones(source: Path, *, download: bool = True) -> pl.DataFrame:
    if not source.exists():
        if not download:
            raise FileNotFoundError(source)
        download_file(ZONES_URL, source)

    return pl.read_csv(source)
