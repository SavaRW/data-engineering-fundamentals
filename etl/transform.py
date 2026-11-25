
import polars as pl


def clean_trips(source: pl.DataFrame) -> pl.DataFrame:
    """Clean TLC taxi trip data and return SQL-aligned columns."""

    column_mapping = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "tip_amount": "tip_amount",
        "total_amount": "total_amount",
    }

    required = list(column_mapping.keys())
    missing = [column for column in required if column not in source.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    trips = source.select(
        [
            pl.col(original).alias(new)
            for original, new in column_mapping.items()
        ]
    )

    trips = trips.with_columns(
        [
            pl.col("vendor_id").cast(pl.Int32, strict=False),
            pl.col("pickup_datetime").cast(pl.Datetime, strict=False),
            pl.col("dropoff_datetime").cast(pl.Datetime, strict=False),
            pl.col("passenger_count").cast(pl.Int32, strict=False),
            pl.col("trip_distance").cast(pl.Float64, strict=False),
            pl.col("pickup_location_id").cast(pl.Int32, strict=False),
            pl.col("dropoff_location_id").cast(pl.Int32, strict=False),
            pl.col("payment_type").cast(pl.Int32, strict=False),
            pl.col("fare_amount").cast(
                pl.Decimal(precision=14, scale=2), strict=False
            ),
            pl.col("tip_amount").cast(
                pl.Decimal(precision=14, scale=2), strict=False
            ),
            pl.col("total_amount").cast(
                pl.Decimal(precision=14, scale=2), strict=False
            ),
        ]
    )

    trips = trips.filter(
        pl.col("pickup_datetime").is_not_null()
        & pl.col("dropoff_datetime").is_not_null()
        & (pl.col("dropoff_datetime") >= pl.col("pickup_datetime"))
        & (
            pl.col("trip_distance").is_null()
            | (pl.col("trip_distance") >= 0)
        )
    )

    return trips
