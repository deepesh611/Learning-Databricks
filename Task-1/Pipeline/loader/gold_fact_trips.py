import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold.fact_trips",
    comment="Fact table built by joining silver trips with all gold dimension tables",
    table_properties={"delta.feature.timestampNtz": "supported"}
)
def fact_trip():

    trips    = dlt.read_stream("silver.silver_trips")
    vendors  = dlt.read("gold.dim_vendor")
    dates    = dlt.read("gold.dim_datetime")
    pickup   = dlt.read("gold.dim_location_scd1").select(
                   F.col("location_id").alias("pickup_location_id")
               )
    dropoff  = dlt.read("gold.dim_location_scd1").select(
                   F.col("location_id").alias("dropoff_location_id")
               )
    ratecodes = dlt.read("gold.dim_ratecode")

    # Derive date_key from pickup_datetime to join with dim_datetime
    trips_with_key = trips.withColumn(
        "date_key",
        F.date_format("pickup_datetime", "yyyyMMdd").cast("int")
    )

    return (
        trips_with_key

        # Left join with dim_vendor — FHV trips have NULL vendor_id, keep them
        .join(vendors, on="vendor_id", how="left")

        # Left join with dim_datetime — keep trips even if date_key is null
        .join(dates, on="date_key", how="left")

        # Left join with dim_location (pickup) — keep trips with unknown pickup zones
        .join(pickup, on="pickup_location_id", how="left")

        # Left join with dim_location (dropoff) — keep trips with unknown dropoff zones
        .join(dropoff, on="dropoff_location_id", how="left")

        # Left join with dim_ratecode — FHV trips have NULL ratecode_id, keep them
        .join(ratecodes, on="ratecode_id", how="left")

        # Select only FK surrogate keys + measures
        .select(
            trips_with_key.vendor_id,
            trips_with_key.date_key,
            trips_with_key.pickup_location_id,
            trips_with_key.dropoff_location_id,
            trips_with_key.ratecode_id,
            trips_with_key.fare_amount,
            trips_with_key.trip_distance,
            trips_with_key.total_amount,
            trips_with_key.passenger_count
        )
    )
