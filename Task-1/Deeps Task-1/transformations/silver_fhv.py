import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver.fhv_trips_clean",
    comment="Cleaned FHV (For-Hire Vehicle) trips from Bronze",
    table_properties={"delta.feature.timestampNtz": "supported"}
)
def silver_fhv():
    return (
        dlt.readStream("bronze.fhv_trips")
        .selectExpr(
            "pickup_datetime",
            "dropoff_datetime",
            "PULocationID as pickup_location_id",
            "DOLocationID as dropoff_location_id",
            "ingestion_timestamp"
        )
        # FHV has no vendor/fare/distance — fill with nulls for schema consistency
        .withColumn("vendor_id",        F.lit(None).cast("int"))
        .withColumn("passenger_count",  F.lit(None).cast("int"))
        .withColumn("trip_distance",    F.lit(None).cast("double"))
        .withColumn("fare_amount",      F.lit(None).cast("double"))
        .withColumn("total_amount",     F.lit(None).cast("double"))
        .withColumn("ratecode_id",      F.lit(None).cast("int"))
        .withColumn("trip_type",        F.lit("fhv"))
        .withColumn("pickup_datetime",      F.col("pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_datetime",     F.col("dropoff_datetime").cast("timestamp"))
        .withColumn("pickup_location_id",   F.col("pickup_location_id").cast("int"))
        .withColumn("dropoff_location_id",  F.col("dropoff_location_id").cast("int"))
        .withColumn("ingestion_timestamp",  F.col("ingestion_timestamp").cast("timestamp"))
        # .dropna(subset=[
        #     "pickup_datetime",
        #     "dropoff_datetime",
        #     "pickup_location_id",
        #     "dropoff_location_id"
        # ])
        .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
    )
