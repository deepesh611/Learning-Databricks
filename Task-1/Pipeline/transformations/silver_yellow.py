import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver.yellow_trips_clean",
    comment="Cleaned Yellow Taxi trips from Bronze",
    table_properties={"delta.feature.timestampNtz": "supported"}
)
def silver_yellow():
    return (
        dlt.readStream("bronze.yellow_trips")
        .selectExpr(
            "VendorID as vendor_id",
            "tpep_pickup_datetime as pickup_datetime",
            "tpep_dropoff_datetime as dropoff_datetime",
            "PULocationID as pickup_location_id",
            "DOLocationID as dropoff_location_id",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "ingestion_timestamp",
            "RatecodeID as ratecode_id"
        )
        .withColumn("trip_type", F.lit("yellow"))
        .withColumn("vendor_id",            F.col("vendor_id").cast("int"))
        .withColumn("passenger_count",      F.col("passenger_count").cast("int"))
        .withColumn("trip_distance",        F.col("trip_distance").cast("double"))
        .withColumn("fare_amount",          F.col("fare_amount").cast("double"))
        .withColumn("total_amount",         F.col("total_amount").cast("double"))
        .withColumn("pickup_datetime",      F.col("pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_datetime",     F.col("dropoff_datetime").cast("timestamp"))
        .withColumn("ratecode_id",          F.col("ratecode_id").cast("int"))
        .withColumn("pickup_location_id",   F.col("pickup_location_id").cast("int"))
        .withColumn("dropoff_location_id",  F.col("dropoff_location_id").cast("int"))
        .withColumn("ingestion_timestamp",  F.col("ingestion_timestamp").cast("timestamp"))
        .filter(
            (F.col("fare_amount") >= 0) &
            (F.col("trip_distance") >= 0) &
            (F.col("dropoff_datetime") >= F.col("pickup_datetime"))
        )
    )
