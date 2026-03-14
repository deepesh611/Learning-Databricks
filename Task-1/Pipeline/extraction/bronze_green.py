import dlt
from pyspark.sql import functions as F


@dlt.table(
    comment="NYC Taxi Green Trips",
    name="bronze.green_trips",
    table_properties={ "delta.feature.timestampNtz": "supported" }
)
def bronze_green():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/deeps-task/vols/volumes/main/raw/nyc_taxi/green/")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )