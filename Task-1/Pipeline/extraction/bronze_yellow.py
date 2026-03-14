import dlt
from pyspark.sql import functions as F

@dlt.table(
    comment="NYC Taxi Yellow Trips",
    name="bronze.yellow_trips",
    table_properties={ "delta.feature.timestampNtz": "supported" }
)
def bronze_yellow():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/deeps-task/vols/volumes/main/raw/nyc_taxi/yellow/")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )