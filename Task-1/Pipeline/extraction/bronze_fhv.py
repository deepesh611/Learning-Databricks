import dlt
from pyspark.sql import functions as F

@dlt.table(
    comment="NYC For Hire Vehicles",
    name="bronze.fhv_trips",
    table_properties={ "delta.feature.timestampNtz": "supported" }
)
def bronze_fhv():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/deeps-task/vols/volumes/main/raw/nyc_taxi/fhv/")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )