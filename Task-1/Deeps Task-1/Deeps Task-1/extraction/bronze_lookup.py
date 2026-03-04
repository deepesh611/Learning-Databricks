import dlt
from pyspark.sql import functions as F

raw_path = "/Volumes/deeps-task/vols/volumes/main/raw/nyc_taxi/lookup/taxi_zone_lookup.csv"


@dlt.table(
    name="bronze.lookup",
    comment="Taxi Zone Lookup",
    table_properties={ "delta.feature.timestampNtz": "supported" }
)
def bronze_lookup():
    df = spark.read.csv(
        "/Volumes/deeps-task/vols/volumes/main/raw/nyc_taxi/lookup/",
        header=True
    )
    return df.withColumn("ingestion_timestamp", F.current_timestamp())