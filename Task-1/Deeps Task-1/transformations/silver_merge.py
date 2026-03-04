import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver.silver_trips",
    comment="Unified Silver trips — merged from yellow, green, and fhv clean silver tables",
    table_properties={"delta.feature.timestampNtz": "supported"}
)
def merge_silver_tables():
    yellow = dlt.read("silver.yellow_trips_clean")
    green  = dlt.read("silver.green_trips_clean")
    fhv    = dlt.read("silver.fhv_trips_clean")

    return (
        yellow
        .unionByName(green)
        .unionByName(fhv)
        .dropDuplicates([
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id"
        ])
    )


@dlt.table(
    name="silver.lookup_zones",
    comment="Lookup table for taxi zones",
    table_properties={"delta.feature.timestampNtz": "supported"}
)
def lookup_zones():
    return (
        dlt.read("bronze.lookup")
        .selectExpr(
            "LocationID as location_id",
            "borough",
            "zone",
            "service_zone",
            "ingestion_timestamp"
        )
        .withColumn("location_id",        F.col("location_id").cast("int"))
        .withColumn("borough",            F.col("Borough").cast("string"))
        .withColumn("zone",               F.col("Zone").cast("string"))
        .withColumn("service_zone",       F.col("service_zone").cast("string"))
        .withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast("timestamp"))
        .dropDuplicates(["Borough", "Zone", "service_zone"])
    )
