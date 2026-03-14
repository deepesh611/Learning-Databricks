import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver.silver_trips",
    comment="Unified Silver trips — merged from yellow, green, and fhv clean silver tables",
    table_properties={"delta.feature.timestampNtz": "supported"}
)
def merge_silver_tables():
    yellow = dlt.read_stream("silver.yellow_trips_clean")
    green  = dlt.read_stream("silver.green_trips_clean")
    fhv    = dlt.read_stream("silver.fhv_trips_clean")

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
