import dlt
from pyspark.sql import functions as F


# ──────────────────────────────────────────────
# Raw streaming view — source for both SCD tables
# ──────────────────────────────────────────────
@dlt.view(
    name="lookup_zones_raw",
    comment="Raw streaming view of bronze lookup for SCD processing"
)
def lookup_zones_raw():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("bronze.lookup")
        .selectExpr(
            "LocationID as location_id",
            "Borough as borough",
            "Zone as zone",
            "service_zone",
            "ingestion_timestamp"
        )
    )


