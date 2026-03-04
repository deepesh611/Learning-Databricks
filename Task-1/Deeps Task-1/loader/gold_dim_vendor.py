import dlt
from pyspark.sql import functions as F


@dlt.table(
    comment="Vendor Dimension Table",
    name="gold.dim_vendor",
    table_properties={
        "delta.feature.timestampNtz": "supported",
        "pipelines.autoOptimize.optimizeWrite": "true"
    }
)
def dim_vendor():
    return (
        dlt.read("silver.silver_trips")
        .select("vendor_id")
        .filter(F.col("vendor_id").isNotNull())
        .dropDuplicates()
        .withColumn(
            "vendor_name",
            F.when(F.col("vendor_id") == 1, "Creative Mobile Technologies, LLC")
             .when(F.col("vendor_id") == 2, "Curb Mobility, LLC")
             .when(F.col("vendor_id") == 6, "Myle Technologies Inc")
             .when(F.col("vendor_id") == 7, "Helix")
             .otherwise("Unknown")
        )
    )
