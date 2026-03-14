import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="gold.dim_ratecode",
    comment="Rate Code Dimension Table",
    table_properties={
        "delta.feature.timestampNtz": "supported",
        "pipelines.autoOptimize.optimizeWrite": "true"
    }
)
def dim_ratecode():

    ratecodes = (
        dlt.read("silver.silver_trips")
        .select("ratecode_id")
        .filter(F.col("ratecode_id").isNotNull())
        .dropDuplicates()
        .withColumn(
            "rate_description",
            F.when(F.col("ratecode_id") == 1, "Standard rate")
             .when(F.col("ratecode_id") == 2, "JFK Airport")
             .when(F.col("ratecode_id") == 3, "Newark Airport")
             .when(F.col("ratecode_id") == 4, "Nassau or Westchester")
             .when(F.col("ratecode_id") == 5, "Negotiated fare")
             .when(F.col("ratecode_id") == 6, "Group ride")
             .otherwise("Unknown")
        )
    )
    return ratecodes