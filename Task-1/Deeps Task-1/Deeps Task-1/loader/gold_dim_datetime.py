import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold.dim_datetime",
    comment="Date Time Dimension Table",
    table_properties={
        "delta.feature.timestampNtz": "supported",
        "pipelines.autoOptimize.optimizeWrite": "true"
    }
)
def dim_datetime():
    dates = (
        dlt.read("silver.silver_trips")
        .select("pickup_datetime")
        .filter(F.col("pickup_datetime").isNotNull())
        .withColumn("date", F.to_date("pickup_datetime"))
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("month", F.month("date"))
        .withColumn("year", F.year("date"))
        .withColumn("weekday", F.date_format("date", "E"))
        .select("date_key", "day", "month", "year", "weekday")
        .dropDuplicates()
    )
    return dates
