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

    vendors = (
        dlt.read("silver.silver_trips")
        .select("vendor_id")
        .filter(F.col("vendor_id").isNotNull())
        .dropDuplicates()
    )

    return vendors



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



@dlt.table(
    name="gold.dim_location",
    comment="Location Dimension Table",
    table_properties={
        "delta.feature.timestampNtz": "supported",
        "pipelines.autoOptimize.optimizeWrite": "true"
    }
)
def dim_location():
    locations = (
        dlt.read("silver.lookup_zones")
        .select("location_id", "borough", "zone", "service_zone")
        .dropDuplicates()
    )
    return locations




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
    


@dlt.table(
    name = "gold.fact_trips",
    comment="Aggregated taxi trips by pickup and dropoff zones",
    table_properties={ "delta.feature.timestampNtz": "supported" }
)
def fact_trip():

    facts = (
        dlt.read("silver.silver_trips")

        # Create date_key from pickup_datetime
        .withColumn(
            "date_key",
            F.date_format("pickup_datetime", "yyyyMMdd").cast("int")
        )

        # Select only foreign keys + measures
        .select(
            "vendor_id",
            "pickup_location_id",
            "dropoff_location_id",
            "date_key",
            "fare_amount",
            "trip_distance",
            "total_amount",
            "passenger_count"
        )

        # Remove invalid foreign keys
        .dropna(subset=[
            "vendor_id",
            "pickup_location_id",
            "dropoff_location_id",
            "date_key"
        ])
    )

    return facts