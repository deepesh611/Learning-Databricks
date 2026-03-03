import dlt
from pyspark.sql import functions as F

@dlt.table(
    comment="Unified cleaned taxi trips (Yellow + Green + FHV)",
    name="silver.silver_trips",
    table_properties={ "delta.feature.timestampNtz": "supported" }
)
def merge_bronze_tables():

    yellow = (
        dlt.read("bronze.yellow_trips")
        .selectExpr(
            "VendorID as vendor_id",
            "tpep_pickup_datetime as pickup_datetime",
            "tpep_dropoff_datetime as dropoff_datetime",
            "PULocationID as pickup_location_id",
            "DOLocationID as dropoff_location_id",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "ingestion_timestamp",
            "RatecodeID as ratecode_id"
        )
        .withColumn("trip_type", F.lit("yellow"))
        .dropna(subset=["passenger_count"])
    )

    green = (
        dlt.read("bronze.green_trips")
        .selectExpr(
            "VendorID as vendor_id",
            "lpep_pickup_datetime as pickup_datetime",
            "lpep_dropoff_datetime as dropoff_datetime",
            "PULocationID as pickup_location_id",
            "DOLocationID as dropoff_location_id",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "ingestion_timestamp",
            "RatecodeID as ratecode_id"
        )
        .withColumn("trip_type", F.lit("green"))
        .dropna(subset=["passenger_count"])
    )

    fhv = (
        dlt.read("bronze.fhv_trips")
        .selectExpr(
            "pickup_datetime",
            "dropoff_datetime",
            "PULocationID as pickup_location_id",
            "DOLocationID as dropoff_location_id",
            "ingestion_timestamp"
        )
        .withColumn("vendor_id", F.lit(None))
        .withColumn("passenger_count", F.lit(None))
        .withColumn("trip_distance", F.lit(None))
        .withColumn("fare_amount", F.lit(None))
        .withColumn("total_amount", F.lit(None))
        .withColumn("trip_type", F.lit("fhv"))
        .withColumn("ratecode_id", F.lit(None))

        .dropna(subset=[
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id"
        ])
    )

    merged = yellow.unionByName(green).unionByName(fhv)

    cleaned = (
                 merged
                .withColumn("vendor_id", F.col("vendor_id").cast("int"))
                .withColumn("passenger_count", F.col("passenger_count").cast("int"))
                .withColumn("trip_distance", F.col("trip_distance").cast("double"))
                .withColumn("fare_amount", F.col("fare_amount").cast("double"))
                .withColumn("total_amount", F.col("total_amount").cast("double"))
                .withColumn("pickup_datetime", F.col("pickup_datetime").cast("timestamp"))
                .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast("timestamp"))
                .withColumn("ratecode_id", F.col("ratecode_id").cast("int"))
                .withColumn("pickup_location_id", F.col("pickup_location_id").cast("int"))
                .withColumn("dropoff_location_id", F.col("dropoff_location_id").cast("int"))
                .withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast("timestamp"))

                .dropDuplicates([
                    "vendor_id",
                    "pickup_datetime",
                    "dropoff_datetime",
                    "pickup_location_id"
                ])

                .filter(
                    (F.col("trip_type") == "fhv") |
                        (
                            (F.col("fare_amount") > 0) &
                            (F.col("trip_distance") > 0) &
                            (F.col("passenger_count") >= 0) &
                            (F.col("dropoff_datetime") > F.col("pickup_datetime"))
                        )
                    )
    )

    return cleaned



@dlt.table(
        name="silver.lookup_zones",
        comment="Lookup table for taxi zones",
        table_properties={ "delta.feature.timestampNtz": "supported" }
    )

def lookup_zones():
        bronze = dlt.read("bronze.lookup")

        cleaned = (
            bronze
            .selectExpr(
                "LocationID as location_id",
                "borough",
                "zone",
                "service_zone",
                "ingestion_timestamp"
            )
            .withColumn("location_id", F.col("location_id").cast("int"))
            .withColumn("borough", F.col("Borough").cast("string"))
            .withColumn("zone", F.col("Zone").cast("string"))
            .withColumn("service_zone", F.col("service_zone").cast("string"))
            .withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast("timestamp"))

            .dropDuplicates([
                "Borough",
                "Zone",
                "service_zone"
            ])

            .dropna()
        )

        return cleaned









