import dlt
from pyspark.sql import functions as F


# ──────────────────────────────────────────────
# 1. INNER JOIN — Trips with vendor data
# Only trips where vendor_id exists in both tables
# ──────────────────────────────────────────────
@dlt.table(
    name="gold.join_inner_trips_vendor",
    comment="Inner Join: Trips matched with vendor dimension"
)
def join_inner_trips_vendor():
    trips = dlt.read("silver.silver_trips")
    vendors = dlt.read("gold.dim_vendor")

    return (
        trips.join(vendors, on="vendor_id", how="inner")
        .select(
            trips.vendor_id,
            trips.pickup_datetime,
            trips.dropoff_datetime,
            trips.fare_amount,
            trips.trip_distance
        )
    )


# ──────────────────────────────────────────────
# 2. LEFT JOIN — Trips with location lookup
# All trips kept, nulls where no matching zone
# ──────────────────────────────────────────────
@dlt.table(
    name="gold.join_left_trips_location",
    comment="Left Join: All trips with location zone info where available"
)
def join_left_trips_location():
    trips = dlt.read("silver.silver_trips")
    zones = dlt.read("silver.lookup_zones")

    return (
        trips.join(
            zones,
            trips.pickup_location_id == zones.location_id,
            how="left"
        )
        .select(
            trips.vendor_id,
            trips.pickup_datetime,
            trips.fare_amount,
            zones.borough,
            zones.zone
        )
    )


# ──────────────────────────────────────────────
# 3. RIGHT JOIN — Data validation
# All zones kept, nulls where no trips exist
# Reveals zones that have no trip activity
# ──────────────────────────────────────────────
@dlt.table(
    name="gold.join_right_zones_trips",
    comment="Right Join: All zones, including those with no trips (validation)"
)
def join_right_zones_trips():
    trips = dlt.read("silver.silver_trips")
    zones = dlt.read("silver.lookup_zones")

    return (
        trips.join(
            zones,
            trips.pickup_location_id == zones.location_id,
            how="right"
        )
        .select(
            zones.location_id,
            zones.zone,
            zones.borough,
            trips.fare_amount
        )
    )


# ──────────────────────────────────────────────
# 4. FULL JOIN — Data reconciliation
# All records from both sides, nulls where no match
# Used to find mismatches between trips and zones
# ──────────────────────────────────────────────
@dlt.table(
    name="gold.join_full_trips_zones",
    comment="Full Join: Reconciliation between trips and zone lookup"
)
def join_full_trips_zones():
    trips = dlt.read("silver.silver_trips")
    zones = dlt.read("silver.lookup_zones")

    return (
        trips.join(
            zones,
            trips.pickup_location_id == zones.location_id,
            how="full"
        )
        .select(
            trips.vendor_id,
            trips.pickup_location_id,
            zones.location_id,
            zones.zone,
            trips.fare_amount
        )
    )


# ──────────────────────────────────────────────
# 5. BROADCAST JOIN — Small lookup optimization
# Broadcasts the small zones table to all workers
# Avoids shuffle, improves performance
# ──────────────────────────────────────────────
@dlt.table(
    name="gold.join_broadcast_trips_zones",
    comment="Broadcast Join: Trips joined with broadcasted zones lookup for performance"
)
def join_broadcast_trips_zones():
    trips = dlt.read("silver.silver_trips")
    zones = dlt.read("silver.lookup_zones")

    return (
        trips.join(
            F.broadcast(zones),
            trips.pickup_location_id == zones.location_id,
            how="inner"
        )
        .select(
            trips.vendor_id,
            trips.pickup_datetime,
            trips.fare_amount,
            zones.zone,
            zones.borough
        )
    )


# ──────────────────────────────────────────────
# 6. SELF JOIN — Pickup vs dropoff comparison
# Joins trips table with itself (via zones)
# Compares pickup zone name vs dropoff zone name
# ──────────────────────────────────────────────
@dlt.table(
    name="gold.join_self_pickup_dropoff",
    comment="Self Join: Comparing pickup zone vs dropoff zone per trip"
)
def join_self_pickup_dropoff():
    trips = dlt.read("silver.silver_trips")
    zones = dlt.read("silver.lookup_zones")

    pickup_zones  = zones.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough")
    )

    dropoff_zones = zones.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough")
    )

    return (
        trips
        .join(pickup_zones,  on="pickup_location_id",  how="left")
        .join(dropoff_zones, on="dropoff_location_id", how="left")
        .select(
            trips.vendor_id,
            trips.pickup_datetime,
            trips.fare_amount,
            trips.trip_distance,
            "pickup_zone",
            "pickup_borough",
            "dropoff_zone",
            "dropoff_borough"
        )
    )
