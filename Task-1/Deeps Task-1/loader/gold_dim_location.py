import dlt
from pyspark.sql import functions as F


# ──────────────────────────────────────────────
# SCD Type 1 — dim_location_scd1
# Zone/borough changes overwrite the old value
# No history is maintained
# Use case: correcting wrong zone names
# ──────────────────────────────────────────────
dlt.create_streaming_table(
    name="gold.dim_location_scd1",
    comment="Location Dimension Table - SCD Type 1 (overwrite, no history)",
    table_properties={
        "delta.feature.timestampNtz": "supported",
        "pipelines.autoOptimize.optimizeWrite": "true"
    }
)

dlt.apply_changes(
    target = "gold.dim_location_scd1",
    source = "silver.lookup_zones",
    keys = ["location_id"],
    sequence_by = "ingestion_timestamp",
    stored_as_scd_type = 1
)


# ──────────────────────────────────────────────
# SCD Type 2 — dim_location_scd2
# Zone/borough changes create a new record
# Old record is closed with __END_AT timestamp
# Use case: tracking historical zone assignments
# Query current: WHERE __END_AT IS NULL
# ──────────────────────────────────────────────
dlt.create_streaming_table(
    name="gold.dim_location_scd2",
    comment="Location Dimension Table - SCD Type 2 (history maintained)",
    table_properties={
        "delta.feature.timestampNtz": "supported",
        "pipelines.autoOptimize.optimizeWrite": "true"
    }
)

dlt.apply_changes(
    target = "gold.dim_location_scd2",
    source = "silver.lookup_zones",
    keys = ["location_id"],
    sequence_by = "ingestion_timestamp",
    stored_as_scd_type = 2,
    track_history_column_list = ["zone", "borough", "service_zone"]
)
